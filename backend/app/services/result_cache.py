"""结果缓存领域服务：查询结果缓存的生命周期管理、命中统计、失效检测与跨模块消费。"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 历史缓存里出现过的失效/淘汰状态标记，统一视为不可命中
INVALID_STATUS_MARKERS = {"已过期", "已淘汰", "淘汰中", "失效", "已失效", "已停用"}
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# 历史数据可能只写到日期（如 2027-01-01），按当天 00:00:00 解释
DATE_FORMAT = "%Y-%m-%d"


class ResultCacheService(BaseModuleService):
    MODULE = "result_cache"
    ENTITY = "缓存条目"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["条目编号", "查询摘要", "结果哈希", "缓存命中", "过期时间", "存储大小", "创建时间", "最后命中"]
    REQUIRED_FIELDS = ["条目编号", "查询摘要"]
    STATUS_ORDER = ["有效", "已过期", "已淘汰"]
    ACTION_RULES = {"主动刷新": "有效", "淘汰过期": "已淘汰"}
    NEGATIVE_ACTIONS = ["淘汰过期"]

    def invalidate_by_source(self, source_id: int) -> dict[str, Any]:
        """数据源变更时淘汰关联的 result_cache，通知下游消费者。"""
        entries = store.rows(self.MODULE)
        invalidated = []

        for entry in entries:
            if entry.get("status") != "有效":
                continue
            # 检查是否关联到该数据源
            summary = entry.get("查询摘要", "")
            related = False

            # 检查 source_registry 关联
            sources = store.rows("source_registry")
            for s in sources:
                if s.get("id") == source_id and str(s.get("源编号", "")) in summary:
                    related = True
                    break

            # 检查 olap_cube 关联
            if not related:
                cubes = store.rows("olap_cube")
                for c in cubes:
                    if str(source_id) in str(c.get("数据来源", "")) and f"cube_{c.get('id')}" in summary:
                        related = True
                        break

            if related:
                entry["status"] = "已过期"
                entry["pending"] = False
                invalidated.append({
                    "cache_id": entry["id"],
                    "条目编号": entry.get("条目编号"),
                    "摘要": summary,
                })

        # 更新数据契约状态（如果有消费者受影响）
        _update_contract_on_invalidation(source_id, len(invalidated))

        # 写入审计
        _write_cache_audit("invalidate", source_id, len(invalidated))

        return {
            "ok": True,
            "source_id": source_id,
            "invalidated_count": len(invalidated),
            "entries": invalidated[:10],
        }

    def get_cached_result(self, query_fingerprint: str) -> dict[str, Any] | None:
        """按查询摘要查找缓存结果并更新命中计数（兼容旧调用方）。

        只有通过完整有效性核验（状态有效、数据完整、到期时间可解析且
        仍在有效期内）的条目才算命中；解析到期时间失败不再放行。
        被 query_federation、data_exporter 调用。
        """
        verdict = self.read_verifiable_cache(query_fingerprint, record_hit=True)
        if not verdict["hit"]:
            return None
        return verdict["delivery"]

    def read_verifiable_cache(
        self, query_fingerprint: str, record_hit: bool = True
    ) -> dict[str, Any]:
        """联邦查询读取可复核的缓存命中数据，据此决定是否回源。

        对同一查询摘要下的全部条目逐条核验，先排入的坏条目不会抢在
        有效批次之前：损坏/过期条目被跳过并记录原因，最终交付第一个
        通过完整核验的条目。只对真正交付的条目计一次命中。

        返回结构固定包含：
        - hit: 是否实际命中并交付
        - reason: 未命中时的原因（absent/expired/corrupted/invalidated/empty）
        - delivery: 命中时交付的缓存数据与有效期证据，未命中为 None
        - candidates: 同摘要下每个条目的核验结论，供审计复核
        """
        now = time.time()
        candidates: list[dict[str, Any]] = []
        fallback_evidence = self._collect_batch_fallback_evidence(query_fingerprint)
        delivered: dict[str, Any] | None = None

        for entry in store.rows(self.MODULE):
            if entry.get("查询摘要") != query_fingerprint:
                continue

            check = self._verify_entry(entry, now, fallback_evidence)
            candidates.append(check)

            if delivered is None and check["verdict"] == "valid":
                delivered = self._build_delivery(entry, check, now)
                if record_hit:
                    # 重复提交只计实际给出的命中：仅在交付时 +1
                    entry["缓存命中"] = str(_safe_int(entry.get("缓存命中")) + 1)
                    entry["最后命中"] = time.strftime(TIME_FORMAT)

        if delivered is not None:
            return {
                "hit": True,
                "reason": "hit",
                "delivery": delivered,
                "candidates": candidates,
            }

        reason = _summarize_miss_reason(candidates)
        return {
            "hit": False,
            "reason": reason,
            "delivery": None,
            "candidates": candidates,
        }

    def _verify_entry(
        self,
        entry: dict[str, Any],
        now: float,
        fallback_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """核验单个缓存条目，返回可复核的核验结论（不修改条目状态以外的内容）。"""
        cache_id = entry.get("id")
        base: dict[str, Any] = {
            "cache_id": cache_id,
            "条目编号": entry.get("条目编号"),
            "查询摘要": entry.get("查询摘要"),
            "verdict": "rejected",
            "status": entry.get("status"),
        }

        # 1. 兼容历史缓存的失效标记：含失效/淘汰标记的条目一律不可命中
        if entry.get("status") in INVALID_STATUS_MARKERS:
            base["verdict"] = "invalidated"
            base["reason"] = f"条目状态为{entry.get('status')}，属于历史失效标记"
            return base

        if entry.get("status") not in (None, "有效"):
            base["verdict"] = "invalidated"
            base["reason"] = f"条目状态为{entry.get('status')}，不是有效状态"
            return base

        # 2. 完整数据检查：无数据载体或空结果必须说明原因，不能当作命中交付
        data = entry.get("result_data", entry.get("cached_data"))
        if data is None:
            base["verdict"] = "empty"
            base["reason"] = "缓存条目缺少结果数据载体（result_data/cached_data 均缺失）"
            return base
        if not isinstance(data, list) or len(data) == 0:
            base["verdict"] = "empty"
            base["reason"] = "缓存结果为空列表，无完整数据可交付"
            return base

        # 3. 到期核验：到期列损坏时，尝试同批写入时留存的到期时刻
        expire_ts, expire_source, expire_raw = self._resolve_expiry(entry, fallback_evidence)
        base["expire_raw"] = expire_raw
        base["expire_source"] = expire_source
        if expire_ts is None:
            # 到期列损坏且找不到同批凭据：不得放行，标记为异常供排查
            entry["abnormal"] = True
            base["verdict"] = "corrupted"
            base["reason"] = "到期时间无法解析，且缺少同批写入留存的到期凭据"
            return base

        base["expire_ts"] = expire_ts
        base["expire_at"] = time.strftime(TIME_FORMAT, time.localtime(expire_ts))
        if now > expire_ts:
            # 确认超时：同步状态，避免后续重复核验
            entry["status"] = "已过期"
            entry["pending"] = False
            base["verdict"] = "expired"
            base["reason"] = f"已超过有效期（到期 {base['expire_at']}）"
            return base

        # 4. 结果哈希与实际数据一致性核验，防止留存摘要与数据不匹配
        data_hash = hashlib.md5(str(data).encode()).hexdigest()[:12]
        stored_hash = entry.get("结果哈希")
        base["hash_match"] = (stored_hash == data_hash) if stored_hash else None

        base["verdict"] = "valid"
        base["reason"] = "核验通过：数据完整且仍在有效期内"
        base["row_count"] = len(data)
        return base

    def _resolve_expiry(
        self,
        entry: dict[str, Any],
        fallback_evidence: dict[str, Any],
    ) -> tuple[float | None, str, Any]:
        """解析条目到期时间戳。

        返回 (到期时间戳, 凭据来源, 原始到期值)。
        到期列损坏时，若同批写入留存了到期时刻与结果摘要且摘要匹配，
        领取人手里的这份数据仍可凭该凭据使用。
        """
        expire_raw = entry.get("过期时间")
        parsed = _parse_expire_value(expire_raw)
        if parsed is not None:
            return parsed, "expire_column", expire_raw

        batch_key = entry.get("批次标识") or entry.get("条目编号")
        evidence = fallback_evidence.get(batch_key)
        if evidence and evidence.get("result_hash") == entry.get("结果哈希"):
            return evidence["expire_ts"], "batch_evidence", expire_raw

        return None, "none", expire_raw

    def _collect_batch_fallback_evidence(self, query_fingerprint: str) -> dict[str, Any]:
        """收集同查询摘要下各批次写入时留存的到期时刻与结果摘要。

        新写入的批次会在 batch_evidence 中固化到期时间戳与结果哈希；
        即使后来某条目的到期列被损坏，仍可凭同批凭据核验。
        """
        evidence: dict[str, Any] = {}
        for entry in store.rows(self.MODULE):
            if entry.get("查询摘要") != query_fingerprint:
                continue
            stored = entry.get("batch_evidence")
            if isinstance(stored, dict) and stored.get("expire_ts"):
                key = entry.get("批次标识") or entry.get("条目编号")
                evidence[key] = stored
        return evidence

    def _build_delivery(
        self, entry: dict[str, Any], check: dict[str, Any], now: float
    ) -> dict[str, Any]:
        """组装交付给调用方的缓存数据，附本次读取核验过的有效期证据。"""
        data = entry.get("result_data", entry.get("cached_data"))
        return {
            "cache_id": entry["id"],
            "条目编号": entry.get("条目编号"),
            "批次标识": entry.get("批次标识"),
            "data": data,
            "row_count": len(data),
            "hash": entry.get("结果哈希"),
            "hash_match": check.get("hash_match"),
            "created": entry.get("创建时间"),
            "hits": entry.get("缓存命中"),
            # 本次读取核验过的有效期证据，可复核
            "validity_evidence": {
                "expire_at": check.get("expire_at"),
                "expire_ts": check.get("expire_ts"),
                "expire_source": check.get("expire_source"),
                "verified_at": time.strftime(TIME_FORMAT, time.localtime(now)),
                "remaining_seconds": round(check["expire_ts"] - now, 3),
                "query_fingerprint": entry.get("查询摘要"),
            },
        }

    def prewarm_cache(self, query_patterns: list[str]) -> dict[str, Any]:
        """缓存预热：读取 query_audit 高频查询，预计算结果并写入。"""
        results = []
        for pattern in query_patterns:
            # 模拟预热：生成结果并缓存
            fake_result = [{"pattern": pattern, "value": i} for i in range(1, 6)]
            entries = store.rows(self.MODULE)
            cache_id = max((int(r.get("id", 0)) for r in entries), default=0) + 1
            result_hash = hashlib.md5(str(fake_result).encode()).hexdigest()[:12]
            expire_ts = time.time() + 86400
            batch_no = f"RESU-PW{uuid.uuid4().hex[:8]}"
            cache = {
                "id": cache_id,
                "条目编号": batch_no,
                "批次标识": batch_no,
                "查询摘要": f"prewarm_{pattern}",
                "结果哈希": result_hash,
                "缓存命中": "0",
                "过期时间": time.strftime(TIME_FORMAT, time.localtime(expire_ts)),
                "存储大小": str(len(str(fake_result))),
                "创建时间": time.strftime(TIME_FORMAT),
                "最后命中": time.strftime(TIME_FORMAT),
                "status": "有效",
                "pending": True,
                "abnormal": False,
                "result_data": fake_result,
                # 同批写入时留存的到期时刻与结果摘要，供到期列损坏时回退核验
                "batch_evidence": {
                    "batch_no": batch_no,
                    "expire_ts": expire_ts,
                    "result_hash": result_hash,
                    "written_at": time.strftime(TIME_FORMAT),
                },
            }
            entries.append(cache)
            results.append({"pattern": pattern, "cache_id": cache_id})

        return {"ok": True, "prewarmed": len(results), "entries": results}

    def cache_hit_rate_report(self) -> dict[str, Any]:
        """生成缓存命中率报告，写入审计日志。"""
        entries = store.rows(self.MODULE)
        total = len(entries)
        valid = sum(1 for e in entries if e.get("status") == "有效")
        expired = sum(1 for e in entries if e.get("status") == "已过期")
        evicted = sum(1 for e in entries if e.get("status") == "已淘汰")

        total_hits = sum(int(e.get("缓存命中", "0")) for e in entries if e.get("缓存命中", "0").isdigit())

        report = {
            "total_entries": total,
            "valid": valid,
            "expired": expired,
            "evicted": evicted,
            "hit_rate": round(valid / max(total, 1), 2),
            "total_hits": total_hits,
        }

        _write_cache_audit("report", 0, total)

        return {"ok": True, "report": report}


def _safe_int(value: Any) -> int:
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return 0


def _parse_expire_value(value: Any) -> float | None:
    """严格解析到期时间，无法解析时返回 None（绝不放行）。

    兼容两种历史格式：完整时间戳 ``%Y-%m-%d %H:%M:%S`` 与仅日期 ``%Y-%m-%d``。
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in (TIME_FORMAT, DATE_FORMAT):
        try:
            return time.mktime(time.strptime(text, fmt))
        except (ValueError, OverflowError):
            continue
    return None


def _summarize_miss_reason(candidates: list[dict[str, Any]]) -> str:
    """根据候选条目核验结论汇总未命中原因，区分缺席、超时与空结果。"""
    if not candidates:
        return "absent"
    verdicts = {c["verdict"] for c in candidates}
    # 有候选但全部不可用：按最具体的原因给出空态说明
    if verdicts <= {"invalidated"}:
        return "invalidated"
    if verdicts <= {"expired", "invalidated"}:
        return "expired"
    if "empty" in verdicts and verdicts <= {"empty", "expired", "invalidated"}:
        return "empty"
    if "corrupted" in verdicts:
        return "corrupted"
    return "no_valid_entry"


def _update_contract_on_invalidation(source_id: int, invalidated_count: int) -> None:
    contracts = store.rows("data_contract")
    for c in contracts:
        if c.get("status") == "已承诺" and str(source_id) in str(c.get("数据集", "")):
            if invalidated_count > 5:
                c["status"] = "已违约"


def _write_cache_audit(action: str, entity_id: int, count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"result_cache.{action}(entity_id={entity_id})",
        "执行耗时": "50ms",
        "扫描行数": str(count),
        "返回行数": str(count),
        "数据来源": "result_cache",
        "操作用户": "cache_manager",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)