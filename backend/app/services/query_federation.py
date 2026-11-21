"""联邦查询领域服务：跨数据源查询计划、谓词下推、代价评估与执行编排。"""
from __future__ import annotations

import hashlib
import random
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.result_cache import ResultCacheService
from app.store import store


# 联邦查询缓存摘要与 result_cache 写入时保持一致
def _fingerprint(plan_id: int) -> str:
    return f"federated_query_plan_{plan_id}"


class QueryFederationService(BaseModuleService):
    MODULE = "query_federation"
    ENTITY = "联邦计划"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["计划编号", "参与来源", "关联键", "下推策略", "数据量估算", "并行度", "超时上限", "结果合并"]
    REQUIRED_FIELDS = ["计划编号", "参与来源", "关联键"]
    STATUS_ORDER = ["草稿", "执行中", "已完成", "执行异常"]
    ACTION_RULES = {"提交执行": "执行中", "取消计划": "已完成", "标记异常": "执行异常"}
    NEGATIVE_ACTIONS = ["标记异常"]

    def build_federation_plan(
        self, sources: list[str], query_type: str = "SELECT"
    ) -> dict[str, Any]:
        """从可用数据源构建联邦查询计划：自动推导关联键、下推策略和并行度。"""
        registered_sources = store.rows("source_registry")
        available = [s for s in registered_sources if s.get("status") == "已注册"]

        # 谓词下推策略
        pushdown = _infer_pushdown_strategy(sources, available)

        # 代价估算
        cost = _estimate_cost(sources, query_type, pushdown)

        plan = {
            "参与来源": ", ".join(sources),
            "关联键": _infer_join_keys(sources, available),
            "下推策略": pushdown,
            "数据量估算": f"~{cost['estimated_rows']}行",
            "并行度": str(min(len(sources) * 2, 8)),
            "超时上限": f"{max(30, cost['estimated_rows'] // 1000)}s",
            "结果合并": "UNION_ALL" if len(sources) > 1 else "DIRECT",
        }
        return plan

    def execute_federation_query(self, plan_id: int) -> dict[str, Any]:
        """执行联邦查询：优先读取可复核缓存，未命中才回源。

        命中时交付缓存批次并把命中结果回写查询审计清单（保留批次来源与
        有效期证据）；未命中时回源执行，写入新缓存批次与回源执行审计。
        回源时源数据已经更正，审计关联的是本次新的执行而非旧批命中。
        """
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": "查询计划不存在"}

        sources = [s.strip() for s in plan.get("参与来源", "").split(",") if s.strip()]
        if not sources:
            return {"ok": False, "message": "没有可用的数据源"}

        # 从 source_registry 获取可用数据源
        registry = store.rows("source_registry")
        valid_sources = [
            s for s in registry
            if s.get("源编号", "") in sources and s.get("status") == "已注册"
        ]

        if len(valid_sources) < len(sources):
            missing = set(sources) - {s["源编号"] for s in valid_sources}
            return {
                "ok": False,
                "message": f"部分数据源不可用: {', '.join(missing)}",
            }

        fingerprint = _fingerprint(plan_id)

        # 1. 先读取可复核的缓存命中数据，据此决定是否回源
        verdict = ResultCacheService().read_verifiable_cache(fingerprint, record_hit=True)
        if verdict["hit"]:
            delivery = verdict["delivery"]
            # 实际命中结果回写查询审计清单，保留所用批次来源
            audit = _audit_cache_hit(plan_id, delivery, verdict["candidates"])
            plan["status"] = "已完成"
            plan["pending"] = False
            return {
                "ok": True,
                "plan_id": plan_id,
                "sources": len(valid_sources),
                "cache_hit": True,
                "cache_key": fingerprint,
                "cache_id": delivery["cache_id"],
                "批次标识": delivery.get("批次标识"),
                "rows_returned": delivery["row_count"],
                "results": delivery["data"][:10],
                "validity_evidence": delivery["validity_evidence"],
                "audit_id": audit["id"],
                "miss_reason": None,
            }

        # 2. 缓存不可用（缺席/超时/损坏/空结果），回源执行
        # 应用查询改写规则
        rewrite_rules = _get_applicable_rewrite_rules(plan)
        _apply_rewrite(plan, rewrite_rules)

        # 执行查询（模拟）
        t0 = time.time()
        result_rows = _simulate_federated_query(sources, plan.get("谓词下推策略", ""))

        # 写入结果缓存（新批次，留存到期凭据与结果摘要）
        cache_entry = _cache_query_result(plan_id, result_rows)

        # 写入查询审计：回源执行，与旧批命中明确区分
        elapsed_ms = (time.time() - t0) * 1000
        audit = _audit_federated_query(
            plan_id, elapsed_ms, len(result_rows),
            miss_reason=verdict["reason"], new_cache_id=cache_entry["id"],
        )

        # 更新多维立方数据（如果涉及）
        _update_cube_if_needed(plan_id, result_rows)

        # 更新计划状态
        plan["status"] = "已完成"
        plan["pending"] = False

        return {
            "ok": True,
            "plan_id": plan_id,
            "sources": len(valid_sources),
            "cache_hit": False,
            "miss_reason": verdict["reason"],
            "cache_verification": verdict["candidates"],
            "rows_returned": len(result_rows),
            "elapsed_ms": elapsed_ms,
            "results": result_rows[:10],
            "cache_key": fingerprint,
            "cache_id": cache_entry["id"],
            "批次标识": cache_entry.get("批次标识"),
            "audit_id": audit["id"],
        }

    def estimate_query_cost(self, sources: list[str], query_type: str) -> dict[str, Any]:
        """对联邦查询做代价估算，写入 cost_govern 配额记录。"""
        available = [s for s in store.rows("source_registry") if s.get("status") == "已注册"]
        pushdown = _infer_pushdown_strategy(sources, available)
        cost = _estimate_cost(sources, query_type, pushdown)

        # 写入成本治理模块
        cost_rows = store.rows("cost_govern")
        cost_id = max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1
        cost_entry = {
            "id": cost_id,
            "策略编号": f"COST-{uuid.uuid4().hex[:8]}",
            "适用角色": "federated_query",
            "查询上限": str(cost["estimated_rows"]),
            "并发上限": str(len(sources)),
            "月度配额": str(cost["estimated_rows"] * 30),
            "告警阈值": str(cost["estimated_rows"] * 2),
            "限流动作": "record_only",
            "生效时段": time.strftime("%Y-%m-%d"),
            "status": "已生效",
            "pending": False,
            "abnormal": cost["estimated_rows"] > 100000,
        }
        cost_rows.append(cost_entry)

        return {"ok": True, "cost": cost, "cost_entry_id": cost_id}


# ---------- 跨模块辅助函数 ----------

def _infer_pushdown_strategy(sources: list[str], available: list[dict[str, Any]]) -> str:
    """根据数据源类型推导谓词下推策略。"""
    source_types = []
    for s in sources:
        for a in available:
            if a.get("源编号") == s:
                source_types.append(a.get("源类型", "unknown"))
                break

    if any(t in ("mysql", "postgresql") for t in source_types):
        return "FULL_PUSHDOWN"
    elif any(t in ("clickhouse", "hive") for t in source_types):
        return "PARTIAL_PUSHDOWN"
    else:
        return "NO_PUSHDOWN"


def _infer_join_keys(sources: list[str], available: list[dict[str, Any]]) -> str:
    """从不同数据源推导关联键。"""
    keys = []
    for s in sources:
        for a in available:
            if a.get("源编号") == s:
                st = a.get("源类型", "unknown")
                if st in ("mysql", "postgresql"):
                    keys.append("user_id")
                elif st in ("clickhouse", "hive"):
                    keys.append("uid")
                else:
                    keys.append("id")
    return " = ".join(keys[:2]) if len(keys) >= 2 else keys[0] if keys else "id"


def _estimate_cost(sources: list[str], query_type: str, pushdown: str) -> dict[str, Any]:
    """估算查询代价（行数、IO、CPU）。"""
    base_rows = random.randint(1000, 50000)
    multiplier = len(sources)
    if pushdown == "FULL_PUSHDOWN":
        multiplier *= 0.3
    elif pushdown == "PARTIAL_PUSHDOWN":
        multiplier *= 0.6
    else:
        multiplier *= 1.5

    estimated = int(base_rows * multiplier)
    return {
        "estimated_rows": estimated,
        "estimated_io_mb": round(estimated * 0.001, 2),
        "estimated_cpu_ms": estimated // 10,
        "pushdown_strategy": pushdown,
        "query_type": query_type,
    }


def _get_applicable_rewrite_rules(plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = store.rows("query_rewrite")
    return [r for r in rows if r.get("status") == "已生效"]


def _apply_rewrite(plan: dict[str, Any], rules: list[dict[str, Any]]) -> str:
    if not rules:
        return f"SELECT * FROM federated({plan.get('参与来源', '')})"
    rewrites = [r.get("替换模式", "") for r in rules]
    return f"SELECT /* rewrite: {', '.join(rewrites)} */ * FROM federated({plan.get('参与来源', '')})"


def _simulate_federated_query(sources: list[str], strategy: str) -> list[dict[str, Any]]:
    """模拟联邦查询执行，生成真实的结果行。"""
    rows = []
    for i in range(random.randint(5, 50)):
        source_idx = i % len(sources)
        rows.append({
            "row_id": i + 1,
            "source": sources[source_idx],
            "value": random.randint(1, 10000),
            "category": f"cat_{random.randint(1, 10)}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "strategy": strategy,
        })
    return rows


def _cache_query_result(plan_id: int, results: list[dict[str, Any]]) -> dict[str, Any]:
    """回源后写入新的缓存批次，固化到期时刻与结果摘要作为同批凭据。"""
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(results)
    result_hash = hashlib.md5(data_str.encode()).hexdigest()[:12]
    batch_no = f"RESU-F{plan_id:04d}-B{cache_id}"
    expire_ts = time.time() + 3600
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-F{plan_id:04d}",
        "批次标识": batch_no,
        "查询摘要": _fingerprint(plan_id),
        "结果哈希": result_hash,
        # 命中次数初始为 0，只有实际交付命中时才递增
        "缓存命中": "0",
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_ts)),
        "存储大小": str(len(data_str)),
        "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "有效",
        "pending": True,
        "abnormal": False,
        "result_data": results,
        # 同批写入留存的到期时刻与结果摘要：到期列损坏时仍可凭此核验
        "batch_evidence": {
            "batch_no": batch_no,
            "expire_ts": expire_ts,
            "result_hash": result_hash,
            "written_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    }
    rows.append(cache)
    return cache


def _audit_cache_hit(
    plan_id: int, delivery: dict[str, Any], candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    """实际命中回写查询审计清单，保留所用批次来源与有效期证据。"""
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    evidence = delivery.get("validity_evidence", {})
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"FEDERATED_CACHE_HIT plan_id={plan_id} cache_id={delivery['cache_id']}",
        "执行耗时": "0ms",
        "扫描行数": "0",
        "返回行数": str(delivery.get("row_count", len(delivery.get("data", [])))),
        "数据来源": "result_cache",
        "操作用户": "federation_engine",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "缓存命中",
        "pending": False,
        "abnormal": False,
        # 保留所用的批次来源与核验证据，可复核
        "批次标识": delivery.get("批次标识"),
        "cache_id": delivery["cache_id"],
        "validity_evidence": evidence,
        "candidate_verdicts": [
            {"cache_id": c.get("cache_id"), "verdict": c.get("verdict"), "reason": c.get("reason")}
            for c in candidates
        ],
    }
    rows.append(audit)
    return audit


def _audit_federated_query(
    plan_id: int,
    elapsed_ms: float,
    result_count: int,
    miss_reason: str | None = None,
    new_cache_id: int | None = None,
) -> dict[str, Any]:
    """回源执行审计，与旧批缓存命中明确区分，记录未命中原因与新批次。"""
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"FEDERATED_QUERY plan_id={plan_id} miss_reason={miss_reason}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(result_count * 3),
        "返回行数": str(result_count),
        "数据来源": "query_federation",
        "操作用户": "federation_engine",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常" if elapsed_ms < 5000 else "慢查询",
        "pending": False,
        "abnormal": elapsed_ms > 30000,
        # 回源关联的是本次新执行与新写入批次，不是旧批命中
        "cache_hit": False,
        "miss_reason": miss_reason,
        "new_cache_id": new_cache_id,
    }
    rows.append(audit)
    return audit


def _update_cube_if_needed(plan_id: int, results: list[dict[str, Any]]) -> None:
    """如果查询结果关联到多维立方，则更新立方数据行数指标。"""
    cubes = store.rows("olap_cube")
    for cube in cubes:
        if cube.get("status") == "可用":
            vals = [r.get("value", 0) for r in results]
            if vals and "value" in str(results[0].keys()):
                old_val = cube.get("数据行数", "0")
                try:
                    new_total = int(old_val) + sum(vals) if old_val.isdigit() else sum(vals)
                except (ValueError, TypeError):
                    new_total = sum(vals)
                cube["数据行数"] = str(new_total)


def _cache_key(plan_id: int) -> str:
    """稳定缓存键：与 result_cache 中写入的查询摘要一致，便于审计复核。"""
    return _fingerprint(plan_id)