"""结果缓存读取链路核验测试：坏到期值不放行、批次凭据回退、命中计数与联邦审计。"""
from __future__ import annotations

import copy
import hashlib
import time

import pytest

from app.services.query_federation import QueryFederationService, _fingerprint
from app.services.result_cache import ResultCacheService
from app.services.source_registry import SourceRegistryService
from app.store import store

TIME_FMT = "%Y-%m-%d %H:%M:%S"


@pytest.fixture
def isolated_tables():
    """每个用例前后隔离 result_cache / query_audit / query_federation 表。"""
    tables = ("result_cache", "query_audit", "query_federation")
    snapshot = {name: copy.deepcopy(store.rows(name)) for name in tables}
    yield
    for name in tables:
        current = store.rows(name)
        current.clear()
        current.extend(copy.deepcopy(snapshot[name]))


def _make_cache_entry(
    fingerprint: str,
    *,
    data: list | None = None,
    expire_value: object = "in_ttl",
    status: str = "有效",
    hits: str = "0",
    with_evidence: bool = False,
    batch_no: str | None = None,
    include_data: bool = True,
) -> dict:
    """构造一条可控的 result_cache 条目。expire_value 支持特殊值控制损坏形态。"""
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    result_data = data if data is not None else [{"row_id": 1, "v": cache_id}]
    result_hash = hashlib.md5(str(result_data).encode()).hexdigest()[:12]
    batch_no = batch_no or f"RESU-T{cache_id:04d}"

    if expire_value == "in_ttl":
        expire_ts = time.time() + 3600
        expire_str = time.strftime(TIME_FMT, time.localtime(expire_ts))
    elif expire_value == "expired":
        expire_ts = time.time() - 60
        expire_str = time.strftime(TIME_FMT, time.localtime(expire_ts))
    else:
        # 到期列损坏：字符串列无法解析；同批凭据仍可独立留存到期时刻
        expire_ts = None
        expire_str = expire_value  # 损坏值，如 "not-a-date"

    entry = {
        "id": cache_id,
        "条目编号": batch_no,
        "批次标识": batch_no,
        "查询摘要": fingerprint,
        "结果哈希": result_hash,
        "缓存命中": hits,
        "过期时间": expire_str,
        "存储大小": str(len(str(result_data))),
        "创建时间": time.strftime(TIME_FMT),
        "最后命中": time.strftime(TIME_FMT),
        "status": status,
        "pending": True,
        "abnormal": False,
    }
    if include_data:
        entry["result_data"] = result_data
    if with_evidence:
        # 同批凭据独立留存到期时刻，不依赖已损坏的到期列
        evidence_expire_ts = expire_ts if expire_ts is not None else time.time() + 3600
        entry["batch_evidence"] = {
            "batch_no": batch_no,
            "expire_ts": evidence_expire_ts,
            "result_hash": result_hash,
            "written_at": time.strftime(TIME_FMT),
        }
    rows.append(entry)
    return entry


class TestCorruptedExpiryRejected:
    def test_corrupted_expiry_value_does_not_hit(self, isolated_tables):
        """根因：读取入口解析时间失败时不得放行。"""
        svc = ResultCacheService()
        entry = _make_cache_entry("fp_corrupt", expire_value="###bad-timestamp###")

        verdict = svc.read_verifiable_cache("fp_corrupt")

        assert verdict["hit"] is False
        assert verdict["reason"] == "corrupted"
        assert verdict["candidates"][0]["verdict"] == "corrupted"
        # 坏条目不会多记一次命中
        assert store.find("result_cache", entry["id"])["缓存命中"] == "0"
        # 损坏条目标记异常便于排查
        assert store.find("result_cache", entry["id"])["abnormal"] is True
        # 旧接口同样返回 None
        assert svc.get_cached_result("fp_corrupt") is None

    def test_bad_entry_queued_first_does_not_shadow_valid_batch(self, isolated_tables):
        """先排入的坏条目抢在有效批次之前时，仍交付真正有效的批次。"""
        svc = ResultCacheService()
        fp = "fp_shadow"
        bad = _make_cache_entry(fp, expire_value="garbage")
        good_data = [{"row_id": i, "v": i * 10} for i in range(1, 4)]
        good = _make_cache_entry(fp, data=good_data)

        verdict = svc.read_verifiable_cache(fp)

        assert verdict["hit"] is True
        # 真正交付的是有效批次，而不是排在前面的坏条目
        assert verdict["delivery"]["cache_id"] == good["id"]
        assert verdict["delivery"]["data"] == good_data
        # 候选清单保留两个条目的核验结论，可复核
        verdicts = {c["cache_id"]: c["verdict"] for c in verdict["candidates"]}
        assert verdicts[bad["id"]] == "corrupted"
        assert verdicts[good["id"]] == "valid"
        # 只有实际交付的有效条目计一次命中，坏条目不计数
        assert store.find("result_cache", good["id"])["缓存命中"] == "1"
        assert store.find("result_cache", bad["id"])["缓存命中"] == "0"

    def test_repeat_submission_only_counts_actual_hits(self, isolated_tables):
        """重复提交只计实际给出的命中。"""
        svc = ResultCacheService()
        fp = "fp_repeat"
        good = _make_cache_entry(fp, data=[{"k": 1}])

        first = svc.read_verifiable_cache(fp)
        second = svc.read_verifiable_cache(fp)
        assert first["hit"] and second["hit"]
        assert store.find("result_cache", good["id"])["缓存命中"] == "2"

        # 损坏条目无论读多少次都不计数
        bad = _make_cache_entry("fp_never", expire_value="broken")
        for _ in range(3):
            v = svc.read_verifiable_cache("fp_never")
            assert v["hit"] is False
        assert store.find("result_cache", bad["id"])["缓存命中"] == "0"


class TestBatchEvidenceFallback:
    def test_corrupted_column_recovered_by_same_batch_evidence(self, isolated_tables):
        """到期列损坏，但同批写入留存了到期时刻与结果摘要时仍可命中。"""
        svc = ResultCacheService()
        fp = "fp_evidence"
        entry = _make_cache_entry(
            fp, data=[{"row_id": 9}], expire_value="CORRUPTED", with_evidence=True
        )

        verdict = svc.read_verifiable_cache(fp)

        assert verdict["hit"] is True
        delivery = verdict["delivery"]
        assert delivery["cache_id"] == entry["id"]
        evidence = delivery["validity_evidence"]
        # 保留本次读取核验过的有效期证据
        assert evidence["expire_source"] == "batch_evidence"
        assert evidence["remaining_seconds"] > 0
        assert evidence["query_fingerprint"] == fp
        check = next(c for c in verdict["candidates"] if c["cache_id"] == entry["id"])
        assert check["expire_source"] == "batch_evidence"

    def test_missing_expiry_basis_is_rejected(self, isolated_tables):
        """缺少到期依据的凭据不得命中。"""
        svc = ResultCacheService()
        entry = _make_cache_entry("fp_no_evidence", expire_value="CORRUPTED", with_evidence=False)

        verdict = svc.read_verifiable_cache("fp_no_evidence")

        assert verdict["hit"] is False
        assert verdict["reason"] == "corrupted"
        assert store.find("result_cache", entry["id"])["缓存命中"] == "0"

    def test_evidence_hash_mismatch_is_rejected(self, isolated_tables):
        """留存摘要与实际结果哈希不匹配时，同批凭据不可用。"""
        svc = ResultCacheService()
        entry = _make_cache_entry(
            "fp_hash_mismatch", expire_value="CORRUPTED", with_evidence=True
        )
        # 篡改结果哈希，使同批凭据摘要对不上
        entry["结果哈希"] = "tamperedhash"

        verdict = svc.read_verifiable_cache("fp_hash_mismatch")
        assert verdict["hit"] is False
        assert verdict["reason"] == "corrupted"


class TestMissReasons:
    def test_absent_expired_and_empty_are_distinguished(self, isolated_tables):
        """缺席、超时和成功的空结果要区分。"""
        svc = ResultCacheService()

        # 缺席：根本没有候选
        absent = svc.read_verifiable_cache("fp_absent")
        assert absent["hit"] is False and absent["reason"] == "absent"
        assert absent["candidates"] == []

        # 超时：到期时间可解析但已过期
        _make_cache_entry("fp_expired", expire_value="expired")
        expired = svc.read_verifiable_cache("fp_expired")
        assert expired["hit"] is False and expired["reason"] == "expired"
        assert expired["candidates"][0]["verdict"] == "expired"

        # 空结果：数据为空列表，空态应能说明原因
        _make_cache_entry("fp_empty", data=[])
        empty = svc.read_verifiable_cache("fp_empty")
        assert empty["hit"] is False and empty["reason"] == "empty"
        assert "空" in empty["candidates"][0]["reason"]

        # 数据载体整体缺失
        _make_cache_entry("fp_no_carrier", include_data=False)
        no_carrier = svc.read_verifiable_cache("fp_no_carrier")
        assert no_carrier["hit"] is False and no_carrier["reason"] == "empty"

    def test_historical_invalid_status_markers_compatible(self, isolated_tables):
        """兼容历史缓存的失效标记：已失效/淘汰中等状态一律不可命中。"""
        svc = ResultCacheService()
        for marker in ("已过期", "已淘汰", "淘汰中", "失效", "已失效"):
            fp = f"fp_marker_{marker}"
            _make_cache_entry(fp, status=marker)
            verdict = svc.read_verifiable_cache(fp)
            assert verdict["hit"] is False, f"{marker} 不应命中"
            assert verdict["reason"] == "invalidated"


def _register_plan() -> int:
    """注册数据源并创建联邦计划，返回计划 id。"""
    source = SourceRegistryService()
    entry, msg = source.register_source({
        "源编号": f"SRC-{time.time_ns()}", "源类型": "mysql", "连接地址": "db://test/demo"})
    assert entry is not None, msg
    federation = QueryFederationService()
    plan, missing = federation.create_entry({
        "计划编号": f"QUER-T{entry['id']:04d}",
        "参与来源": entry["源编号"],
        "关联键": "id", "下推策略": "FULL_PUSHDOWN", "数据量估算": "1000",
        "并行度": "4", "超时上限": "30s", "结果合并": "DIRECT"})
    assert plan is not None, missing
    return plan["id"]


class TestFederationCacheFlow:
    def test_first_execution_fetches_back_and_second_hits_cache(self, isolated_tables):
        """联邦查询先回源写入新批次，再次执行命中缓存，审计区分两种关联。"""
        federation = QueryFederationService()
        plan_id = _register_plan()
        fp = _fingerprint(plan_id)

        # 首次执行：缓存缺席 -> 回源
        first = federation.execute_federation_query(plan_id)
        assert first["ok"] and first["cache_hit"] is False
        assert first["miss_reason"] == "absent"
        back_audit = store.find("query_audit", first["audit_id"])
        assert back_audit["数据来源"] == "query_federation"
        assert back_audit["cache_hit"] is False
        assert back_audit["new_cache_id"] == first["cache_id"]

        written = store.find("result_cache", first["cache_id"])
        assert written["查询摘要"] == fp
        assert written["batch_evidence"]["result_hash"] == written["结果哈希"]
        # 新写入批次命中次数初始为 0，回源本身不算命中
        assert written["缓存命中"] == "0"
        back_rows = first["results"]

        # 再次执行：命中同一批次，交付实际行值
        second = federation.execute_federation_query(plan_id)
        assert second["ok"] and second["cache_hit"] is True
        assert second["cache_id"] == first["cache_id"]
        assert second["批次标识"] == written["批次标识"]
        assert second["rows_returned"] == len(written["result_data"])
        assert second["results"] == written["result_data"][:10]
        assert second["results"] == back_rows
        # 命中审计保留批次来源与有效期证据
        hit_audit = store.find("query_audit", second["audit_id"])
        assert hit_audit["数据来源"] == "result_cache"
        assert hit_audit["status"] == "缓存命中"
        assert hit_audit["批次标识"] == written["批次标识"]
        assert hit_audit["validity_evidence"]["expire_source"] == "expire_column"
        # 只有实际命中才计数一次
        assert store.find("result_cache", first["cache_id"])["缓存命中"] == "1"

    def test_corrupted_entry_ahead_delivers_valid_batch_without_extra_hits(self, isolated_tables):
        """坏条目排在有效结果前面时，核对真正交付的批次与命中次数。"""
        federation = QueryFederationService()
        plan_id = _register_plan()
        fp = _fingerprint(plan_id)

        # 先回源拿到一个有效批次
        first = federation.execute_federation_query(plan_id)
        valid_id = first["cache_id"]
        valid_entry = store.find("result_cache", valid_id)
        valid_hits_before = int(valid_entry["缓存命中"])

        # 把到期列损坏的坏条目插到有效批次前面
        bad = _make_cache_entry(fp, expire_value="###corrupt###", hits="0")
        cache_rows = store.rows("result_cache")
        cache_rows.remove(bad)
        valid_index = next(i for i, r in enumerate(cache_rows) if r["id"] == valid_id)
        cache_rows.insert(valid_index, bad)

        result = federation.execute_federation_query(plan_id)

        assert result["cache_hit"] is True
        assert result["cache_id"] == valid_id  # 真正交付有效批次
        assert result["results"] == valid_entry["result_data"][:10]
        # 坏条目未被计数，有效条目仅 +1
        assert store.find("result_cache", bad["id"])["缓存命中"] == "0"
        assert int(store.find("result_cache", valid_id)["缓存命中"]) == valid_hits_before + 1
        # 本次没有产生新的回源审计
        source_audits = [a for a in store.rows("query_audit")
                         if a.get("数据来源") == "query_federation"
                         and f"plan_id={plan_id}" in a.get("查询语句", "")]
        assert len(source_audits) == 1

    def test_remove_valid_entry_retry_with_and_without_credential(self, isolated_tables):
        """移除有效条目后：有同批凭据仍可用，缺到期依据则回源，对照行值与回源记录。"""
        federation = QueryFederationService()
        plan_id = _register_plan()
        fp = _fingerprint(plan_id)

        first = federation.execute_federation_query(plan_id)
        original_id = first["cache_id"]
        original = store.find("result_cache", original_id)
        original_rows = original["result_data"]
        evidence_copy = copy.deepcopy(original["batch_evidence"])

        def federation_source_audits():
            return [a for a in store.rows("query_audit")
                    if a.get("数据来源") == "query_federation"
                    and f"plan_id={plan_id}" in a.get("查询语句", "")]

        # 场景一：移除有效条目，仅保留到期列损坏且缺少到期依据的凭据 -> 回源
        cache_rows = store.rows("result_cache")
        cache_rows.remove(original)
        orphan = _make_cache_entry(
            fp, data=original_rows, expire_value="DAMAGED", with_evidence=False)

        retry_miss = federation.execute_federation_query(plan_id)
        assert retry_miss["cache_hit"] is False
        assert retry_miss["miss_reason"] == "corrupted"
        # 回源产生新的执行与新批次，审计关联的是新执行而非旧批命中
        assert len(federation_source_audits()) == 2
        new_batch = store.find("result_cache", retry_miss["cache_id"])
        assert new_batch["id"] != orphan["id"]
        # 缺少到期依据的坏条目未计命中
        assert store.find("result_cache", orphan["id"])["缓存命中"] == "0"

        # 场景二：移除新有效批次，用仍在期内的原始写入凭据重试 -> 命中旧数据
        store.rows("result_cache").remove(new_batch)
        orphan["batch_evidence"] = evidence_copy  # 补回同批写入凭据
        # 凭据里的结果摘要与条目哈希一致才能领取
        assert evidence_copy["result_hash"] == orphan["结果哈希"]

        retry_hit = federation.execute_federation_query(plan_id)
        assert retry_hit["cache_hit"] is True
        assert retry_hit["cache_id"] == orphan["id"]
        assert retry_hit["results"] == original_rows[:10]
        assert retry_hit["validity_evidence"]["expire_source"] == "batch_evidence"
        # 回源记录没有再增加，命中只计一次
        assert len(federation_source_audits()) == 2
        assert store.find("result_cache", orphan["id"])["缓存命中"] == "1"
