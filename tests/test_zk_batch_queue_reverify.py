"""VerificationQueue.reverify_outcome 失败复核对账测试。

覆盖：
* 对账范围严格沿用 reverify_failures 的选取，selected/items/归类列表均按
  源批次原序（不随 result.failures 的 proof_id 排序）；
* before_status 固定 failed、before_* 取源作业失败定位；恢复项
  after_status=passed 且定位为空；失败项 after_* 取复核 Failure；
  outcome 仅 recovered/still_failed；
* to_dict 固定键序（报告与明细）；
* 查询只读已保存结果：不调用验证器、不创建作业、不改状态，重复查询一致；
* UnknownJobError（含执行异常移除的复核作业）/ ResultUnavailableError
  （queued/cancelled）/ ReverifyLineageMismatchError（普通作业、他人复核、
  间接血缘、自己对自己）三类异常互不替代，检查顺序固定；
* 重复复核得到不同 job_id，父子关系互不覆盖；
* 报告不含 proof、public_inputs 或验证器内部信息；
* 现有分组验证、report、reverify_failures 与逐项任务流行为不受影响。

直接运行：python tests/test_zk_batch_queue_reverify.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    IncompatibleAggregationError,
    ReverifyLineageMismatchError,
    ReverifyOutcomeItem,
    ReverifyOutcomeReport,
    ResultUnavailableError,
    UnknownJobError,
    VerificationQueue,
    ZKVerifier,
)


def proof(pid, proto="groth16", circuit="c1", key="k1", inputs=None, body=None):
    return {
        "proof_id": pid,
        "protocol": proto,
        "circuit_id": circuit,
        "aggregation_key": key,
        "public_inputs": inputs if inputs is not None else ["pi"],
        "proof": body if body is not None else ("blob", pid),
    }


class StubVerifier(ZKVerifier):
    """可编排行为的假验证器（与队列报告测试同款）。"""

    protocol = "groth16"

    def __init__(
        self,
        aggregate_ok=True,
        reject_ids=(),
        error_ids=(),
        agg_verify=True,
        agg_error=None,
    ):
        self._aggregate_ok = aggregate_ok
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._agg_verify = agg_verify
        self._agg_error = agg_error
        self.calls = {"aggregate": 0, "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        if not self._aggregate_ok:
            raise IncompatibleAggregationError("cannot aggregate this group")
        return ["AGG"]

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        if self._agg_error is not None:
            raise RuntimeError(self._agg_error)
        return self._agg_verify

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        if p.proof_id in self._errors:
            raise ValueError("bad proof bytes")
        return p.proof_id not in self._reject


def _run(q, batch, verifier):
    j = q.enqueue(batch, verifier)
    q.run_next()
    return j


# ================================================================ 对账内容

class TestReverifyOutcomeContent(unittest.TestCase):
    def test_all_recovered_locations_and_key_order(self):
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"p2"}, error_ids={"p3"}
        )
        q = VerificationQueue(flaky)
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("p1", key="k1"),
                proof("p2", key="k1"),
                proof("p3", key="k2"),
                proof("p4", key="k2"),
                proof("p5", key="k2"),
            ],
        }
        j = _run(q, batch, flaky)
        retry_v = StubVerifier()
        rj = q.reverify_failures(j, retry_v)
        q.run_next()

        report = q.reverify_outcome(j, rj)
        self.assertIsInstance(report, ReverifyOutcomeReport)
        self.assertEqual(report.source_job_id, j)
        self.assertEqual(report.retry_job_id, rj)
        # 列表按源批次原序 p2,p3（不是 result.failures 的 proof_id 排序结果，
        # 本例恰好相同，顺序用例另见 test_order_follows_source_batch）。
        self.assertEqual(report.selected_proof_ids, ["p2", "p3"])
        self.assertEqual(report.recovered_proof_ids, ["p2", "p3"])
        self.assertEqual(report.still_failed_proof_ids, [])
        self.assertEqual(len(report.items), 2)
        for item in report.items:
            self.assertIsInstance(item, ReverifyOutcomeItem)
            self.assertEqual(item.outcome, "recovered")
            self.assertEqual(item.before_status, "failed")
            self.assertEqual(item.after_status, "passed")
            # 恢复项复核定位为空
            self.assertEqual(
                (item.after_stage, item.after_code, item.after_message),
                ("", "", ""),
            )

        by_id = {i.proof_id: i for i in report.items}
        # rejected 与 verify_error 两种源定位原样保留
        self.assertEqual(
            (by_id["p2"].before_stage, by_id["p2"].before_code,
             by_id["p2"].before_message),
            ("single_verify", "rejected", "proof rejected by verifier"),
        )
        self.assertEqual(
            (by_id["p3"].before_stage, by_id["p3"].before_code,
             by_id["p3"].before_message),
            ("single_verify", "verify_error", "ValueError: bad proof bytes"),
        )

        payload = report.to_dict()
        self.assertEqual(
            list(payload.keys()),
            ["source_job_id", "retry_job_id", "selected_proof_ids", "items",
             "recovered_proof_ids", "still_failed_proof_ids"],
        )
        self.assertEqual(
            list(payload["items"][0].keys()),
            ["proof_id", "outcome", "before_status", "before_stage",
             "before_code", "before_message", "after_status",
             "after_stage", "after_code", "after_message"],
        )
        self.assertEqual(
            payload["items"][0],
            {
                "proof_id": "p2",
                "outcome": "recovered",
                "before_status": "failed",
                "before_stage": "single_verify",
                "before_code": "rejected",
                "before_message": "proof rejected by verifier",
                "after_status": "passed",
                "after_stage": "",
                "after_code": "",
                "after_message": "",
            },
        )

    def test_mixed_outcome_still_failed_takes_retry_failure(self):
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"p2"}, error_ids={"p3"}
        )
        q = VerificationQueue(flaky)
        j = _run(
            q,
            {"batch_id": "B", "proofs": [
                proof("p1", key="k1"), proof("p2", key="k1"),
                proof("p3", key="k2"), proof("p4", key="k2"),
            ]},
            flaky,
        )
        # 复核：p2 所在组聚合直接通过；p3 单独成组、聚合被拒后回退单证仍拒
        class RetryVerifier(StubVerifier):
            def verify_aggregate(self, proofs, aggregated):
                return proofs[0].aggregation_key != "k2"

        retry_v = RetryVerifier(reject_ids={"p3"})
        rj = q.reverify_failures(j, retry_v)
        q.run_next()

        report = q.reverify_outcome(j, rj)
        self.assertEqual(report.selected_proof_ids, ["p2", "p3"])
        self.assertEqual(report.recovered_proof_ids, ["p2"])
        self.assertEqual(report.still_failed_proof_ids, ["p3"])
        self.assertEqual(
            [(i.proof_id, i.before_status, i.after_status, i.outcome)
             for i in report.items],
            [("p2", "failed", "passed", "recovered"),
             ("p3", "failed", "failed", "still_failed")],
        )
        still = report.items[1]
        self.assertEqual(
            (still.before_stage, still.before_code),
            ("single_verify", "verify_error"),
        )
        self.assertEqual(
            (still.after_stage, still.after_code, still.after_message),
            ("single_verify", "rejected", "proof rejected by verifier"),
        )

    def test_order_follows_source_batch_not_failure_sort(self):
        # 源序 z,a,m；result.failures 按 proof_id 排序为 a,m,z。
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"z", "m"}, error_ids={"a"}
        )
        q = VerificationQueue(flaky)
        j = _run(
            q, {"batch_id": "B", "proofs": [proof("z"), proof("a"), proof("m")]},
            flaky,
        )
        self.assertEqual(
            [f.proof_id for f in q.result(j).failures], ["a", "m", "z"]
        )
        rj = q.reverify_failures(j)  # 沿用 flaky：三证仍失败
        q.run_next()

        report = q.reverify_outcome(j, rj)
        self.assertEqual(report.selected_proof_ids, ["z", "a", "m"])
        self.assertEqual(report.recovered_proof_ids, [])
        self.assertEqual(report.still_failed_proof_ids, ["z", "a", "m"])
        self.assertEqual(
            [i.proof_id for i in report.items], ["z", "a", "m"]
        )
        self.assertEqual(
            [(i.proof_id, i.outcome) for i in report.items],
            [("z", "still_failed"), ("a", "still_failed"),
             ("m", "still_failed")],
        )

    def test_group_wide_failure_before_location(self):
        # 单证全部通过但聚合验证被拒：失败定位在 aggregate_verify，
        # 对账的 before_* 取该 Failure。
        v = StubVerifier(agg_verify=False)
        q = VerificationQueue(v)
        j = _run(
            q,
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            v,
        )
        failures = {f.proof_id: f for f in q.result(j).failures}
        self.assertEqual(failures["p1"].stage, "aggregate_verify")
        rj = q.reverify_failures(j, StubVerifier())
        q.run_next()

        report = q.reverify_outcome(j, rj)
        self.assertEqual(report.selected_proof_ids, ["p1", "p2"])
        item = report.items[0]
        self.assertEqual(item.before_status, "failed")
        self.assertEqual(item.before_stage, "aggregate_verify")
        self.assertEqual(item.before_code, "rejected")
        self.assertEqual(item.outcome, "recovered")
        self.assertEqual(item.after_status, "passed")


# ================================================================ 只读/幂等

class TestReverifyOutcomeReadOnly(unittest.TestCase):
    def test_repeated_query_consistent_and_side_effect_free(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        j = _run(
            q, {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            flaky,
        )
        retry_v = StubVerifier()
        rj = q.reverify_failures(j, retry_v)
        q.run_next()
        calls_after_run = (
            retry_v.calls["aggregate"],
            retry_v.calls["verify_aggregate"],
            list(retry_v.calls["verify"]),
        )
        source_result = q.result(j)
        source_report = q.report(j)

        r1 = q.reverify_outcome(j, rj)
        r2 = q.reverify_outcome(j, rj)
        self.assertEqual(r1.to_dict(), r2.to_dict())
        # 不调用验证器
        self.assertEqual(
            (retry_v.calls["aggregate"],
             retry_v.calls["verify_aggregate"],
             retry_v.calls["verify"]),
            calls_after_run,
        )
        # 不创建作业、不改状态或已保存结果
        self.assertIsNone(q.run_next())
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.status(rj), "completed")
        self.assertIs(q.result(j), source_result)
        self.assertIs(q.report(j), source_report)
        self.assertEqual(q.result(rj).failed, 0)

    def test_no_proof_material_or_verifier_internals(self):
        secret_inputs = ["SECRET-INPUT-xyz"]
        secret_proof = {"secret": "SECRET-PROOF-abc"}
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"p2"}, error_ids={"p1"}
        )
        q = VerificationQueue(flaky)
        j = _run(
            q,
            {"batch_id": "B", "proofs": [
                proof("p1", inputs=secret_inputs, body=secret_proof),
                proof("p2", inputs=secret_inputs, body=secret_proof),
            ]},
            flaky,
        )
        rj = q.reverify_failures(j, StubVerifier())
        q.run_next()
        text = repr(q.reverify_outcome(j, rj).to_dict())
        self.assertNotIn("SECRET-INPUT-xyz", text)
        self.assertNotIn("SECRET-PROOF-abc", text)
        self.assertNotIn("public_inputs", text)
        self.assertNotIn("Traceback", text)


# ================================================================ 异常约定

class TestReverifyOutcomeErrors(unittest.TestCase):
    def test_unknown_job(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        j = _run(
            q, {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            flaky,
        )
        rj = q.reverify_failures(j, StubVerifier())
        q.run_next()
        with self.assertRaises(UnknownJobError):
            q.reverify_outcome("ghost", rj)
        with self.assertRaises(UnknownJobError):
            q.reverify_outcome(j, "ghost")

    def test_not_completed_raises_unavailable(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p1"})
        q = VerificationQueue(flaky)
        j = _run(
            q, {"batch_id": "B", "proofs": [proof("p1")]}, flaky
        )
        # 复核作业仍 queued：血缘已记录但结果不可用
        rj = q.reverify_failures(j, StubVerifier())
        with self.assertRaises(ResultUnavailableError):
            q.reverify_outcome(j, rj)
        # 取消后同样不可用，查询不改状态
        q.cancel(rj)
        with self.assertRaises(ResultUnavailableError):
            q.reverify_outcome(j, rj)
        self.assertEqual(q.status(rj), "cancelled")
        # 源作业未完成（未知性先于状态；这里源作业存在且 queued）
        other = q.enqueue({"batch_id": "B2", "proofs": [proof("p2")]})
        with self.assertRaises(ResultUnavailableError):
            q.reverify_outcome(other, j)

    def test_lineage_mismatch_cases(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p1", "x1"})
        q = VerificationQueue(flaky)
        j = _run(q, {"batch_id": "B", "proofs": [proof("p1")]}, flaky)
        other = _run(
            q, {"batch_id": "B2", "proofs": [proof("x1")]}, flaky
        )
        # 普通提交的作业不是复核作业
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(j, other)
        # 自己对自己
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(j, j)
        # 别的源作业的复核作业
        other_retry = q.reverify_failures(other, StubVerifier())
        q.run_next()
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(j, other_retry)
        # 间接血缘：reverify 的 reverify 不是源作业的直接复核作业
        first = q.reverify_failures(j, flaky)
        q.run_next()
        second = q.reverify_failures(first, StubVerifier())
        q.run_next()
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(j, second)
        # 直接血缘可对账
        direct = q.reverify_outcome(first, second)
        self.assertEqual(direct.source_job_id, first)
        self.assertEqual(direct.retry_job_id, second)

    def test_error_precedence_unknown_before_state_before_lineage(self):
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p1"})
        )
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        # 源未知 -> UnknownJobError（即便复核 id 也不存在/无血缘）
        with self.assertRaises(UnknownJobError):
            q.reverify_outcome("ghost", "ghost2")
        q.run_next()
        rj = q.reverify_failures(j, StubVerifier())
        # 复核作业 queued：状态检查先于血缘不匹配的可能
        with self.assertRaises(ResultUnavailableError):
            q.reverify_outcome(j, rj)

    def test_execution_error_propagates_and_breaks_lineage(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p1"})
        q = VerificationQueue(flaky)
        j = _run(q, {"batch_id": "B", "proofs": [proof("p1")]}, flaky)
        rj = q.reverify_failures(j, StubVerifier(aggregate_ok=False))
        # 执行异常按 run_next 传播，复核作业移除（含父子记录）
        with self.assertRaises(IncompatibleAggregationError):
            q.run_next()
        with self.assertRaises(UnknownJobError):
            q.reverify_outcome(j, rj)
        # 源作业不受影响
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.result(j).failed, 1)


# ================================================================ 重复复核

class TestRepeatedReverifyLineage(unittest.TestCase):
    def test_repeated_reverifies_get_distinct_jobs_with_independent_lineage(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        j = _run(
            q, {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            flaky,
        )
        # 第一次复核仍失败
        r1 = q.reverify_failures(j, flaky)
        q.run_next()
        # 第二次复核换用修复验证器，全部恢复
        r2 = q.reverify_failures(j, StubVerifier())
        q.run_next()
        self.assertNotIn(r1, (j, r2))
        self.assertNotEqual(r1, r2)

        report1 = q.reverify_outcome(j, r1)
        report2 = q.reverify_outcome(j, r2)
        self.assertEqual(report1.retry_job_id, r1)
        self.assertEqual(report2.retry_job_id, r2)
        self.assertEqual(
            [(i.proof_id, i.outcome) for i in report1.items],
            [("p2", "still_failed")],
        )
        self.assertEqual(
            [(i.proof_id, i.outcome) for i in report2.items],
            [("p2", "recovered")],
        )
        # 两条父子关系互不覆盖：反向交叉血缘不成立
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(r1, r2)
        # 源作业结果始终不变
        self.assertEqual(
            [f.proof_id for f in q.result(j).failures], ["p2"]
        )

    def test_reverify_chain_each_link_directly_reconcilable(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        j = _run(
            q, {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            flaky,
        )
        j2 = q.reverify_failures(j, flaky)
        q.run_next()
        j3 = q.reverify_failures(j2, StubVerifier())
        q.run_next()
        # 每一段直接血缘都可独立对账
        link1 = q.reverify_outcome(j, j2)
        link2 = q.reverify_outcome(j2, j3)
        self.assertEqual(
            [i.outcome for i in link1.items], ["still_failed"]
        )
        self.assertEqual(
            [i.outcome for i in link2.items], ["recovered"]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
