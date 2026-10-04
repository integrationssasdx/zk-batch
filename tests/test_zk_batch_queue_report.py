"""VerificationQueue.report 入口测试。

覆盖：
* completed 作业返回 BatchVerificationReport，与 result 同一次执行
  （``report.result is result(job_id)``），字段与各对象 to_dict 一致；
* 查询报告不再次调用验证器、不改变作业状态、重复查询同一对象；
* groups/逐证明细/消息脱敏/固定键序沿用 verify_batch_detailed 口径；
* queued/running/cancelled 抛 ResultUnavailableError，未知或因执行错误
  移除的作业抛 UnknownJobError；
* 执行错误（空批次/字段/重复 ID/未知系统/契约/不能聚合）传播并移除作业；
* 零失败作业也有完整报告；
* reverify_failures 新作业可 report，源作业结果与报告不变，复核只含原
  失败 proof_id 且保持批次顺序。

直接运行：python tests/test_zk_batch_queue_report.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchVerificationReport,
    BatchVerificationResult,
    CancelledJobError,
    DuplicateProofIdError,
    EmptyBatchError,
    GroupVerificationReport,
    IncompatibleAggregationError,
    InvalidProofError,
    ProofVerificationDetail,
    ResultUnavailableError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerificationQueue,
    VerifierContractError,
    ZKVerifier,
    verify_batch_detailed,
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
    """可编排行为的假验证器（与详细报告测试同款）。"""

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


class IncompleteVerifier(ZKVerifier):
    protocol = "halo2"
    # 三个方法都不实现


# ================================================================ 基本同值

class TestReportBasics(unittest.TestCase):
    def test_completed_returns_report_with_shared_result(self):
        v = StubVerifier()
        q = VerificationQueue(v)
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        q.run_next()

        report = q.report(j)
        self.assertIsInstance(report, BatchVerificationReport)
        self.assertIsInstance(report.result, BatchVerificationResult)
        # 同一次执行：报告内 result 与 result() 是同一对象
        self.assertIs(report.result, q.result(j))

    def test_report_result_fields_match_result(self):
        batch = {
            "batch_id": "B-9",
            "proofs": [
                proof("p1", key="k1"),
                proof("p2", key="k2"),
                proof("p3", key="k1"),
            ],
        }
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p3"}, error_ids={"p2"})
        )
        j = q.enqueue(batch)
        q.run_next()

        report = q.report(j)
        result = q.result(j)
        for name in ("batch_id", "aggregate_count", "passed", "failed"):
            self.assertEqual(getattr(report.result, name), getattr(result, name))
        self.assertEqual(report.result.failures, result.failures)
        self.assertEqual(report.result.batch_id, "B-9")
        self.assertEqual(report.result.aggregate_count, 2)
        self.assertEqual((report.result.passed, report.result.failed), (1, 2))
        self.assertEqual(
            [f.proof_id for f in report.result.failures], ["p2", "p3"]
        )
        # 各对象 to_dict 结果一致
        self.assertEqual(report.result.to_dict(), result.to_dict())
        self.assertEqual(
            report.to_dict()["result"], result.to_dict()
        )

    def test_groups_match_detailed_pipeline(self):
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("z1", key="k2"),
                proof("a1", key="k1"),
                proof("m2", key="k1"),
            ],
        }
        v = StubVerifier(agg_verify=False, reject_ids={"a1"})
        q = VerificationQueue(v)
        j = q.enqueue(batch)
        q.run_next()

        # 与直接跑详细流水线（独立同构验证器）的报告结构一致
        expected = verify_batch_detailed(
            batch, StubVerifier(agg_verify=False, reject_ids={"a1"})
        )
        report = q.report(j)
        self.assertEqual(report.to_dict(), expected.to_dict())
        # 组按首次出现序，组内按批次原序
        self.assertEqual(
            [g.group_id for g in report.groups],
            ["groth16:c1:k2", "groth16:c1:k1"],
        )
        self.assertIsInstance(report.groups[0], GroupVerificationReport)
        self.assertIsInstance(report.groups[0].proofs[0], ProofVerificationDetail)

    def test_all_group_scenarios_in_queue_report(self):
        # 聚合成功 / 聚合验证异常回退 / 回退后单证失败，三种组并存
        class StageVerifier(StubVerifier):
            def aggregate(self, proofs):
                if proofs[0].aggregation_key == "k-agg-err":
                    raise RuntimeError("aggregator down")
                return super().aggregate(proofs)

            def verify_aggregate(self, proofs, aggregated):
                if proofs[0].aggregation_key == "k-fail":
                    return False
                return True

            def verify(self, p):
                return p.proof_id != "p3"

        batch = {
            "batch_id": "B",
            "proofs": [
                proof("p1", key="k-ok"),
                proof("p2", key="k-agg-err"),
                proof("p3", key="k-fail"),
            ],
        }
        q = VerificationQueue(StageVerifier())
        j = q.enqueue(batch)
        q.run_next()
        groups = {g.group_id: g for g in q.report(j).groups}

        g_ok = groups["groth16:c1:k-ok"]
        self.assertEqual(g_ok.aggregate_call_status, "succeeded")
        self.assertEqual(g_ok.aggregate_verify_status, "passed")
        self.assertFalse(g_ok.fell_back)
        self.assertEqual([d.status for d in g_ok.proofs], ["passed"])

        g_agg_err = groups["groth16:c1:k-agg-err"]
        self.assertEqual(g_agg_err.aggregate_call_status, "error")
        self.assertEqual(g_agg_err.aggregate_verify_status, "not_run")
        self.assertFalse(g_agg_err.fell_back)
        self.assertEqual([d.status for d in g_agg_err.proofs], ["not_run"])
        self.assertEqual(
            g_agg_err.proofs[0].message, "RuntimeError: aggregator down"
        )

        g_fail = groups["groth16:c1:k-fail"]
        self.assertEqual(g_fail.aggregate_call_status, "succeeded")
        self.assertEqual(g_fail.aggregate_verify_status, "rejected")
        self.assertTrue(g_fail.fell_back)
        self.assertEqual(
            [(d.proof_id, d.status, d.message) for d in g_fail.proofs],
            [("p3", "rejected", "proof rejected by verifier")],
        )

    def test_zero_failure_completed_job_has_full_report(self):
        batch = {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]}
        q = VerificationQueue(StubVerifier())
        j = q.enqueue(batch)
        q.run_next()

        report = q.report(j)
        self.assertEqual(report.result.failed, 0)
        self.assertEqual(report.result.failures, [])
        self.assertEqual(report.result.passed, 2)
        self.assertEqual(len(report.groups), 1)
        g = report.groups[0]
        self.assertEqual(g.aggregate_verify_status, "passed")
        # 零失败时逐证状态仍准确
        self.assertEqual(
            [(d.proof_id, d.status) for d in g.proofs],
            [("p1", "passed"), ("p2", "passed")],
        )
        self.assertEqual(
            report.to_dict()["result"]["failures"], []
        )


# ================================================================ 无副作用

class TestReportNoSideEffects(unittest.TestCase):
    def test_report_does_not_call_verifier_or_change_state(self):
        v = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(v)
        j = q.enqueue(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]}
        )
        q.run_next()
        calls_after_run = dict(v.calls)
        calls_after_run["verify"] = list(v.calls["verify"])

        report1 = q.report(j)
        report2 = q.report(j)
        # 不再次调用验证器
        self.assertEqual(v.calls["aggregate"], calls_after_run["aggregate"])
        self.assertEqual(
            v.calls["verify_aggregate"], calls_after_run["verify_aggregate"]
        )
        self.assertEqual(v.calls["verify"], calls_after_run["verify"])
        # 不改变作业状态
        self.assertEqual(q.status(j), "completed")
        # 重复查询返回同一保存对象，不重新执行
        self.assertIs(report1, report2)
        self.assertIs(report1.result, q.result(j))

    def test_report_does_not_mutate_serialized_views(self):
        v = StubVerifier(agg_verify=False, reject_ids={"p2"}, error_ids={"p3"})
        q = VerificationQueue(v)
        j = q.enqueue(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2"), proof("p3")]}
        )
        q.run_next()
        before = q.result(j).to_dict()
        q.report(j).to_dict()
        q.report(j)
        self.assertEqual(q.result(j).to_dict(), before)


# ================================================================ 脱敏与键序

class TestReportSanitizationAndKeyOrder(unittest.TestCase):
    def test_messages_never_contain_proof_or_inputs(self):
        secret_inputs = ["SECRET-INPUT-xyz"]
        secret_proof = {"secret": "SECRET-PROOF-abc"}
        v = StubVerifier(agg_verify=False, reject_ids={"p1"}, error_ids={"p2"})
        q = VerificationQueue(v)
        j = q.enqueue(
            {
                "batch_id": "B",
                "proofs": [
                    proof("p1", inputs=secret_inputs, body=secret_proof),
                    proof("p2", inputs=secret_inputs, body=secret_proof),
                ],
            }
        )
        q.run_next()
        text = str(q.report(j).to_dict())
        self.assertNotIn("SECRET-INPUT-xyz", text)
        self.assertNotIn("SECRET-PROOF-abc", text)

    def test_fixed_key_order(self):
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p2"})
        )
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1"), proof("p2")]})
        q.run_next()
        payload = q.report(j).to_dict()
        self.assertEqual(list(payload.keys()), ["result", "groups"])
        self.assertEqual(
            list(payload["result"].keys()),
            ["batch_id", "aggregate_count", "passed", "failed", "failures"],
        )
        g = payload["groups"][0]
        self.assertEqual(
            list(g.keys()),
            [
                "group_id",
                "proof_ids",
                "aggregate_call_status",
                "aggregate_verify_status",
                "fell_back",
                "proofs",
            ],
        )
        self.assertEqual(
            list(g["proofs"][0].keys()), ["proof_id", "status", "message"]
        )


# ================================================================ 状态与异常

class TestReportStatesAndErrors(unittest.TestCase):
    def test_unknown_job_raises(self):
        q = VerificationQueue(StubVerifier())
        with self.assertRaises(UnknownJobError):
            q.report("ghost")

    def test_queued_running_cancelled_raise_unavailable(self):
        v = StubVerifier()
        q = VerificationQueue(v)
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})

        # queued
        with self.assertRaises(ResultUnavailableError):
            q.report(j)

        # running：在验证器内部查询
        def check_running(proofs, aggregated):
            with self.assertRaises(ResultUnavailableError):
                q.report(j)
            return True

        v.verify_aggregate = check_running
        q.run_next()
        self.assertEqual(q.status(j), "completed")

        # cancelled 的作业
        j2 = q.enqueue({"batch_id": "B2", "proofs": [proof("p2")]})
        q.cancel(j2)
        with self.assertRaises(ResultUnavailableError):
            q.report(j2)
        # 查询异常不改变状态
        self.assertEqual(q.status(j2), "cancelled")

    def _assert_execution_error_removes_job(self, batch, verifier, exc_type):
        q = VerificationQueue(verifier)
        j = q.enqueue(batch)
        with self.assertRaises(exc_type):
            q.run_next()
        # 执行错误移除作业：报告、结果、状态一律 UnknownJobError
        with self.assertRaises(UnknownJobError):
            q.report(j)
        with self.assertRaises(UnknownJobError):
            q.result(j)
        with self.assertRaises(UnknownJobError):
            q.status(j)

    def test_empty_batch_error_propagates_and_removes(self):
        self._assert_execution_error_removes_job(
            {"batch_id": "B", "proofs": []}, StubVerifier(), EmptyBatchError
        )

    def test_invalid_fields_error_propagates_and_removes(self):
        bad = proof("p1")
        del bad["circuit_id"]
        self._assert_execution_error_removes_job(
            {"batch_id": "B", "proofs": [bad]},
            StubVerifier(),
            InvalidProofError,
        )

    def test_duplicate_proof_id_error_propagates_and_removes(self):
        self._assert_execution_error_removes_job(
            {"batch_id": "B", "proofs": [proof("dup"), proof("dup")]},
            StubVerifier(),
            DuplicateProofIdError,
        )

    def test_unknown_system_error_propagates_and_removes(self):
        self._assert_execution_error_removes_job(
            {"batch_id": "B", "proofs": [proof("p1", proto="mystery")]},
            StubVerifier(),
            UnsupportedProofSystemError,
        )

    def test_contract_error_propagates_and_removes(self):
        self._assert_execution_error_removes_job(
            {"batch_id": "B", "proofs": [proof("p1", proto="halo2")]},
            IncompleteVerifier(),
            VerifierContractError,
        )

    def test_incompatible_aggregation_error_propagates_and_removes(self):
        self._assert_execution_error_removes_job(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            StubVerifier(aggregate_ok=False),
            IncompatibleAggregationError,
        )

    def test_unknown_job_precedence_over_state(self):
        # 未知 id 一律 UnknownJobError（而非 ResultUnavailableError）
        q = VerificationQueue(StubVerifier())
        with self.assertRaises(UnknownJobError):
            q.report("never-existed")

    def test_cancel_still_raises_cancelled_error(self):
        # report 的新增不改变 cancel 的异常约定
        q = VerificationQueue(StubVerifier())
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        q.cancel(j)
        with self.assertRaises(CancelledJobError):
            q.cancel(j)


# ================================================================ 复核作业

class TestReverifyReport(unittest.TestCase):
    def test_reverify_job_report_contains_only_failed_in_order(self):
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"p2"}, error_ids={"p3"}
        )
        q = VerificationQueue(flaky)
        proofs = [
            proof("p1", key="k1"),
            proof("p2", key="k1"),
            proof("p3", key="k2"),
            proof("p4", key="k2"),
            proof("p5", key="k2"),
        ]
        source_batch = {"batch_id": "B", "proofs": proofs}
        j = q.enqueue(source_batch)
        q.run_next()
        source_result = q.result(j)
        source_report = q.report(j)
        self.assertEqual(source_result.failed, 2)

        # 复核作业换用已修复的验证器
        new_j = q.reverify_failures(j, StubVerifier())
        q.run_next()
        new_report = q.report(new_j)

        # 复核只含原失败 proof_id，保持源批次相对顺序
        all_ids = [
            pid for g in new_report.groups for pid in g.proof_ids
        ]
        self.assertEqual(all_ids, ["p2", "p3"])
        self.assertEqual(new_report.result.batch_id, "B")
        self.assertEqual(new_report.result.failed, 0)
        # p2(k1) 与 p3(k2) 分成两组
        self.assertEqual(
            sorted(g.group_id for g in new_report.groups),
            ["groth16:c1:k1", "groth16:c1:k2"],
        )
        self.assertEqual(
            new_report.groups[0].proof_ids, ["p2"]
        )
        self.assertEqual(
            new_report.groups[1].proof_ids, ["p3"]
        )
        for g in new_report.groups:
            self.assertEqual(g.aggregate_verify_status, "passed")
            self.assertEqual(
                [d.status for d in g.proofs], ["passed"]
            )

        # 源作业普通结果与详细报告不变（同一对象）
        self.assertEqual(q.status(j), "completed")
        self.assertIs(q.result(j), source_result)
        self.assertIs(q.report(j), source_report)
        self.assertEqual(q.result(j).failed, 2)
        self.assertEqual(
            [f.proof_id for f in q.result(j).failures], ["p2", "p3"]
        )
        self.assertEqual(
            [g.group_id for g in q.report(j).groups],
            ["groth16:c1:k1", "groth16:c1:k2"],
        )

    def test_reverify_still_failing_report_detail(self):
        # 复核仍失败时，报告的组级状态与逐证明细准确
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"z", "m"}, error_ids={"a"}
        )
        q = VerificationQueue(flaky)
        j = q.enqueue(
            {"batch_id": "B", "proofs": [proof("z"), proof("a"), proof("m")]}
        )
        q.run_next()
        new_j = q.reverify_failures(j)  # 沿用队列默认 flaky
        q.run_next()

        report = q.report(new_j)
        self.assertEqual(report.result.failed, 3)
        g = report.groups[0]
        self.assertTrue(g.fell_back)
        self.assertEqual(g.aggregate_verify_status, "rejected")
        # 组内保持源批次顺序 z,a,m；result.failures 仍按 proof_id 排序
        self.assertEqual(
            [d.proof_id for d in g.proofs], ["z", "a", "m"]
        )
        self.assertEqual(
            [(d.proof_id, d.status) for d in g.proofs],
            [("z", "rejected"), ("a", "error"), ("m", "rejected")],
        )
        self.assertEqual(
            [f.proof_id for f in report.result.failures], ["a", "m", "z"]
        )

    def test_reverify_new_job_report_states(self):
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p2"})
        )
        j = q.enqueue(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]}
        )
        q.run_next()
        new_j = q.reverify_failures(j, StubVerifier())
        # queued 时无报告
        with self.assertRaises(ResultUnavailableError):
            q.report(new_j)
        # 取消后仍无报告
        q.cancel(new_j)
        with self.assertRaises(ResultUnavailableError):
            q.report(new_j)
        # 源作业报告不受影响
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.report(j).result.failed, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
