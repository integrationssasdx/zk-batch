"""冒烟测试：覆盖六类验证错误、聚合/回退单证、队列状态机。

直接运行：python tests/test_zk_batch.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchVerificationResult,
    CancelledJobError,
    CompletedJobError,
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidProofError,
    ResultUnavailableError,
    RunningJobError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerificationQueue,
    VerifierContractError,
    ZKVerifier,
    verify_batch,
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


# ---------------------------------------------------------------- 假验证器

class StubVerifier(ZKVerifier):
    """可编排行为的假验证器。"""

    protocol = "groth16"

    def __init__(
        self,
        aggregate_ok=True,
        agg_result=None,
        reject_ids=(),
        error_ids=(),
        agg_verify=True,
        agg_error=False,
    ):
        self._aggregate_ok = aggregate_ok
        self._agg_result = agg_result if agg_result is not None else ["AGG"]
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._agg_verify = agg_verify
        self._agg_error = agg_error
        self.calls = {"aggregate": 0, "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        if not self._aggregate_ok:
            raise IncompatibleAggregationError("cannot aggregate this group")
        return self._agg_result

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        if self._agg_error:
            raise RuntimeError("aggregate verifier exploded")
        if self._agg_verify is True or self._agg_verify is False:
            return self._agg_verify
        return self._agg_verify  # 非布尔注入

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        if p.proof_id in self._errors:
            raise ValueError("bad proof bytes")
        return p.proof_id not in self._reject


class OtherVerifier(ZKVerifier):
    protocol = "plonk"

    def aggregate(self, proofs):
        return ["PLONK-AGG"]

    def verify_aggregate(self, proofs, aggregated):
        return True

    def verify(self, p):
        return True


class IncompleteVerifier(ZKVerifier):
    protocol = "halo2"
    # 三个方法都不实现


class NonBoolVerifier(ZKVerifier):
    protocol = "stark"

    def aggregate(self, proofs):
        return ["S"]

    def verify_aggregate(self, proofs, aggregated):
        return "yes"

    def verify(self, p):
        return 1


# ================================================================ 输入与错误

class TestInputValidation(unittest.TestCase):
    def test_empty_batch_variants(self):
        v = StubVerifier()
        with self.assertRaises(EmptyBatchError):
            verify_batch({}, v)
        with self.assertRaises(EmptyBatchError):
            verify_batch({"batch_id": "b", "proofs": []}, v)
        with self.assertRaises(EmptyBatchError):
            verify_batch({"proofs": [proof("p1")]}, v)
        with self.assertRaises(EmptyBatchError):
            verify_batch("not-a-dict", v)

    def test_invalid_fields(self):
        v = StubVerifier()
        bad = proof("p1")
        del bad["circuit_id"]
        with self.assertRaises(InvalidProofError):
            verify_batch({"batch_id": "b", "proofs": [bad]}, v)

        with self.assertRaises(InvalidProofError):
            verify_batch(
                {"batch_id": "b", "proofs": [{"proof_id": "",
                                              "protocol": "groth16",
                                              "circuit_id": "c",
                                              "aggregation_key": "k",
                                              "public_inputs": [],
                                              "proof": b"x"}]},
                v,
            )
        with self.assertRaises(InvalidProofError):
            verify_batch(
                {"batch_id": "b",
                 "proofs": [proof("p1", inputs=("tuple",))]},
                v,
            )

    def test_duplicate_id(self):
        v = StubVerifier()
        with self.assertRaises(DuplicateProofIdError) as ctx:
            verify_batch(
                {"batch_id": "b", "proofs": [proof("dup"), proof("dup", key="k2")]},
                v,
            )
        self.assertEqual(ctx.exception.proof_id, "dup")

    def test_error_order_empty_before_invalid(self):
        v = StubVerifier()
        with self.assertRaises(EmptyBatchError):
            verify_batch({"batch_id": "b", "proofs": []}, v)

    def test_invalid_before_duplicate(self):
        v = StubVerifier()
        p1 = proof("dup")
        p2 = proof("dup")
        p2["protocol"] = 123  # 字段类型错误先于重复检测
        with self.assertRaises(InvalidProofError):
            verify_batch({"batch_id": "b", "proofs": [p1, p2]}, v)


class TestVerifierErrors(unittest.TestCase):
    def test_unsupported_protocol(self):
        v = StubVerifier()
        with self.assertRaises(UnsupportedProofSystemError) as ctx:
            verify_batch(
                {"batch_id": "b", "proofs": [proof("p1", proto="mystery")]}, v
            )
        self.assertEqual(ctx.exception.protocol, "mystery")

    def test_incompatible_aggregation_aborts_batch(self):
        v = StubVerifier(aggregate_ok=False)
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch({"batch_id": "b", "proofs": [proof("p1"), proof("p2")]}, v)

    def test_incomplete_verifier_contract(self):
        v = IncompleteVerifier()
        with self.assertRaises(VerifierContractError):
            verify_batch({"batch_id": "b", "proofs": [proof("p1", proto="halo2")]}, v)

    def test_non_bool_return_contract(self):
        v = NonBoolVerifier()
        with self.assertRaises(VerifierContractError):
            verify_batch({"batch_id": "b", "proofs": [proof("p1", proto="stark")]}, v)

    def test_aggregation_unsupported_order(self):
        # 第一组不能聚合（IncompatibleAggregationError 先抛出），
        # 即使批次后面还有未知系统
        v = StubVerifier(aggregate_ok=False)
        batch = {
            "batch_id": "b",
            "proofs": [proof("p1"), proof("p2", proto="mystery")],
        }
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch(batch, v)

    def test_incompatible_before_contract_error_across_groups(self):
        # 组1 (groth16) 聚合不能进行；组2 (halo2) 验证器缺方法。
        # 组按首次出现序处理，故先抛 IncompatibleAggregationError。
        verifiers = {
            "groth16": StubVerifier(aggregate_ok=False),
            "halo2": IncompleteVerifier(),
        }
        batch = {
            "batch_id": "b",
            "proofs": [proof("p1", proto="groth16"),
                       proof("p2", proto="halo2")],
        }
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch(batch, verifiers)

    def test_unknown_system_before_contract_within_pipeline(self):
        # 组1 正常通过后，组2 是未知系统；组3（永远到不了）验证器缺方法。
        verifiers = {
            "groth16": StubVerifier(),
            "stark": IncompleteVerifier(),
        }
        batch = {
            "batch_id": "b",
            "proofs": [proof("p1", proto="groth16"),
                       proof("p2", proto="mystery"),
                       proof("p3", proto="stark")],
        }
        with self.assertRaises(UnsupportedProofSystemError):
            verify_batch(batch, verifiers)

    def test_non_bool_single_verify_raises_contract(self):
        class SingleNonBool(StubVerifier):
            def verify(self, p):
                return 1

        v = SingleNonBool(agg_verify=False)
        with self.assertRaises(VerifierContractError):
            verify_batch(
                {"batch_id": "b", "proofs": [proof("p1")]}, v
            )

    def test_single_proof_group_passes_and_fails(self):
        ok = verify_batch(
            {"batch_id": "b", "proofs": [proof("solo")]}, StubVerifier()
        )
        self.assertEqual((ok.passed, ok.failed, ok.aggregate_count), (1, 0, 1))

        bad = verify_batch(
            {"batch_id": "b", "proofs": [proof("only")]},
            StubVerifier(agg_verify=False, reject_ids={"only"}),
        )
        self.assertEqual((bad.passed, bad.failed), (0, 1))
        self.assertEqual(bad.failures[0].stage, "single_verify")


# ================================================================ 成功与回退

class TestVerificationFlow(unittest.TestCase):
    def test_all_aggregated_pass(self):
        v = StubVerifier()
        batch = {
            "batch_id": "B-1",
            "proofs": [
                proof("a", key="k1"),
                proof("b", key="k1"),
                proof("c", key="k2"),
            ],
        }
        result = verify_batch(batch, v)
        self.assertIsInstance(result, BatchVerificationResult)
        self.assertEqual(result.batch_id, "B-1")
        self.assertEqual(result.aggregate_count, 2)
        self.assertEqual(result.passed, 3)
        self.assertEqual(result.failed, 0)
        self.assertEqual(result.failures, [])
        # 每组各聚合一次；聚合通过不回退单证
        self.assertEqual(v.calls["aggregate"], 2)
        self.assertEqual(v.calls["verify"], [])

    def test_fallback_marks_rejected_and_errors(self):
        v = StubVerifier(agg_verify=False, reject_ids={"p2"}, error_ids={"p3"})
        batch = {
            "batch_id": "B",
            "proofs": [proof("p1"), proof("p2"), proof("p3")],
        }
        result = verify_batch(batch, v)
        self.assertEqual(result.passed, 1)
        self.assertEqual(result.failed, 2)
        by_id = {f.proof_id: f for f in result.failures}
        self.assertEqual(by_id["p2"].stage, "single_verify")
        self.assertEqual(by_id["p2"].code, "rejected")
        self.assertEqual(by_id["p2"].group_id, "groth16:c1:k1")
        self.assertEqual(by_id["p3"].stage, "single_verify")
        self.assertEqual(by_id["p3"].code, "verify_error")
        self.assertIn("bad proof bytes", by_id["p3"].message)
        # 失败项按 proof_id 排序
        self.assertEqual([f.proof_id for f in result.failures], ["p2", "p3"])

    def test_aggregate_error_falls_back(self):
        v = StubVerifier(agg_error=True)
        batch = {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]}
        result = verify_batch(batch, v)
        # 单证都通过 -> 归责聚合验证阶段，组内每证一条
        self.assertEqual(result.failed, 2)
        self.assertTrue(
            all(f.stage == "aggregate_verify" for f in result.failures)
        )
        self.assertTrue(all(f.code == "verify_error" for f in result.failures))

    def test_aggregate_call_exception_marks_aggregate_stage(self):
        class BoomAggregate(StubVerifier):
            def aggregate(self, proofs):
                self.calls["aggregate"] += 1
                raise RuntimeError("aggregator down")

        v = BoomAggregate()
        result = verify_batch(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]}, v
        )
        self.assertEqual(result.failed, 2)
        self.assertTrue(all(f.stage == "aggregate" for f in result.failures))
        self.assertTrue(all(f.code == "verify_error" for f in result.failures))
        # 聚合器崩溃不回退单证
        self.assertEqual(v.calls["verify"], [])

    def test_multi_protocol_registration(self):
        v1 = StubVerifier()
        v2 = OtherVerifier()
        batch = {
            "batch_id": "B",
            "proofs": [proof("a", proto="groth16"), proof("b", proto="plonk")],
        }
        result = verify_batch(batch, [v1, v2])
        self.assertEqual(result.aggregate_count, 2)
        self.assertEqual(result.passed, 2)

        # 字典形式
        result2 = verify_batch(batch, {"groth16": v1, "plonk": v2})
        self.assertEqual(result2.passed, 2)

    def test_proof_fields_preserved(self):
        seen = {}

        class Capturing(StubVerifier):
            def aggregate(self, proofs):
                for p in proofs:
                    seen[p.proof_id] = p
                return super().aggregate(proofs)

        sentinel_inputs = [{"x": 1}, [1, 2, 3]]
        sentinel_proof = object()
        v = Capturing()
        batch = {
            "batch_id": 99,
            "proofs": [proof("p1", inputs=sentinel_inputs, body=sentinel_proof)],
        }
        result = verify_batch(batch, v)
        self.assertEqual(result.batch_id, 99)
        self.assertIs(seen["p1"].public_inputs, sentinel_inputs)
        self.assertIs(seen["p1"].proof, sentinel_proof)


# ================================================================ 队列

class TestQueue(unittest.TestCase):
    def make_queue(self, **kw):
        return VerificationQueue(StubVerifier(**kw))

    def test_enqueue_run_next_flow(self):
        q = self.make_queue()
        job_id = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        self.assertIsInstance(job_id, str)
        self.assertEqual(q.status(job_id), "queued")
        self.assertEqual(q.run_next(), q.result(job_id))
        self.assertEqual(q.status(job_id), "completed")
        result = q.result(job_id)
        self.assertEqual(result.passed, 1)
        # 队列已空
        self.assertIsNone(q.run_next())

    def test_fifo_order(self):
        q = self.make_queue()
        j1 = q.enqueue({"batch_id": "B1", "proofs": [proof("p1")]})
        j2 = q.enqueue({"batch_id": "B2", "proofs": [proof("p2")]})
        r1 = q.run_next()
        self.assertEqual(r1.batch_id, "B1")
        self.assertEqual(q.status(j1), "completed")
        self.assertEqual(q.status(j2), "queued")
        r2 = q.run_next()
        self.assertEqual(r2.batch_id, "B2")

    def test_cancel_queued(self):
        q = self.make_queue()
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        self.assertTrue(q.cancel(j))
        self.assertEqual(q.status(j), "cancelled")
        # 已取消的作业不被执行
        self.assertIsNone(q.run_next())

    def test_cancel_errors(self):
        q = self.make_queue()
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        q.cancel(j)
        with self.assertRaises(CancelledJobError):
            q.cancel(j)

        j2 = q.enqueue({"batch_id": "B2", "proofs": [proof("p2")]})
        q.run_next()
        with self.assertRaises(CompletedJobError):
            q.cancel(j2)

        with self.assertRaises(UnknownJobError):
            q.cancel("nope")

        with self.assertRaises(UnknownJobError):
            q.status("nope")

    def test_cancel_running(self):
        # 同步执行期间在验证器内部尝试取消：作业处于 running
        v = StubVerifier()

        def cancel_during_verify(proofs, aggregated):
            with self.assertRaises(RunningJobError):
                q.cancel(j)
            return True

        v.verify_aggregate = cancel_during_verify
        q = VerificationQueue(v)
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        q.run_next()
        self.assertEqual(q.status(j), "completed")

    def test_result_unavailable(self):
        q = self.make_queue()
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        with self.assertRaises(ResultUnavailableError):
            q.result(j)
        q.cancel(j)
        with self.assertRaises(ResultUnavailableError):
            q.result(j)
        with self.assertRaises(UnknownJobError):
            q.result("ghost")

    def test_failed_batch_propagates(self):
        q = self.make_queue()
        j = q.enqueue({"batch_id": "B", "proofs": []})
        with self.assertRaises(EmptyBatchError):
            q.run_next()
        with self.assertRaises(UnknownJobError):
            q.status(j)

    def test_per_job_verifier_override(self):
        q = VerificationQueue(StubVerifier())
        # 覆盖为只认识 plonk 的验证器 -> groth16 未知
        q.enqueue(
            {"batch_id": "B", "proofs": [proof("p1")]}, verifiers=OtherVerifier()
        )
        with self.assertRaises(UnsupportedProofSystemError):
            q.run_next()

    def test_failure_sorting_and_payload(self):
        v = StubVerifier(agg_verify=False, reject_ids={"z", "a", "m"})
        q = VerificationQueue(v)
        q.enqueue(
            {
                "batch_id": "B",
                "proofs": [proof("z"), proof("a"), proof("m")],
            }
        )
        result = q.run_next()
        self.assertEqual(
            [f.proof_id for f in result.failures], ["a", "m", "z"]
        )
        payload = result.to_dict()
        self.assertEqual(payload["failed"], 3)
        self.assertEqual(payload["aggregate_count"], 1)
        self.assertEqual(
            {k for k in payload["failures"][0]},
            {"proof_id", "group_id", "stage", "code", "message"},
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
