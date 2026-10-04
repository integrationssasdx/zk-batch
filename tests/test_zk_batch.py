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
    Failure,
    IncompatibleAggregationError,
    InvalidProofError,
    NoFailedProofError,
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


# ================================================================ 失败复核

class TestReverifyFailures(unittest.TestCase):
    def _completed_job(self, q, proofs, **verifier_kw):
        """入队并跑完一个批次，返回 (job_id, result)。"""
        jid = q.enqueue({"batch_id": "B", "proofs": proofs})
        return jid, q.run_next()

    def test_unknown_job(self):
        q = VerificationQueue(StubVerifier())
        with self.assertRaises(UnknownJobError):
            q.reverify_failures("ghost")

    def test_not_completed_raises_unavailable(self):
        q = VerificationQueue(StubVerifier())
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        # queued
        with self.assertRaises(ResultUnavailableError):
            q.reverify_failures(j)
        # cancelled
        q.cancel(j)
        with self.assertRaises(ResultUnavailableError):
            q.reverify_failures(j)

    def test_no_failures_raises_no_failed_proof(self):
        q = VerificationQueue(StubVerifier())
        j, result = self._completed_job(q, [proof("p1"), proof("p2")])
        self.assertEqual(result.failed, 0)
        with self.assertRaises(NoFailedProofError):
            q.reverify_failures(j)

    def test_only_failed_proofs_selected_in_original_order(self):
        # 源批次：p1/p2 在 k1，p3/p4/p5 在 k2；p2 被拒、p3 验证异常。
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
        j, source = self._completed_job(q, proofs)
        self.assertEqual(source.failed, 2)

        # 复核作业使用"已修复"的验证器（聚合直接通过）
        fixed = StubVerifier()
        new_j = q.reverify_failures(j, fixed)
        self.assertNotEqual(new_j, j)
        self.assertEqual(q.status(new_j), "queued")
        result = q.run_next()
        self.assertIsInstance(result, BatchVerificationResult)
        # 只复核 p2、p3 两证；重新分组为 2 组（k1 只剩 p2，k2 只剩 p3）
        self.assertEqual(result.passed + result.failed, 2)
        self.assertEqual(result.failed, 0)
        self.assertEqual(result.aggregate_count, 2)
        self.assertEqual(result.batch_id, "B")
        self.assertEqual(fixed.calls["aggregate"], 2)
        self.assertEqual(fixed.calls["verify"], [])
        self.assertIsNone(q.run_next())

    def test_failures_sorted_and_codes_distinguished(self):
        # 复核仍失败：源序 z,a,m，结果按 proof_id 升序；
        # rejected 与 verify_error 区分保留。
        flaky = StubVerifier(
            agg_verify=False, reject_ids={"z", "m"}, error_ids={"a"}
        )
        q = VerificationQueue(flaky)
        proofs = [proof("z"), proof("a"), proof("m")]
        j, _ = self._completed_job(q, proofs)

        new_j = q.reverify_failures(j)  # 沿用队列默认（同一个 flaky）
        result = q.run_next()
        self.assertEqual(result.failed, 3)
        self.assertEqual([f.proof_id for f in result.failures], ["a", "m", "z"])
        by_id = {f.proof_id: f for f in result.failures}
        self.assertEqual(by_id["a"].code, "verify_error")
        self.assertEqual(by_id["a"].stage, "single_verify")
        self.assertEqual(by_id["m"].code, "rejected")
        self.assertEqual(by_id["z"].code, "rejected")
        self.assertEqual(by_id["z"].group_id, "groth16:c1:k1")

    def test_public_inputs_and_proof_preserved_as_is(self):
        seen = {}

        class Capturing(StubVerifier):
            def aggregate(self, members):
                for p in members:
                    seen[p.proof_id] = p
                return super().aggregate(members)

        sentinel_inputs = [{"x": 1}, [9, 8]]
        sentinel_proof = object()
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p2"})
        )
        proofs = [
            proof("p1"),
            proof("p2", inputs=sentinel_inputs, body=sentinel_proof),
        ]
        j, _ = self._completed_job(q, proofs)
        q.reverify_failures(j, Capturing())
        q.run_next()
        self.assertEqual(set(seen), {"p2"})
        self.assertIs(seen["p2"].public_inputs, sentinel_inputs)
        self.assertIs(seen["p2"].proof, sentinel_proof)

    def test_source_job_untouched(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        j, source = self._completed_job(
            q, [proof("p1"), proof("p2"), proof("p3")]
        )

        new_j = q.reverify_failures(j, StubVerifier())
        q.run_next()
        # 源作业状态、结果不变
        self.assertEqual(q.status(j), "completed")
        self.assertIs(q.result(j), source)
        self.assertEqual(q.result(j).failed, 1)
        self.assertEqual([f.proof_id for f in q.result(j).failures], ["p2"])
        # 新作业是独立作业
        self.assertNotEqual(new_j, j)
        self.assertEqual(q.status(new_j), "completed")

    def test_new_job_can_cancel(self):
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p2"})
        )
        j, _ = self._completed_job(q, [proof("p1"), proof("p2")])
        new_j = q.reverify_failures(j, StubVerifier())
        self.assertTrue(q.cancel(new_j))
        self.assertEqual(q.status(new_j), "cancelled")
        self.assertIsNone(q.run_next())
        with self.assertRaises(ResultUnavailableError):
            q.result(new_j)
        # 源作业仍可取结果
        self.assertEqual(q.status(j), "completed")

    def test_verifier_override_scoped_to_new_job(self):
        # 队列默认验证器认识 groth16 且 p1 失败；复核时覆盖为只认识 plonk
        # 的验证器 -> 执行复核时 groth16 未知；覆盖不影响队列默认。
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p1"})
        )
        j, source = self._completed_job(q, [proof("p1")])
        self.assertEqual(source.failed, 1)
        new_j = q.reverify_failures(j, OtherVerifier())
        with self.assertRaises(UnsupportedProofSystemError):
            q.run_next()
        # 异常后复核作业按既有约定移除
        with self.assertRaises(UnknownJobError):
            q.status(new_j)
        # 源作业不受影响
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.result(j).failed, 1)

    def test_default_verifiers_used_when_omitted(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p1"})
        q = VerificationQueue(flaky)
        j, _ = self._completed_job(q, [proof("p1")])
        # 不传 verifiers：用队列默认（仍是 flaky），复核依旧失败
        new_j = q.reverify_failures(j)
        result = q.run_next()
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.failures[0].proof_id, "p1")

    def test_chain_reverify_until_clean(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        j, _ = self._completed_job(q, [proof("p1"), proof("p2"), proof("p3")])

        # 第一次复核：仍失败 -> 可以继续复核
        j2 = q.reverify_failures(j)
        r2 = q.run_next()
        self.assertEqual(r2.failed, 1)
        j3 = q.reverify_failures(j2)
        r3 = q.run_next()
        self.assertEqual(r3.failed, 1)
        # 换用通过的验证器后复核全过
        j4 = q.reverify_failures(j3, StubVerifier())
        r4 = q.run_next()
        self.assertEqual((r4.passed, r4.failed), (1, 0))
        # 无失败证明 -> 不能再复核
        with self.assertRaises(NoFailedProofError):
            q.reverify_failures(j4)

    def test_reverify_preserves_incompatible_aggregation_error(self):
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p1", "p2"})
        )
        j, _ = self._completed_job(
            q, [proof("p1", key="k1"), proof("p2", key="k1")]
        )
        q.reverify_failures(j, StubVerifier(aggregate_ok=False))
        with self.assertRaises(IncompatibleAggregationError):
            q.run_next()

    def test_reverify_preserves_contract_error(self):
        class Groth16NonBool(NonBoolVerifier):
            protocol = "groth16"

        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p1"})
        )
        j, _ = self._completed_job(q, [proof("p1")])
        q.reverify_failures(j, Groth16NonBool())
        with self.assertRaises(VerifierContractError):
            q.run_next()


# ================================================================ 分组失败视图

class _AggregateBoomVerifier(ZKVerifier):
    """aggregate 调用本身抛异常：组内每证归责 aggregate 阶段。"""

    protocol = "stark"

    def aggregate(self, proofs):
        raise RuntimeError("aggregator down")

    def verify_aggregate(self, proofs, aggregated):
        return True

    def verify(self, p):
        return True


class _AggregateRejectVerifier(ZKVerifier):
    """整组验证被拒、单证全部通过：归责 aggregate_verify 阶段。"""

    protocol = "plonk"

    def aggregate(self, proofs):
        return ["PLONK-AGG"]

    def verify_aggregate(self, proofs, aggregated):
        return False

    def verify(self, p):
        return True


class TestFailureGroups(unittest.TestCase):
    def test_empty_when_all_passed(self):
        result = verify_batch(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            StubVerifier(),
        )
        self.assertEqual(result.failure_groups(), [])
        # 直接构造的全通过结果同样返回空列表
        plain = BatchVerificationResult(
            batch_id="x", aggregate_count=0, passed=0, failed=0
        )
        self.assertEqual(plain.failure_groups(), [])

    def test_single_group_fallback_fields_and_codes(self):
        v = StubVerifier(agg_verify=False, reject_ids={"p2"}, error_ids={"p3"})
        result = verify_batch(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2"), proof("p3")]},
            v,
        )
        groups = result.failure_groups()
        self.assertEqual(len(groups), 1)
        summary = groups[0]
        self.assertEqual(
            set(summary), {"group_id", "failed_proof_ids", "failed_count", "failures"}
        )
        self.assertEqual(summary["group_id"], "groth16:c1:k1")
        self.assertEqual(summary["failed_proof_ids"], ["p2", "p3"])
        self.assertEqual(summary["failed_count"], 2)
        # rejected 与 verify_error 分别保留，消息原样不改写
        by_id = {f["proof_id"]: f for f in summary["failures"]}
        self.assertEqual(by_id["p2"]["stage"], "single_verify")
        self.assertEqual(by_id["p2"]["code"], "rejected")
        self.assertEqual(by_id["p2"]["message"], "proof rejected by verifier")
        self.assertEqual(by_id["p3"]["stage"], "single_verify")
        self.assertEqual(by_id["p3"]["code"], "verify_error")
        self.assertIn("bad proof bytes", by_id["p3"]["message"])
        # 明细键沿用 Failure 的四元（不含 group_id）
        self.assertEqual(
            set(by_id["p2"]), {"proof_id", "stage", "code", "message"}
        )

    def test_groups_sorted_and_stages_distinguished(self):
        verifiers = [
            StubVerifier(agg_verify=False, reject_ids={"b"}, error_ids={"a"}),
            _AggregateRejectVerifier(),
            _AggregateBoomVerifier(),
        ]
        batch = {
            "batch_id": "B",
            "proofs": [
                # 回退单证验证组：b rejected、a verify_error
                proof("b", proto="groth16", circuit="c1", key="g1"),
                proof("a", proto="groth16", circuit="c1", key="g1"),
                # 聚合验证失败组：单证全过，归责 aggregate_verify
                proof("d", proto="plonk", circuit="c2", key="g2"),
                proof("c", proto="plonk", circuit="c2", key="g2"),
                # 聚合阶段异常组：归责 aggregate
                proof("e", proto="stark", circuit="c3", key="g3"),
            ],
        }
        result = verify_batch(batch, verifiers)
        groups = result.failure_groups()

        # 摘要按 group_id 升序
        self.assertEqual(
            [g["group_id"] for g in groups],
            [
                "groth16:c1:g1",
                "plonk:c2:g2",
                "stark:c3:g3",
            ],
        )

        single, agg_verify, agg = groups

        self.assertEqual(single["failed_proof_ids"], ["a", "b"])
        self.assertEqual(single["failed_count"], 2)
        self.assertTrue(
            all(f["stage"] == "single_verify" for f in single["failures"])
        )
        self.assertEqual(
            sorted(f["code"] for f in single["failures"]),
            ["rejected", "verify_error"],
        )

        self.assertEqual(agg_verify["failed_proof_ids"], ["c", "d"])
        self.assertEqual(agg_verify["failed_count"], 2)
        self.assertTrue(
            all(
                f["stage"] == "aggregate_verify" and f["code"] == "rejected"
                for f in agg_verify["failures"]
            )
        )

        self.assertEqual(agg["failed_proof_ids"], ["e"])
        self.assertEqual(agg["failed_count"], 1)
        self.assertEqual(agg["failures"][0]["stage"], "aggregate")
        self.assertEqual(agg["failures"][0]["code"], "verify_error")
        self.assertIn("aggregator down", agg["failures"][0]["message"])

    def test_repeated_calls_are_identical(self):
        v = StubVerifier(agg_verify=False, reject_ids={"b"}, error_ids={"a"})
        result = verify_batch(
            {
                "batch_id": "B",
                "proofs": [
                    proof("b", key="g1"),
                    proof("a", key="g1"),
                    proof("c", key="g2"),
                ],
            },
            v,
        )
        first = result.failure_groups()
        second = result.failure_groups()
        self.assertEqual(first, second)

    def test_same_proof_two_records_both_retained(self):
        # 直接构造：同一 proof_id 在同一组同时有 verify_error 与 rejected
        result = BatchVerificationResult(
            batch_id="x",
            aggregate_count=1,
            passed=0,
            failed=2,
            failures=[
                Failure(
                    proof_id="p1",
                    group_id="g:a:b",
                    stage="single_verify",
                    code="verify_error",
                    message="ValueError: boom",
                ),
                Failure(
                    proof_id="p1",
                    group_id="g:a:b",
                    stage="single_verify",
                    code="rejected",
                    message="proof rejected by verifier",
                ),
            ],
        )
        groups = result.failure_groups()
        self.assertEqual(len(groups), 1)
        summary = groups[0]
        self.assertEqual(summary["failed_count"], 2)
        self.assertEqual(summary["failed_proof_ids"], ["p1", "p1"])
        self.assertEqual(
            [f["code"] for f in summary["failures"]],
            ["verify_error", "rejected"],
        )

    def test_constructed_groups_sort_by_group_id_then_proof_id(self):
        result = BatchVerificationResult(
            batch_id="x",
            aggregate_count=2,
            passed=0,
            failed=3,
            failures=[
                Failure("z2", "z:c:k", "aggregate", "verify_error", "m"),
                Failure("a2", "a:c:k", "aggregate", "verify_error", "m"),
                Failure("a1", "a:c:k", "aggregate", "verify_error", "m"),
            ],
        )
        groups = result.failure_groups()
        self.assertEqual([g["group_id"] for g in groups], ["a:c:k", "z:c:k"])
        self.assertEqual(groups[0]["failed_proof_ids"], ["a1", "a2"])
        self.assertEqual(groups[0]["failed_count"], 2)
        self.assertEqual(groups[1]["failed_proof_ids"], ["z2"])
        self.assertEqual(groups[1]["failed_count"], 1)

    def test_to_dict_unchanged(self):
        v = StubVerifier(agg_verify=False, reject_ids={"p2"}, error_ids={"p3"})
        result = verify_batch(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2"), proof("p3")]},
            v,
        )
        # 调用分组视图不影响 to_dict：键名与内容保持原样
        result.failure_groups()
        payload = result.to_dict()
        self.assertEqual(
            set(payload),
            {"batch_id", "aggregate_count", "passed", "failed", "failures"},
        )
        self.assertEqual(payload["failed"], 2)
        self.assertEqual(payload["passed"], 1)
        self.assertEqual(
            [f["proof_id"] for f in payload["failures"]], ["p2", "p3"]
        )
        self.assertEqual(
            set(payload["failures"][0]),
            {"proof_id", "group_id", "stage", "code", "message"},
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
