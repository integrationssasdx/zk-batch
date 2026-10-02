"""VerificationQueue 的串行生命周期与异常矩阵测试。"""

from __future__ import annotations

import unittest

from zk_batch import (
    BatchVerificationResult,
    DefaultVerifier,
    VerificationQueue,
    ZKVerifier,
)
from zk_batch.errors import (
    CancelledJobError,
    CompletedJobError,
    EmptyBatchError,
    ResultUnavailableError,
    RunningJobError,
    UnknownJobError,
)
from zk_batch.queue import (
    STATE_CANCELLED,
    STATE_COMPLETED,
    STATE_QUEUED,
    STATE_RUNNING,
)


def p(pid, proof=b"ok", inputs=(1,)):
    return {
        "proof_id": pid,
        "protocol": "groth16",
        "circuit_id": "c1",
        "aggregation_key": "k1",
        "public_inputs": inputs,
        "proof": proof,
    }


def batch(proofs, bid="b1"):
    return {"batch_id": bid, "proofs": proofs}


class QueueLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.registry = ZKVerifier()
        self.registry.register("groth16", DefaultVerifier())
        self.q = VerificationQueue(self.registry)

    def test_enqueue_returns_job_ids_in_sequence(self):
        j1 = self.q.enqueue(batch([p("a")]))
        j2 = self.q.enqueue(batch([p("b")]))
        self.assertEqual(j1, "job-1")
        self.assertEqual(j2, "job-2")
        self.assertEqual(self.q.status(j1), STATE_QUEUED)

    def test_run_next_empty_returns_none(self):
        self.assertIsNone(self.q.run_next())

    def test_run_next_fifo_and_completes(self):
        j1 = self.q.enqueue(batch([p("a")], "b-a"))
        j2 = self.q.enqueue(batch([p("b"), p("c")], "b-b"))

        r1 = self.q.run_next()
        self.assertIsInstance(r1, BatchVerificationResult)
        self.assertEqual(r1.batch_id, "b-a")
        self.assertEqual(r1.passed, ("a",))
        self.assertEqual(self.q.status(j1), STATE_COMPLETED)
        self.assertEqual(self.q.status(j2), STATE_QUEUED)

        r2 = self.q.run_next()
        self.assertEqual(r2.batch_id, "b-b")
        self.assertEqual(self.q.status(j2), STATE_COMPLETED)
        self.assertIsNone(self.q.run_next())

    def test_result_only_for_completed(self):
        j1 = self.q.enqueue(batch([p("a")]))
        with self.assertRaises(ResultUnavailableError) as ctx:
            self.q.result(j1)
        self.assertEqual(ctx.exception.state, STATE_QUEUED)

        self.q.run_next()
        result = self.q.result(j1)
        self.assertEqual(result.passed, ("a",))

    def test_cancel_queued(self):
        j1 = self.q.enqueue(batch([p("a")]))
        j2 = self.q.enqueue(batch([p("b")]))
        self.assertTrue(self.q.cancel(j1))
        self.assertEqual(self.q.status(j1), STATE_CANCELLED)
        # 取消后队首推进到 j2
        result = self.q.run_next()
        self.assertEqual(result.passed, ("b",))
        # 已取消作业不能取结果
        with self.assertRaises(ResultUnavailableError):
            self.q.result(j1)

    def test_cancel_running_raises(self):
        # 用验证器在单证验证执行中（作业处于 running）观察队列状态。
        observed = {}

        class Observe(DefaultVerifier):
            def verify_aggregate(self, aggregate, proofs, public_inputs):
                return False  # 强制进入逐单证验证路径

            def verify(self, public_inputs, proof):
                observed["status"] = q.status(job)
                with self.assertRaises(RunningJobError):
                    q.cancel(job)
                return super().verify(public_inputs, proof)

        registry = ZKVerifier()
        registry.register("groth16", Observe())
        q = VerificationQueue(registry)
        job = q.enqueue(batch([p("a")]))
        q.run_next()
        self.assertEqual(observed["status"], STATE_RUNNING)
        # 运行结束后 cancel → CompletedJobError
        with self.assertRaises(CompletedJobError):
            q.cancel(job)

    def test_cancel_completed_raises_completed(self):
        j1 = self.q.enqueue(batch([p("a")]))
        self.q.run_next()
        with self.assertRaises(CompletedJobError):
            self.q.cancel(j1)

    def test_cancel_cancelled_raises_cancelled(self):
        j1 = self.q.enqueue(batch([p("a")]))
        self.q.cancel(j1)
        with self.assertRaises(CancelledJobError):
            self.q.cancel(j1)

    def test_unknown_job(self):
        with self.assertRaises(UnknownJobError):
            self.q.status("job-999")
        with self.assertRaises(UnknownJobError):
            self.q.cancel("job-999")
        with self.assertRaises(UnknownJobError):
            self.q.result("job-999")

    def test_run_next_propagates_verify_batch_errors(self):
        j1 = self.q.enqueue(batch([]))  # EmptyBatchError
        with self.assertRaises(EmptyBatchError):
            self.q.run_next()
        # fail-closed：异常后作业脱离 queued，不会被重复执行
        self.assertIsNone(self.q.run_next())

    def test_cancelled_job_not_executed(self):
        j1 = self.q.enqueue(batch([p("a")]))
        self.q.cancel(j1)
        self.assertIsNone(self.q.run_next())

    def test_per_job_verifier_override(self):
        j1 = self.q.enqueue(batch([p("a")]))  # 使用队列级 registry
        self.q.run_next()
        self.assertEqual(self.q.result(j1).passed, ("a",))

    def test_queue_without_default_registry(self):
        q = VerificationQueue()
        j1 = q.enqueue(batch([p("a", b"aa")]))  # 默认 groth16
        result = q.run_next()
        self.assertEqual(result.passed, ("a",))


if __name__ == "__main__":
    unittest.main()
