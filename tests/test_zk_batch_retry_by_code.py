"""VerificationQueue 按失败错误码定向重试测试。

覆盖：
* 全批次重试（不提供 proof_ids）：按批次原序选出匹配错误码且可重试的证明；
* 指定证明重试：accepted/skipped 按输入 proof_ids 顺序返回；
* 混合接受与跳过：UNKNOWN_PROOF / NOT_RETRYABLE / ALREADY_COMPLETED；
* 重复提交：ALREADY_QUEUED / ALREADY_COMPLETED / 再次可重试的确定行为；
* 批次不存在 BatchNotFoundException、证明归属其他批次
  ProofNotInBatchException、输入错误 InvalidRetrySelectionException；
* 筛选后无可重试证明：不报错、不建作业，accepted 为空、skipped 逐条原因；
* 取消后恢复可重试、原始作业待验证时记 ALREADY_QUEUED；
* 既有 result/report 查询给出新验证状态与失败定位，历史结果不被改写；
* 相同输入重复执行得到相同结果（确定性）。

直接运行：python tests/test_zk_batch_retry_by_code.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchNotFoundError,
    BatchNotFoundException,
    InvalidRetrySelectionError,
    InvalidRetrySelectionException,
    NoFailedProofError,
    ProofNotInBatchError,
    ProofNotInBatchException,
    ProofRetrySubmission,
    RETRY_SKIP_ALREADY_COMPLETED,
    RETRY_SKIP_ALREADY_QUEUED,
    RETRY_SKIP_NOT_RETRYABLE,
    RETRY_SKIP_REASONS,
    RETRY_SKIP_UNKNOWN_PROOF,
    RETRY_SKIP_PROOF_NOT_IN_BATCH,
    VerificationQueue,
    ZKVerifier,
)


def proof(pid, key="g1", body="ok"):
    return {
        "proof_id": pid,
        "protocol": "groth16",
        "circuit_id": "c1",
        "aggregation_key": key,
        "public_inputs": ["pi"],
        "proof": ("blob", body),
    }


# 源批次：p1/p5 通过；p2/p4 单证被拒（rejected）；p3 单证异常（verify_error）。
SOURCE_PROOFS = [
    proof("p1", body="ok"),
    proof("p2", body="bad"),
    proof("p3", body="err"),
    proof("p4", body="bad"),
    proof("p5", body="ok"),
]


class FlakyVerifier(ZKVerifier):
    """body 决定单证行为：bad 拒绝、err 抛异常；healed 中的证明转为通过。"""

    protocol = "groth16"

    def __init__(self):
        self.healed = set()

    def _ok(self, proof_obj):
        if proof_obj.proof_id in self.healed:
            return True
        return proof_obj.proof[1] not in ("bad", "err")

    def aggregate(self, proofs):
        return ["AGG"]

    def verify_aggregate(self, proofs, aggregated):
        # 组内有坏证时整组验证失败，触发回退单证验证。
        return all(self._ok(p) for p in proofs)

    def verify(self, proof_obj):
        if proof_obj.proof_id in self.healed:
            return True
        body = proof_obj.proof[1]
        if body == "err":
            raise RuntimeError("boom")
        return body != "bad"


def make_queue():
    verifier = FlakyVerifier()
    queue = VerificationQueue(verifiers=verifier)
    return queue, verifier


def run_batch(queue, batch_id="B1", proofs=SOURCE_PROOFS):
    job_id = queue.enqueue({"batch_id": batch_id, "proofs": list(proofs)})
    result = queue.run_next()
    return job_id, result


class RetryByErrorCodeTest(unittest.TestCase):
    def test_full_batch_retry_selects_matching_code(self):
        queue, verifier = make_queue()
        source_job, source_result = run_batch(queue)
        self.assertEqual(source_result.failed, 3)

        submission = queue.retry_by_error_code("B1", "rejected")
        self.assertIsInstance(submission, ProofRetrySubmission)
        self.assertEqual(submission.batch_id, "B1")
        self.assertEqual(submission.error_code, "rejected")
        self.assertEqual(submission.accepted, ["p2", "p4"])
        self.assertEqual(submission.skipped, [])
        self.assertIsNotNone(submission.job_id)
        self.assertEqual(queue.status(submission.job_id), "queued")

        # 恢复 p2 后执行重试作业：既有结果查询给出新验证状态与失败定位。
        verifier.healed.add("p2")
        retry_result = queue.run_next()
        self.assertEqual(retry_result.passed, 1)
        self.assertEqual(retry_result.failed, 1)
        self.assertEqual([f.proof_id for f in retry_result.failures], ["p4"])
        self.assertEqual(retry_result.failures[0].code, "rejected")
        self.assertIs(queue.result(submission.job_id), retry_result)
        self.assertIs(
            queue.report(submission.job_id).result,
            queue.result(submission.job_id),
        )

        # 历史失败结果不被改写：源作业结果保持原样。
        self.assertIs(queue.result(source_job), source_result)
        self.assertEqual(source_result.failed, 3)

        # 最新结果驱动下一轮筛选：p2 已成功，只剩 p4 可重试。
        again = queue.retry_by_error_code("B1", "rejected")
        self.assertEqual(again.accepted, ["p4"])
        self.assertEqual(again.skipped, [])

    def test_full_batch_retry_by_verify_error_code(self):
        queue, _ = make_queue()
        run_batch(queue)
        submission = queue.retry_by_error_code("B1", "verify_error")
        self.assertEqual(submission.accepted, ["p3"])
        self.assertEqual(submission.skipped, [])

    def test_retry_specific_proofs_input_order(self):
        queue, _ = make_queue()
        run_batch(queue)
        submission = queue.retry_by_error_code("B1", "rejected", ["p4", "p2"])
        self.assertEqual(submission.accepted, ["p4", "p2"])
        self.assertEqual(submission.skipped, [])
        # 子批次按接受顺序保留证明材料。
        job = queue._jobs[submission.job_id]
        self.assertEqual(
            [raw["proof_id"] for raw in job.batch["proofs"]], ["p4", "p2"]
        )
        self.assertEqual(job.batch["batch_id"], "B1")

    def test_mixed_accept_and_skip(self):
        queue, _ = make_queue()
        run_batch(queue)
        submission = queue.retry_by_error_code(
            "B1", "rejected", ["p2", "p1", "p3", "pX"]
        )
        self.assertEqual(submission.accepted, ["p2"])
        self.assertEqual(
            [(s.proof_id, s.reason) for s in submission.skipped],
            [
                ("p1", RETRY_SKIP_ALREADY_COMPLETED),
                ("p3", RETRY_SKIP_NOT_RETRYABLE),
                ("pX", RETRY_SKIP_UNKNOWN_PROOF),
            ],
        )
        # 跳过原因只取五种固定字面量之一。
        for skip in submission.skipped:
            self.assertIn(skip.reason, RETRY_SKIP_REASONS)

    def test_duplicate_request_deterministic(self):
        queue, verifier = make_queue()
        run_batch(queue)

        first = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(first.accepted, ["p2"])

        # 已接受并处于待验证：重复请求确定地记 ALREADY_QUEUED，不建作业。
        second = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(second.accepted, [])
        self.assertEqual(
            [(s.proof_id, s.reason) for s in second.skipped],
            [("p2", RETRY_SKIP_ALREADY_QUEUED)],
        )
        self.assertIsNone(second.job_id)
        third = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(third.accepted, second.accepted)
        self.assertEqual(
            [(s.proof_id, s.reason) for s in third.skipped],
            [(s.proof_id, s.reason) for s in second.skipped],
        )

        # 重试后仍失败：可再次重试，得到不同的新作业标识。
        queue.run_next()
        fourth = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(fourth.accepted, ["p2"])
        self.assertNotEqual(fourth.job_id, first.job_id)

        # 重试后通过：已有最新成功结果，确定地记 ALREADY_COMPLETED。
        verifier.healed.add("p2")
        queue.run_next()
        fifth = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(fifth.accepted, [])
        self.assertEqual(
            [(s.proof_id, s.reason) for s in fifth.skipped],
            [("p2", RETRY_SKIP_ALREADY_COMPLETED)],
        )
        self.assertIsNone(fifth.job_id)

    def test_batch_not_found(self):
        queue, _ = make_queue()
        run_batch(queue)
        with self.assertRaises(BatchNotFoundException):
            queue.retry_by_error_code("NOPE", "rejected")
        # 批次确认先于输入校验：批次不存在时即使选择非法也抛批次异常。
        with self.assertRaises(BatchNotFoundException):
            queue.retry_by_error_code("NOPE", "not-a-code", ["p1"])
        # *Error 别名与 *Exception 为同一类型。
        self.assertIs(BatchNotFoundError, BatchNotFoundException)
        with self.assertRaises(BatchNotFoundError):
            queue.retry_by_error_code("NOPE", "rejected")

    def test_proof_not_in_batch(self):
        queue, _ = make_queue()
        run_batch(queue, "B1")
        run_batch(queue, "B2", [proof("q1", key="g2")])
        # 已知归属其他批次的证明：请求级错误，不进入筛选。
        with self.assertRaises(ProofNotInBatchException):
            queue.retry_by_error_code("B1", "rejected", ["p2", "q1"])
        self.assertIs(ProofNotInBatchError, ProofNotInBatchException)
        # 完全未知的标识不是请求级错误：逐条记 UNKNOWN_PROOF 跳过。
        submission = queue.retry_by_error_code("B1", "rejected", ["zz"])
        self.assertEqual(submission.accepted, [])
        self.assertEqual(
            [(s.proof_id, s.reason) for s in submission.skipped],
            [("zz", RETRY_SKIP_UNKNOWN_PROOF)],
        )

    def test_invalid_retry_selection(self):
        queue, _ = make_queue()
        run_batch(queue)
        # 空 proof_ids。
        for empty in ([], ()):
            with self.assertRaises(InvalidRetrySelectionException):
                queue.retry_by_error_code("B1", "rejected", empty)
        # 非列表/元组、非字符串元素。
        for bad in ("p2", 123, ["p2", 7], ["p2", ""], [None]):
            with self.assertRaises(InvalidRetrySelectionException):
                queue.retry_by_error_code("B1", "rejected", bad)
        # 重复 proofId：直接报错而不静默去重。
        with self.assertRaises(InvalidRetrySelectionException):
            queue.retry_by_error_code("B1", "rejected", ["p2", "p2"])
        # 无法识别的失败错误码。
        for bad_code in ("nope", "", None, 123, "REJECTED"):
            with self.assertRaises(InvalidRetrySelectionException):
                queue.retry_by_error_code("B1", bad_code)
        self.assertIs(InvalidRetrySelectionError, InvalidRetrySelectionException)
        # 输入错误不建作业：队列中没有新增 queued 作业。
        self.assertIsNone(queue.run_next())

    def test_no_retryable_after_filtering(self):
        queue, _ = make_queue()
        run_batch(queue, "B9", [proof("a1"), proof("a2")])
        # 全部已通过：accepted 为空、不建作业，skipped 逐条给出原因。
        submission = queue.retry_by_error_code("B9", "rejected", ["a1", "a2"])
        self.assertEqual(submission.accepted, [])
        self.assertIsNone(submission.job_id)
        self.assertEqual(
            [(s.proof_id, s.reason) for s in submission.skipped],
            [("a1", RETRY_SKIP_ALREADY_COMPLETED),
             ("a2", RETRY_SKIP_ALREADY_COMPLETED)],
        )
        # 不提供 proof_ids 且无匹配失败：同样不报错、不建作业。
        empty = queue.retry_by_error_code("B9", "rejected")
        self.assertEqual(empty.accepted, [])
        self.assertEqual(empty.skipped, [])
        self.assertIsNone(empty.job_id)

    def test_cancelled_retry_restores_selection(self):
        queue, _ = make_queue()
        run_batch(queue)
        first = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(first.accepted, ["p2"])
        self.assertTrue(queue.cancel(first.job_id))
        # 取消后未重新验证：最新结果仍是旧失败，可再次重试。
        second = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(second.accepted, ["p2"])
        self.assertNotEqual(second.job_id, first.job_id)

    def test_pending_original_job_is_already_queued(self):
        queue, _ = make_queue()
        queue.enqueue({"batch_id": "B1", "proofs": list(SOURCE_PROOFS)})
        # 原始作业仍在待验证：显式请求记 ALREADY_QUEUED。
        submission = queue.retry_by_error_code("B1", "rejected", ["p2"])
        self.assertEqual(submission.accepted, [])
        self.assertEqual(
            [(s.proof_id, s.reason) for s in submission.skipped],
            [("p2", RETRY_SKIP_ALREADY_QUEUED)],
        )
        # 尚无失败定位：全批次选择为空，不报错。
        empty = queue.retry_by_error_code("B1", "rejected")
        self.assertEqual(empty.accepted, [])
        self.assertEqual(empty.skipped, [])

    def test_retry_job_uses_existing_queries(self):
        queue, verifier = make_queue()
        run_batch(queue)
        verifier.healed.update({"p2", "p4"})
        submission = queue.retry_by_error_code("B1", "rejected")
        queue.run_next()
        result = queue.result(submission.job_id)
        self.assertEqual(result.failed, 0)
        self.assertEqual(result.passed, 2)
        report = queue.report(submission.job_id)
        self.assertIs(report.result, result)
        # 重试作业同样是普通作业：既有复核入口的异常约定不变。
        with self.assertRaises(NoFailedProofError):
            queue.reverify_failures(submission.job_id)

    def test_response_to_dict_fixed_key_order(self):
        queue, _ = make_queue()
        run_batch(queue)
        submission = queue.retry_by_error_code(
            "B1", "rejected", ["p2", "p1"]
        )
        data = submission.to_dict()
        self.assertEqual(
            list(data.keys()),
            ["batch_id", "error_code", "job_id", "accepted", "skipped"],
        )
        self.assertEqual(data["batch_id"], "B1")
        self.assertEqual(data["error_code"], "rejected")
        self.assertEqual(data["accepted"], ["p2"])
        self.assertEqual(len(data["skipped"]), 1)
        self.assertEqual(
            list(data["skipped"][0].keys()), ["proof_id", "reason"]
        )
        self.assertEqual(
            data["skipped"][0],
            {"proof_id": "p1", "reason": RETRY_SKIP_ALREADY_COMPLETED},
        )

    def test_same_input_repeated_execution_same_result(self):
        # 两个相同现场执行相同输入得到相同响应（含作业标识序列）。
        responses = []
        for _ in range(2):
            queue, _ = make_queue()
            run_batch(queue)
            submission = queue.retry_by_error_code(
                "B1", "rejected", ["p4", "p1", "pX"]
            )
            responses.append(submission.to_dict())
        self.assertEqual(responses[0], responses[1])

    def test_skip_reason_constants(self):
        self.assertEqual(RETRY_SKIP_UNKNOWN_PROOF, "UNKNOWN_PROOF")
        self.assertEqual(RETRY_SKIP_PROOF_NOT_IN_BATCH, "PROOF_NOT_IN_BATCH")
        self.assertEqual(RETRY_SKIP_NOT_RETRYABLE, "NOT_RETRYABLE")
        self.assertEqual(RETRY_SKIP_ALREADY_QUEUED, "ALREADY_QUEUED")
        self.assertEqual(RETRY_SKIP_ALREADY_COMPLETED, "ALREADY_COMPLETED")
        self.assertEqual(len(RETRY_SKIP_REASONS), 5)


if __name__ == "__main__":
    unittest.main()
