"""VerificationTaskQueue（可排队、可定位失败原因的逐项验证）测试。

覆盖：
* 提交前完整校验：EmptyBatchError / BatchSizeLimitError /
  InvalidItemIdError / InvalidProofFormatError / DuplicateItemIdError
  的触发条件、固定优先级，以及校验失败不生成任务、不进入队列；
* 提交回执：稳定任务标识、queued 状态与批次摘要；
* 逐项验证：按输入顺序、单证失败不中止其他项、每项只验证一次、
  稳定的 stage/code、failed_item_ids 顺序、计数自洽、脱敏与固定键序；
* 进度：queued/processing/completed 的真实 completed 计数，未结束不提前
  给出最终结果；
* 任务异常：TaskNotFoundError、重复消费/终态再消费/未结束取结果的
  TaskStateConflictError；
* 基础设施错误：无法读取材料（invalid_proof）、验证器执行失败
  （verifier_fault）、无验证器（verifier_unavailable）、无法保存结果
  （result_save_fault），均保留此前已完成项；
* 终态与结果查询幂等，不再次调用验证器；
* 复核：retry_failed 从 completed/failed 源任务选取待复核项（失败项、
  错误项及其后无结果项，按根任务输入顺序去重），生成独立 queued 任务，
  ItemResult.index 保留根任务下标；NoRetryableItemsError /
  TaskNotFoundError / TaskStateConflictError 互不替代。

直接运行：python tests/test_zk_batch_tasks.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchSizeLimitError,
    BatchTaskResult,
    DuplicateItemIdError,
    EmptyBatchError,
    InvalidItemIdError,
    InvalidProofFormatError,
    ItemResult,
    MAX_BATCH_ITEMS,
    NoRetryableItemsError,
    TaskNotFoundError,
    TaskProgress,
    TaskRetrySubmission,
    TaskStateConflictError,
    VerificationInfrastructureError,
    VerificationTaskQueue,
    ZKVerifier,
)


def material(pid, proto="groth16", circuit="c1", key="k1", inputs=None,
             body=None):
    """与聚合引擎同口径的完整证明材料。"""
    return {
        "proof_id": pid,
        "protocol": proto,
        "circuit_id": circuit,
        "aggregation_key": key,
        "public_inputs": inputs if inputs is not None else ["pi"],
        "proof": body if body is not None else ("blob", pid),
    }


def item(iid, mat=None):
    return {"item_id": iid, "proof": mat if mat is not None else material(iid)}


class StubVerifier(ZKVerifier):
    """按 proof_id 编排通过/拒绝/异常的假验证器。"""

    protocol = "groth16"

    def __init__(self, reject_ids=(), error_ids=(), non_bool_ids=(),
                 missing=False):
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._non_bool = set(non_bool_ids)
        self._missing = missing
        self.verified = []

    def verify(self, p):
        self.verified.append(p.proof_id)
        if p.proof_id in self._errors:
            raise RuntimeError("verifier exploded: " + p.proof_id)
        if p.proof_id in self._non_bool:
            return "yes"
        return p.proof_id not in self._reject


class OtherVerifier(ZKVerifier):
    protocol = "plonk"

    def verify(self, p):
        return True


class VerifyLessVerifier(ZKVerifier):
    protocol = "halo2"
    # 不实现 verify


# ================================================================ 提交校验

class TestSubmitValidation(unittest.TestCase):
    def test_empty_batch_variants(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(EmptyBatchError):
            q.submit([])
        with self.assertRaises(EmptyBatchError):
            q.submit(())
        with self.assertRaises(EmptyBatchError):
            q.submit("not-a-list")
        with self.assertRaises(EmptyBatchError):
            q.submit(None)

    def test_size_limit(self):
        q = VerificationTaskQueue(StubVerifier())
        items = [item(f"i{k}") for k in range(MAX_BATCH_ITEMS + 1)]
        with self.assertRaises(BatchSizeLimitError) as ctx:
            q.submit(items)
        self.assertEqual(ctx.exception.count, MAX_BATCH_ITEMS + 1)
        self.assertEqual(ctx.exception.limit, MAX_BATCH_ITEMS)
        # 恰好等于上限允许提交
        ok = q.submit(items[:MAX_BATCH_ITEMS])
        self.assertEqual(ok.summary.total, MAX_BATCH_ITEMS)

    def test_invalid_item_id_variants(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"proof": material("p1")}])  # 缺 item_id
        with self.assertRaises(InvalidItemIdError):
            q.submit([item("")])
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"item_id": 7, "proof": material("p1")}])
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"item_id": None, "proof": material("p1")}])
        with self.assertRaises(InvalidItemIdError):
            q.submit(["plain-string"])

    def test_invalid_proof_format_variants(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a"}])  # 缺 proof
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof": None}])
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof": b"bytes"}])
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof": ["list"]}])
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof": {}}])  # 空映射

    def test_duplicate_item_id(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(DuplicateItemIdError) as ctx:
            q.submit([item("a"), item("b"), item("a")])
        self.assertEqual(ctx.exception.item_id, "a")

    def test_validation_priority_empty_before_size(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(EmptyBatchError):
            q.submit([])

    def test_validation_priority_size_before_item_fields(self):
        q = VerificationTaskQueue(StubVerifier())
        items = [{"item_id": ""}] * (MAX_BATCH_ITEMS + 1)
        with self.assertRaises(BatchSizeLimitError):
            q.submit(items)

    def test_validation_priority_per_item_before_duplicate(self):
        q = VerificationTaskQueue(StubVerifier())
        # 第 1 项标识非法先于任何重复检出
        with self.assertRaises(InvalidItemIdError):
            q.submit([item("dup"), {"item_id": ""}, item("dup")])
        # 第 2 项材料格式非法先于第 3 项与第 1 项重复
        with self.assertRaises(InvalidProofFormatError):
            q.submit([
                item("dup"),
                {"item_id": "x", "proof": {}},
                item("dup"),
            ])

    def test_validation_priority_id_before_format_within_item(self):
        q = VerificationTaskQueue(StubVerifier())
        # 同一项内标识非法先于材料格式检查
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"item_id": "", "proof": {}}])

    def test_rejected_validation_leaves_no_task(self):
        q = VerificationTaskQueue(StubVerifier())
        for bad in (
            lambda: q.submit([]),
            lambda: q.submit([item("a"), item("a")]),
            lambda: q.submit([{"item_id": "a"}]),
        ):
            with self.assertRaises(Exception):
                bad()
        # 没有任何任务入队：无可消费任务，任何标识都查不到
        self.assertIsNone(q.run_next())
        with self.assertRaises(TaskNotFoundError):
            q.status("task-1")


# ================================================================ 提交回执

class TestSubmissionReceipt(unittest.TestCase):
    def test_receipt_fields(self):
        q = VerificationTaskQueue(StubVerifier())
        receipt = q.submit([item("z"), item("a"), item("m")])
        self.assertIsInstance(receipt.task_id, str)
        self.assertTrue(receipt.task_id)
        self.assertEqual(receipt.status, "queued")
        self.assertEqual(receipt.summary.total, 3)
        self.assertEqual(receipt.summary.item_ids, ["z", "a", "m"])
        self.assertEqual(
            receipt.to_dict(),
            {
                "task_id": receipt.task_id,
                "status": "queued",
                "summary": {"total": 3, "item_ids": ["z", "a", "m"]},
            },
        )

    def test_task_ids_stable_and_distinct(self):
        q = VerificationTaskQueue(StubVerifier())
        r1 = q.submit([item("a")])
        r2 = q.submit([item("a")])  # 重新提交同内容是独立新任务
        self.assertNotEqual(r1.task_id, r2.task_id)
        self.assertEqual(q.status(r1.task_id), "queued")
        self.assertEqual(q.status(r2.task_id), "queued")


# ================================================================ 逐项执行

class TestItemwiseVerification(unittest.TestCase):
    def test_all_pass_order_and_counts(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("z"), item("a"), item("m")]).task_id
        result = q.run_next()
        self.assertIsInstance(result, BatchTaskResult)
        self.assertEqual(result.task_id, tid)
        self.assertEqual((result.total, result.passed, result.failed), (3, 3, 0))
        self.assertEqual(result.failed_item_ids, [])
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in result.results],
            [(0, "z", "passed"), (1, "a", "passed"), (2, "m", "passed")],
        )
        # 每项恰好验证一次，顺序与输入一致
        self.assertEqual(q.status(tid), "completed")

    def test_single_failure_does_not_abort_batch(self):
        # p2 被拒（普通业务失败）后，p3、p4 必须继续验证
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        r = q.run_next()
        self.assertEqual(q.status(tid), "completed")
        self.assertEqual((r.total, r.passed, r.failed), (4, 3, 1))
        self.assertEqual(r.failed_item_ids, ["p2"])
        failed = r.results[1]
        self.assertEqual(
            (failed.index, failed.item_id, failed.status,
             failed.stage, failed.code),
            (1, "p2", "failed", "verify", "rejected"),
        )
        self.assertTrue(failed.message)  # 有人工定位描述
        self.assertEqual(
            [r.status for r in r.results],
            ["passed", "failed", "passed", "passed"],
        )

    def test_each_item_verified_exactly_once(self):
        v = StubVerifier(reject_ids={"p2"})
        q = VerificationTaskQueue(v)
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        q.run_next()
        self.assertEqual(v.verified, ["p1", "p2", "p3"])
        # 终态后反复查询不再次验证
        for _ in range(3):
            q.result(tid)
            q.progress(tid)
        self.assertEqual(v.verified, ["p1", "p2", "p3"])

    def test_results_share_input_order_not_sorted(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"z", "m"}))
        tid = q.submit([item("z"), item("a"), item("m")]).task_id
        r = q.run_next()
        # 结果与失败标识列表都保持输入顺序，不按 id 排序
        self.assertEqual([x.item_id for x in r.results], ["z", "a", "m"])
        self.assertEqual(r.failed_item_ids, ["z", "m"])
        self.assertEqual((r.passed, r.failed), (1, 2))

    def test_item_result_to_dict_key_order(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        q.submit([item("p1"), item("p2")])
        payload = q.run_next().to_dict()
        self.assertEqual(
            list(payload.keys()),
            ["task_id", "total", "passed", "failed", "results",
             "failed_item_ids"],
        )
        self.assertEqual(
            list(payload["results"][0].keys()),
            ["index", "item_id", "status", "stage", "code", "message"],
        )
        # 通过项 stage/code/message 为空串，不臆造内容
        self.assertEqual(
            payload["results"][0],
            {"index": 0, "item_id": "p1", "status": "passed",
             "stage": "", "code": "", "message": ""},
        )
        self.assertEqual(payload["failed_item_ids"], ["p2"])

    def test_messages_and_payload_never_contain_proof_material(self):
        secret_inputs = ["SECRET-INPUT-xyz"]
        secret_body = {"secret": "SECRET-PROOF-abc"}
        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p1"}, error_ids={"p2"})
        )
        tid = q.submit([
            item("p1", material("p1", inputs=secret_inputs, body=secret_body)),
            item("p2", material("p2", inputs=secret_inputs, body=secret_body)),
        ]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        # failed 终态的部分结果、进度快照、错误对象都不得泄露材料
        blob = str(q.result(tid).to_dict())
        blob += str(q.progress(tid).to_dict())
        blob += str(q.task_error(tid).args)
        self.assertNotIn("SECRET-INPUT-xyz", blob)
        self.assertNotIn("SECRET-PROOF-abc", blob)

    def test_per_submit_verifier_override_and_protocols(self):
        # 默认验证器只认识 groth16；提交时覆盖为 plonk 验证器
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit(
            [item("a", material("a", proto="plonk"))],
            verifiers=[OtherVerifier()],
        ).task_id
        self.assertEqual(q.run_next().passed, 1)

        # 字典形式按 protocol 选择，两类证明各自走对应验证器
        q2 = VerificationTaskQueue()
        q2.submit(
            [item("g", material("g", proto="groth16")),
             item("p", material("p", proto="plonk"))],
            verifiers={"groth16": StubVerifier(), "plonk": OtherVerifier()},
        )
        r = q2.run_next()
        self.assertEqual((r.passed, r.failed), (2, 0))


# ================================================================ 进度

class TestProgress(unittest.TestCase):
    def test_queued_progress(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("a"), item("b")]).task_id
        p = q.progress(tid)
        self.assertIsInstance(p, TaskProgress)
        self.assertEqual(p.status, "queued")
        self.assertEqual((p.total, p.completed), (2, 0))
        self.assertIsNone(p.result)

    def test_processing_progress_is_real(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        tid = q.submit([item("a"), item("b"), item("c")]).task_id

        seen = []

        def spy(proof):
            # 第一项验证期间：processing，completed=0 且无最终结果
            if proof.proof_id == "a":
                p = q.progress(tid)
                self.assertEqual(p.status, "processing")
                self.assertEqual((p.total, p.completed), (3, 0))
                self.assertIsNone(p.result)
            if proof.proof_id == "c":
                p = q.progress(tid)
                # a、b 已完成
                self.assertEqual(p.status, "processing")
                self.assertEqual(p.completed, 2)
                self.assertIsNone(p.result)
                seen.append("observed")
            return True

        v.verify = spy
        q.run_next()
        self.assertEqual(seen, ["observed"])
        p = q.progress(tid)
        self.assertEqual(p.status, "completed")
        self.assertEqual(p.completed, 3)
        self.assertIsNotNone(p.result)
        self.assertIs(p.result, q.result(tid))


# ================================================================ 状态冲突

class TestTaskErrors(unittest.TestCase):
    def test_unknown_task_all_queries(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(TaskNotFoundError):
            q.status("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.progress("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.result("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.run_task("ghost")

    def test_result_before_finish_conflict(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("a")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.result(tid)

    def test_double_consume_during_processing(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        tid = q.submit([item("a")]).task_id
        other = q.submit([item("b")]).task_id

        def reenter(proof):
            # 处理中重复消费当前任务、消费别的任务、run_next 都冲突
            with self.assertRaises(TaskStateConflictError):
                q.run_task(tid)
            with self.assertRaises(TaskStateConflictError):
                q.run_task(other)
            with self.assertRaises(TaskStateConflictError):
                q.run_next()
            return True

        v.verify = reenter
        q.run_next()

    def test_terminal_task_not_consumed_again(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("a")]).task_id
        q.run_next()
        # 终态后显式再次入队/消费 -> 冲突
        with self.assertRaises(TaskStateConflictError):
            q.run_task(tid)
        # run_next 不再捞起终态任务：队列空返回 None
        self.assertIsNone(q.run_next())
        # 另一个排队任务仍可正常消费
        other = q.submit([item("b")]).task_id
        self.assertEqual(q.run_next().task_id, other)
        with self.assertRaises(TaskStateConflictError):
            q.run_task(other)

    def test_run_task_specific_and_fifo(self):
        q = VerificationTaskQueue(StubVerifier())
        t1 = q.submit([item("a")]).task_id
        t2 = q.submit([item("b")]).task_id
        # run_task 可越过队头消费指定任务
        self.assertEqual(q.run_task(t2).task_id, t2)
        self.assertEqual(q.status(t1), "queued")
        # 队头 t1 仍由 run_next 消费
        self.assertEqual(q.run_next().task_id, t1)


# ================================================================ 基础设施错误

class TestInfrastructureErrors(unittest.TestCase):
    def test_unreadable_proof_material_preserves_prior_items(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        broken = {"proof_id": "p2"}  # 非空映射通过提交校验，执行期读不出
        tid = q.submit([
            item("p1"),
            {"item_id": "p2", "proof": broken},
            item("p3"),
        ]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        err = ctx.exception
        self.assertEqual((err.index, err.item_id, err.stage, err.code),
                         (1, "p2", "proof_read", "invalid_proof"))
        # 此前已完成项保留；出错项与其后项不出现；total 仍是整批
        result = q.result(tid)
        self.assertEqual([r.item_id for r in result.results], ["p1"])
        self.assertEqual(result.total, 3)
        self.assertEqual((result.passed, result.failed), (1, 0))
        # 出错项之后的 p3 从未验证
        self.assertEqual(v.verified, ["p1"])
        # 终态查询幂等
        self.assertIs(q.result(tid), q.result(tid))
        self.assertIs(q.task_error(tid), err)

    def test_verifier_raises_is_infrastructure_fault(self):
        q = VerificationTaskQueue(StubVerifier(error_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        err = ctx.exception
        self.assertEqual((err.index, err.item_id, err.stage),
                         (1, "p2", "verify"))
        self.assertEqual(err.code, "verifier_fault")
        result = q.result(tid)
        self.assertEqual([r.item_id for r in result.results], ["p1"])
        self.assertNotIn("Traceback", str(err))

    def test_unknown_protocol_unavailable(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit(
            [item("p1", material("p1", proto="mystery"))]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        err = ctx.exception
        self.assertEqual(err.code, "verifier_unavailable")
        self.assertEqual(err.stage, "verify")
        self.assertEqual(err.item_id, "p1")
        self.assertEqual(q.result(tid).results, [])

    def test_contract_violation_is_fault(self):
        q = VerificationTaskQueue(VerifyLessVerifier())
        tid = q.submit(
            [item("p1", material("p1", proto="halo2"))]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.code, "verifier_fault")

        q2 = VerificationTaskQueue(
            StubVerifier(non_bool_ids={"p1"})
        )
        t2 = q2.submit([item("p1")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q2.run_next()
        self.assertEqual(ctx.exception.code, "verifier_fault")
        self.assertEqual(q2.result(t2).results, [])

    def test_save_failure_keeps_completed_results(self):
        saved = []

        def store(result):
            saved.append(result)
            raise OSError("disk full")

        v = StubVerifier(reject_ids={"p2"})
        q = VerificationTaskQueue(v, result_store=store)
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        err = ctx.exception
        self.assertEqual(err.stage, "result_save")
        self.assertEqual(err.code, "result_save_fault")
        self.assertIsNone(err.item_id)
        self.assertEqual(q.status(tid), "failed")
        # 所有项都已处理完，结果仍可幂等查询
        result = q.result(tid)
        self.assertEqual(result.total, 3)
        self.assertEqual((result.passed, result.failed), (2, 1))
        self.assertEqual(result.failed_item_ids, ["p2"])
        self.assertIs(q.result(tid), result)
        # 保存钩子只被调用一次，没有重复执行
        self.assertEqual(len(saved), 1)

    def test_successful_store_called_once(self):
        saved = []
        q = VerificationTaskQueue(StubVerifier(), result_store=saved.append)
        tid = q.submit([item("a"), item("b")]).task_id
        result = q.run_next()
        self.assertEqual(saved, [result])
        q.result(tid)
        q.progress(tid)
        self.assertEqual(len(saved), 1)

    def test_failed_task_progress_exposes_partial_result(self):
        q = VerificationTaskQueue(StubVerifier(error_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        p = q.progress(tid)
        self.assertEqual(p.status, "failed")
        self.assertEqual((p.total, p.completed), (3, 1))
        self.assertIsNotNone(p.result)
        self.assertEqual(
            [r["item_id"] for r in p.result.to_dict()["results"]], ["p1"]
        )


# ================================================================ 幂等

class TestIdempotency(unittest.TestCase):
    def test_terminal_queries_idempotent(self):
        v = StubVerifier(reject_ids={"p2"})
        q = VerificationTaskQueue(v)
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        r1 = q.result(tid)
        r2 = q.result(tid)
        self.assertIs(r1, r2)
        self.assertIs(q.progress(tid).result, r1)
        self.assertEqual(v.verified, ["p1", "p2"])
        # 状态不因查询变化
        self.assertEqual(q.status(tid), "completed")


# ================================================================ 复核

class TestRetryFailed(unittest.TestCase):
    def test_completed_selects_only_failed_items(self):
        v = StubVerifier(reject_ids={"p2", "p4"})
        q = VerificationTaskQueue(v)
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        q.run_next()
        receipt = q.retry_failed(tid)
        self.assertIsInstance(receipt, TaskRetrySubmission)
        self.assertEqual(receipt.source_task_id, tid)
        self.assertNotEqual(receipt.task_id, tid)
        self.assertEqual(receipt.status, "queued")
        self.assertEqual(receipt.summary.total, 2)
        self.assertEqual(receipt.summary.item_ids, ["p2", "p4"])
        self.assertEqual(receipt.retried_indexes, [1, 3])
        self.assertEqual(q.status(receipt.task_id), "queued")
        # to_dict 固定字段顺序，summary 走 BatchSummary.to_dict
        self.assertEqual(
            list(receipt.to_dict()),
            ["source_task_id", "task_id", "status",
             "summary", "retried_indexes"],
        )
        self.assertEqual(
            receipt.to_dict(),
            {
                "source_task_id": tid,
                "task_id": receipt.task_id,
                "status": "queued",
                "summary": {"total": 2, "item_ids": ["p2", "p4"]},
                "retried_indexes": [1, 3],
            },
        )

    def test_retry_task_preserves_root_indexes(self):
        v = StubVerifier(reject_ids={"p2", "p4"})
        q = VerificationTaskQueue(v)
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        q.run_next()
        self.assertEqual(v.verified, ["p1", "p2", "p3", "p4"])

        v2 = StubVerifier(reject_ids={"p4"})
        receipt = q.retry_failed(tid, verifiers=v2)
        result = q.run_task(receipt.task_id)
        # 只验证入选项，每项恰好一次
        self.assertEqual(v2.verified, ["p2", "p4"])
        # ItemResult.index 保留根任务下标
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in result.results],
            [(1, "p2", "passed"), (3, "p4", "failed")],
        )
        self.assertEqual((result.total, result.passed, result.failed),
                         (2, 1, 1))
        self.assertEqual(result.failed_item_ids, ["p4"])
        # 沿用既有查询：status/progress/result/task_error
        self.assertEqual(q.status(receipt.task_id), "completed")
        self.assertIs(q.result(receipt.task_id), result)
        self.assertIs(q.progress(receipt.task_id).result, result)
        self.assertIsNone(q.task_error(receipt.task_id))

    def test_failed_source_selects_error_item_tail_and_failed(self):
        v = StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        q = VerificationTaskQueue(v)
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4"), item("p5")]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        receipt = q.retry_failed(tid)
        # p2 已有失败结果 + p4 错误项 + p5 尚无结果项，按根任务输入顺序去重
        self.assertEqual(receipt.retried_indexes, [1, 3, 4])
        self.assertEqual(receipt.summary.item_ids, ["p2", "p4", "p5"])

    def test_save_fault_with_full_coverage_selects_only_failed(self):
        def store(result):
            raise OSError("disk full")

        v = StubVerifier(reject_ids={"p2"})
        q = VerificationTaskQueue(v, result_store=store)
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        receipt = q.retry_failed(tid)
        # 结果已覆盖全部输入：只选失败项
        self.assertEqual(receipt.retried_indexes, [1])
        self.assertEqual(receipt.summary.item_ids, ["p2"])

    def test_no_retryable_items(self):
        # completed 且全部通过
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        with self.assertRaises(NoRetryableItemsError):
            q.retry_failed(tid)

        # result_save 失败、结果覆盖全部输入且全部通过
        def store(result):
            raise OSError("disk full")

        q2 = VerificationTaskQueue(StubVerifier(), result_store=store)
        t2 = q2.submit([item("p1")]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q2.run_next()
        with self.assertRaises(NoRetryableItemsError) as ctx:
            q2.retry_failed(t2)
        # 异常消息不含证明材料与验证器信息
        msg = str(ctx.exception)
        self.assertNotIn("blob", msg)
        self.assertNotIn("public_inputs", msg)
        self.assertNotIn("Verifier", msg)

    def test_unknown_task(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(TaskNotFoundError):
            q.retry_failed("ghost")

    def test_non_terminal_source_conflict(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        tid = q.submit([item("a")]).task_id
        # queued 源任务
        with self.assertRaises(TaskStateConflictError):
            q.retry_failed(tid)

        # processing 源任务（验证器回调内重入）
        def reenter(proof):
            with self.assertRaises(TaskStateConflictError):
                q.retry_failed(tid)
            return True

        v.verify = reenter
        q.run_next()

    def test_verifiers_omitted_inherits_source_choice(self):
        # 队列默认全部通过；源任务提交时给定拒绝 p2 的验证器
        source_v = StubVerifier(reject_ids={"p2"})
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit(
            [item("p1"), item("p2")], verifiers=source_v
        ).task_id
        q.run_next()
        # 省略 verifiers：继承源任务选择，而非队列默认
        receipt = q.retry_failed(tid)
        result = q.run_task(receipt.task_id)
        self.assertEqual(result.failed_item_ids, ["p2"])
        # 显式 verifiers 只作用于新任务
        pass_v = StubVerifier()
        receipt2 = q.retry_failed(tid, verifiers=pass_v)
        r2 = q.run_task(receipt2.task_id)
        self.assertEqual(r2.failed_item_ids, [])
        self.assertEqual(pass_v.verified, ["p2"])
        # 源任务结果不受影响
        self.assertEqual(q.result(tid).failed_item_ids, ["p2"])

    def test_verifiers_contract_error_surfaces_at_execution(self):
        source_v = StubVerifier(reject_ids={"p1"})
        source_v.protocol = "halo2"
        q = VerificationTaskQueue(source_v)
        tid = q.submit(
            [item("p1", material("p1", proto="halo2"))]
        ).task_id
        q.run_next()
        # 与 submit 同口径：契约异常在执行期转化为基础设施错误，
        # retry_failed 本身不抛
        receipt = q.retry_failed(tid, verifiers=VerifyLessVerifier())
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_task(receipt.task_id)
        self.assertEqual(ctx.exception.code, "verifier_fault")
        self.assertEqual(q.status(receipt.task_id), "failed")

    def test_repeated_retries_distinct_ids_stable_order(self):
        v = StubVerifier(reject_ids={"p3", "p1"})
        q = VerificationTaskQueue(v)
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        result = q.run_next()
        r1 = q.retry_failed(tid)
        r2 = q.retry_failed(tid)
        self.assertNotEqual(r1.task_id, r2.task_id)
        self.assertEqual(r1.retried_indexes, [0, 2])
        self.assertEqual(r2.retried_indexes, [0, 2])
        # 源任务状态、结果与错误定位不变
        self.assertEqual(q.status(tid), "completed")
        self.assertIs(q.result(tid), result)
        self.assertIsNone(q.task_error(tid))
        # 两个复核任务各自独立排队
        self.assertEqual(q.status(r1.task_id), "queued")
        self.assertEqual(q.status(r2.task_id), "queued")

    def test_retry_of_retry_keeps_root_indexes(self):
        v = StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        q = VerificationTaskQueue(v)
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        # 第一轮复核：p2 失败 + p4 错误项（根任务下标 1、3）
        first = q.retry_failed(tid)
        self.assertEqual(first.retried_indexes, [1, 3])
        # 继承的源验证器仍会在 p4 上出错：再次失败仍定位根任务下标
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_task(first.task_id)
        err = ctx.exception
        self.assertEqual((err.index, err.item_id), (3, "p4"))
        self.assertEqual(
            [(r.index, r.item_id, r.status)
             for r in q.result(first.task_id).results],
            [(1, "p2", "failed")],
        )
        # 第二轮复核：p2 已有失败结果 + p4 错误项，仍按根任务下标
        second = q.retry_failed(first.task_id)
        self.assertEqual(second.source_task_id, first.task_id)
        self.assertEqual(second.retried_indexes, [1, 3])
        # 用全部通过的验证器执行第二轮复核
        pass_v = StubVerifier()
        third = q.retry_failed(first.task_id, verifiers=pass_v)
        r3 = q.run_task(third.task_id)
        self.assertEqual(pass_v.verified, ["p2", "p4"])
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in r3.results],
            [(1, "p2", "passed"), (3, "p4", "passed")],
        )
        # 全部通过后无可复核项
        with self.assertRaises(NoRetryableItemsError):
            q.retry_failed(third.task_id)

    def test_retry_task_consumed_via_run_next_fifo(self):
        v = StubVerifier(reject_ids={"p1"})
        q = VerificationTaskQueue(v)
        tid = q.submit([item("p1")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        other = q.submit([item("x")]).task_id
        # 复核任务先入队，run_next 按 FIFO 消费
        self.assertEqual(q.run_next().task_id, receipt.task_id)
        self.assertEqual(q.run_next().task_id, other)


if __name__ == "__main__":
    unittest.main(verbosity=2)
