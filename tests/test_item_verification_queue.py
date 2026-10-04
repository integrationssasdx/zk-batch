"""ItemVerificationQueue（排队逐项验证）测试。

覆盖：
* 提交期完整输入校验与异常固定/优先级（Empty/InvalidItemId/
  BatchSizeLimit/InvalidProofFormat/DuplicateItemId），校验失败不入队；
* 回执（稳定 task_id、queued 状态、批次摘要）与 to_dict 固定键序；
* 逐项执行、输出与输入同序、单项失败不中止、已完成项不重复验证；
* processing 真实进度、未结束不提前给最终结果；
* 查询幂等、不再次调用验证器、TaskNotFoundError；
* 重复消费/终态再次入队/非法迁移 → TaskStateConflictError；
* 基础设施故障（读材料/执行/保存）→ VerificationInfrastructureError，
  保留已完成项，retry 续跑跳过已完成项；
* 定位只含序号/标识/阶段/错误码，不暴露证明材料、调用栈、未公开信息。

直接运行：python tests/test_item_verification_queue.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchSizeLimitError,
    DuplicateItemIdError,
    EmptyBatchError,
    INFRA_CODE_CONTRACT,
    INFRA_CODE_EXECUTION,
    INFRA_CODE_MATERIAL,
    INFRA_CODE_SAVE,
    INFRA_CODE_UNSUPPORTED,
    INFRA_STAGE_READ,
    INFRA_STAGE_SAVE,
    INFRA_STAGE_VERIFY,
    InvalidItemIdError,
    InvalidProofFormatError,
    ItemVerificationQueue,
    MAX_BATCH_ITEMS,
    TaskNotFoundError,
    TaskStateConflictError,
    VerificationInfrastructureError,
    ZKVerifier,
)


def material(proto="groth16", circuit="c1", key="k1", inputs=None, body=None):
    return {
        "protocol": proto,
        "circuit_id": circuit,
        "aggregation_key": key,
        "public_inputs": inputs if inputs is not None else ["pi"],
        "proof": body if body is not None else {"blob": "proof"},
    }


def item(iid, mat=None):
    return {"item_id": iid, "proof_material": mat if mat is not None else material()}


class StubVerifier(ZKVerifier):
    """按 item_id 编排通过/拒绝/异常，并记录调用次序。"""

    protocol = "groth16"

    def __init__(self, reject_ids=(), error_ids=()):
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self.verified = []

    def verify(self, p):
        self.verified.append(p.proof_id)
        if p.proof_id in self._errors:
            raise RuntimeError("verifier exploded: internal-detail")
        return p.proof_id not in self._reject


class FlakyOnceVerifier(ZKVerifier):
    """对指定 id 的前 fail_times 次调用抛异常，之后通过。"""

    protocol = "groth16"

    def __init__(self, fail_id, fail_times=1):
        self._fail_id = fail_id
        self._fail_times = fail_times
        self.calls = []

    def verify(self, p):
        self.calls.append(p.proof_id)
        if p.proof_id == self._fail_id and self.calls.count(p.proof_id) <= self._fail_times:
            raise RuntimeError("temporary outage")
        return True


class NonBoolVerifier(ZKVerifier):
    protocol = "groth16"

    def verify(self, p):
        return "yes"


# ================================================================ 提交校验

class TestSubmitValidation(unittest.TestCase):
    def test_empty_batch_variants(self):
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(EmptyBatchError):
            q.submit([])
        with self.assertRaises(EmptyBatchError):
            q.submit(None)
        with self.assertRaises(EmptyBatchError):
            q.submit("not-a-list")
        with self.assertRaises(EmptyBatchError):
            q.submit({})  # 映射不是列表

    def test_invalid_item_id(self):
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"proof_material": material()}])  # 缺 item_id
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"item_id": "", "proof_material": material()}])
        with self.assertRaises(InvalidItemIdError):
            q.submit([{"item_id": 123, "proof_material": material()}])
        with self.assertRaises(InvalidItemIdError):
            q.submit([["not-a-mapping"]])

    def test_size_limit(self):
        q = ItemVerificationQueue(StubVerifier(), max_items=3)
        with self.assertRaises(BatchSizeLimitError) as ctx:
            q.submit([item(f"i{i}") for i in range(4)])
        self.assertEqual(ctx.exception.size, 4)
        self.assertEqual(ctx.exception.limit, 3)
        # 上限处可以提交
        receipt = q.submit([item(f"i{i}") for i in range(3)])
        self.assertEqual(receipt.summary.total, 3)

    def test_default_limit_constant(self):
        self.assertGreaterEqual(MAX_BATCH_ITEMS, 1)
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(BatchSizeLimitError):
            q.submit([item(f"i{i}") for i in range(MAX_BATCH_ITEMS + 1)])

    def test_invalid_proof_format(self):
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof_material": None}])
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof_material": "nope"}])

        bad = material()
        del bad["circuit_id"]
        with self.assertRaises(InvalidProofFormatError):
            q.submit([item("a", bad)])

        with self.assertRaises(InvalidProofFormatError):
            q.submit([item("a", material(proto=""))])
        with self.assertRaises(InvalidProofFormatError):
            q.submit([item("a", material(proto=123))])
        with self.assertRaises(InvalidProofFormatError):
            q.submit([item("a", material(inputs=("tuple",)))])

        # 缺 proof 字段也算格式错误
        no_body = material()
        del no_body["proof"]
        with self.assertRaises(InvalidProofFormatError):
            q.submit([item("a", no_body)])

    def test_duplicate_item_id(self):
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(DuplicateItemIdError) as ctx:
            q.submit([item("dup"), item("x"), item("dup")])
        self.assertEqual(ctx.exception.item_id, "dup")

    def test_validation_precedence(self):
        q = ItemVerificationQueue(StubVerifier(), max_items=1)

        # 空批次先于一切
        with self.assertRaises(EmptyBatchError):
            q.submit([])

        # 非法标识先于数量上限（2 项超限、且标识为空）
        with self.assertRaises(InvalidItemIdError):
            q.submit(
                [
                    {"item_id": "", "proof_material": material()},
                    {"item_id": "b", "proof_material": material()},
                ]
            )

        # 数量上限先于材料格式（2 项超限，且材料坏）
        with self.assertRaises(BatchSizeLimitError):
            q.submit([item("a"), {"item_id": "b", "proof_material": None}])

        # 材料格式先于重复标识（用默认上限的队列，避免数量限制先触发）
        qf = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(InvalidProofFormatError):
            qf.submit(
                [
                    {"item_id": "dup", "proof_material": material()},
                    {"item_id": "dup", "proof_material": None},
                ]
            )

        # 标识合法、材料合法时才轮到重复
        with self.assertRaises(DuplicateItemIdError):
            qf.submit([item("dup"), item("dup")])

    def test_validation_failure_does_not_enqueue(self):
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(DuplicateItemIdError):
            q.submit([item("dup"), item("dup")])
        # 没有任何任务进入队列
        self.assertIsNone(q.run_next())
        with self.assertRaises(TaskNotFoundError):
            q.get_task("task-1")


# ================================================================ 回执与摘要

class TestReceipt(unittest.TestCase):
    def test_receipt_shape_and_stable_id(self):
        q = ItemVerificationQueue(StubVerifier())
        receipt = q.submit([item("a"), item("b"), item("c")])
        self.assertEqual(receipt.task_id, "task-1")
        self.assertEqual(receipt.status, "queued")
        self.assertEqual(receipt.summary.total, 3)
        self.assertEqual(receipt.summary.item_ids, ["a", "b", "c"])

        second = q.submit([item("x")])
        self.assertEqual(second.task_id, "task-2")
        self.assertNotEqual(receipt.task_id, second.task_id)

    def test_receipt_to_dict_key_order(self):
        q = ItemVerificationQueue(StubVerifier())
        payload = q.submit([item("a"), item("b")]).to_dict()
        self.assertEqual(list(payload.keys()), ["task_id", "status", "summary"])
        self.assertEqual(list(payload["summary"].keys()), ["total", "item_ids"])
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["summary"], {"total": 2, "item_ids": ["a", "b"]})


# ================================================================ 逐项执行

class TestExecution(unittest.TestCase):
    def test_all_passed(self):
        v = StubVerifier()
        q = ItemVerificationQueue(v)
        tid = q.submit([item("a"), item("b"), item("c")]).task_id
        result = q.run_next()

        self.assertEqual(q.status(tid), "completed")
        self.assertEqual(result.total, 3)
        self.assertEqual(result.passed_count, 3)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(result.failed_item_ids, [])
        # 验证按输入顺序、每项一次
        self.assertEqual(v.verified, ["a", "b", "c"])
        # 结果顺序 == 输入顺序
        self.assertEqual(
            [(r.index, r.item_id, r.passed) for r in result.results],
            [(0, "a", True), (1, "b", True), (2, "c", True)],
        )
        for r in result.results:
            self.assertEqual(r.stage, "")
            self.assertEqual(r.code, "")
            self.assertEqual(r.message, "")

    def test_single_failure_does_not_abort_others(self):
        v = StubVerifier(reject_ids={"b"})
        q = ItemVerificationQueue(v)
        tid = q.submit([item("a"), item("b"), item("c"), item("d")]).task_id
        result = q.run_next()

        self.assertEqual(q.status(tid), "completed")
        self.assertEqual(v.verified, ["a", "b", "c", "d"])  # b 之后仍继续
        self.assertEqual(result.passed_count, 3)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.failed_item_ids, ["b"])
        failed = result.results[1]
        self.assertFalse(failed.passed)
        self.assertEqual(failed.stage, "verify")
        self.assertEqual(failed.code, "rejected")
        self.assertEqual(failed.message, "proof rejected by verifier")
        # 序号是输入序号
        self.assertEqual([r.index for r in result.results], [0, 1, 2, 3])

    def test_multiple_failures_keep_input_order(self):
        v2 = StubVerifier(reject_ids={"a", "c"})
        q2 = ItemVerificationQueue(v2)
        q2.submit([item("a"), item("b"), item("c")])
        r2 = q2.run_next()
        self.assertEqual(
            [(x.index, x.item_id, x.passed) for x in r2.results],
            [(0, "a", False), (1, "b", True), (2, "c", False)],
        )
        self.assertEqual(r2.failed_item_ids, ["a", "c"])

    def test_completed_snapshot_carries_result(self):
        q = ItemVerificationQueue(StubVerifier(reject_ids={"b"}))
        tid = q.submit([item("a"), item("b")]).task_id
        q.run_next()
        snap = q.get_task(tid)
        self.assertEqual(snap.status, "completed")
        self.assertEqual(snap.completed, 2)
        self.assertEqual(snap.total, 2)
        self.assertIsNotNone(snap.result)
        self.assertEqual(snap.result.failed_item_ids, ["b"])
        # items 与 result.results 同序同值
        self.assertEqual(
            [r.item_id for r in snap.items],
            [r.item_id for r in snap.result.results],
        )

    def test_empty_queue_returns_none(self):
        q = ItemVerificationQueue(StubVerifier())
        self.assertIsNone(q.run_next())

    def test_fifo_order(self):
        q = ItemVerificationQueue(StubVerifier())
        t1 = q.submit([item("a1")]).task_id
        t2 = q.submit([item("b1")]).task_id
        r1 = q.run_next()
        self.assertEqual(r1.task_id, t1)
        self.assertEqual(q.status(t2), "queued")
        r2 = q.run_next()
        self.assertEqual(r2.task_id, t2)


# ================================================================ 进度与未结束

class TestProgress(unittest.TestCase):
    def test_queued_progress(self):
        q = ItemVerificationQueue(StubVerifier())
        tid = q.submit([item("a"), item("b")]).task_id
        prog = q.progress(tid)
        self.assertEqual(prog.status, "queued")
        self.assertEqual((prog.total, prog.completed, prog.remaining), (2, 0, 2))
        snap = q.get_task(tid)
        self.assertIsNone(snap.result)  # 未结束不提前给最终结果
        self.assertEqual(snap.completed, 0)

    def test_processing_progress_is_real_and_no_final_result(self):
        q = ItemVerificationQueue(StubVerifier())
        tid = q.submit([item("a"), item("b"), item("c"), item("d")]).task_id

        seen = []

        def during(p):
            snap = q.get_task(tid)
            seen.append((snap.status, snap.completed, snap.total, snap.result))
            prog = q.progress(tid)
            self.assertEqual(prog.status, "processing")
            # 处理中取不到最终结果
            self.assertIsNone(snap.result)
            return True

        v = q._default_verifiers
        v.verify = during
        q.run_next()

        statuses = [row[0] for row in seen]
        self.assertTrue(all(s == "processing" for s in statuses))
        # 完成数随项递增：0,1,2,3
        self.assertEqual([row[1] for row in seen], [0, 1, 2, 3])
        self.assertTrue(all(row[3] is None for row in seen))
        # 已完成项实时可见、按输入序
        mid = seen[2]
        self.assertEqual(mid[1], 2)

    def test_result_unavailable_before_completion(self):
        q = ItemVerificationQueue(StubVerifier())
        tid = q.submit([item("a")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.result(tid)


# ================================================================ 查询幂等与未知

class TestQueryIdempotency(unittest.TestCase):
    def test_unknown_task_raises_not_found(self):
        q = ItemVerificationQueue(StubVerifier())
        with self.assertRaises(TaskNotFoundError):
            q.get_task("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.progress("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.status("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.result("ghost")
        with self.assertRaises(TaskNotFoundError):
            q.retry("ghost")

    def test_terminal_query_is_idempotent_and_does_not_recall(self):
        v = StubVerifier(reject_ids={"b"})
        q = ItemVerificationQueue(v)
        tid = q.submit([item("a"), item("b"), item("c")]).task_id
        q.run_next()
        after_run = list(v.verified)

        s1 = q.get_task(tid)
        s2 = q.get_task(tid)
        r1 = q.result(tid)
        r2 = q.result(tid)
        # 同一稳定结果对象
        self.assertIs(s1.result, s2.result)
        self.assertIs(r1, r2)
        self.assertIs(s1.result, r1)
        # 不再次调用验证器
        self.assertEqual(v.verified, after_run)
        # 状态不变
        self.assertEqual(q.status(tid), "completed")

    def test_snapshot_to_dict_key_order(self):
        q = ItemVerificationQueue(StubVerifier(reject_ids={"a"}))
        tid = q.submit([item("a"), item("b")]).task_id
        q.run_next()
        payload = q.get_task(tid).to_dict()
        self.assertEqual(
            list(payload.keys()),
            ["task_id", "status", "total", "completed", "remaining", "result", "items"],
        )
        self.assertEqual(
            list(payload["result"].keys()),
            [
                "task_id", "total", "passed_count", "failed_count",
                "results", "failed_item_ids",
            ],
        )
        self.assertEqual(
            list(payload["result"]["results"][0].keys()),
            ["index", "item_id", "passed", "stage", "code", "message"],
        )
        self.assertEqual(payload["result"]["failed_item_ids"], ["a"])


# ================================================================ 状态冲突

class TestStateConflicts(unittest.TestCase):
    def _completed(self, q, ids):
        tid = q.submit([item(i) for i in ids]).task_id
        q.run_next()
        return tid

    def test_no_duplicate_consumption(self):
        v = StubVerifier()
        q = ItemVerificationQueue(v)
        self._completed(q, ["a", "b"])
        # 终态任务不会被再次消费
        self.assertIsNone(q.run_next())
        self.assertEqual(v.verified, ["a", "b"])

    def test_retry_non_failed_conflicts(self):
        q = ItemVerificationQueue(StubVerifier())
        queued = q.submit([item("a")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.retry(queued)  # queued 不能 retry

        q.run_next()
        with self.assertRaises(TaskStateConflictError):
            q.retry(queued)  # completed（终态后再次入队）冲突

        cid = q.submit([item("c")]).task_id
        q.cancel(cid)
        with self.assertRaises(TaskStateConflictError):
            q.retry(cid)  # cancelled 冲突

    def test_cancel_rules(self):
        q = ItemVerificationQueue(StubVerifier())
        tid = q.submit([item("a"), item("b")]).task_id
        self.assertTrue(q.cancel(tid))
        self.assertEqual(q.status(tid), "cancelled")
        # 已取消的不被执行
        self.assertIsNone(q.run_next())
        # 重复取消冲突
        with self.assertRaises(TaskStateConflictError):
            q.cancel(tid)

        done = self._completed(q, ["z"])
        with self.assertRaises(TaskStateConflictError):
            q.cancel(done)  # completed 冲突

    def test_cancel_processing_conflicts(self):
        q = ItemVerificationQueue(StubVerifier())
        tid = q.submit([item("a")]).task_id

        def during(p):
            with self.assertRaises(TaskStateConflictError):
                q.cancel(tid)
            return True

        q._default_verifiers.verify = during
        q.run_next()
        self.assertEqual(q.status(tid), "completed")

    def test_result_on_cancelled_conflicts(self):
        q = ItemVerificationQueue(StubVerifier())
        tid = q.submit([item("a")]).task_id
        q.cancel(tid)
        with self.assertRaises(TaskStateConflictError):
            q.result(tid)


# ================================================================ 基础设施故障

class TestInfrastructure(unittest.TestCase):
    def test_verifier_exception_fails_task_and_retains_prior_items(self):
        v = StubVerifier(error_ids={"c"})
        q = ItemVerificationQueue(v)
        tid = q.submit(
            [item("a"), item("b"), item("c"), item("d")]
        ).task_id

        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        exc = ctx.exception
        self.assertEqual(exc.stage, INFRA_STAGE_VERIFY)
        self.assertEqual(exc.code, INFRA_CODE_EXECUTION)
        self.assertEqual(exc.task_id, tid)
        self.assertEqual(exc.index, 2)
        self.assertEqual(exc.completed, 2)
        self.assertEqual(exc.total, 4)

        # 任务 failed，保留此前已完成项 a、b（含定位）
        self.assertEqual(q.status(tid), "failed")
        snap = q.get_task(tid)
        self.assertEqual(snap.status, "failed")
        self.assertEqual(snap.completed, 2)
        self.assertIsNone(snap.result)  # 未完成不给最终聚合结果
        self.assertEqual([r.item_id for r in snap.items], ["a", "b"])
        self.assertTrue(all(r.passed for r in snap.items))
        # failed 取不到最终结果
        with self.assertRaises(TaskStateConflictError):
            q.result(tid)

    def test_retry_resumes_without_reverifying_completed(self):
        v = FlakyOnceVerifier("c", fail_times=1)
        q = ItemVerificationQueue(v)
        tid = q.submit(
            [item("a"), item("b"), item("c"), item("d")]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        self.assertEqual(v.calls, ["a", "b", "c"])

        receipt = q.retry(tid)
        self.assertEqual(receipt.task_id, tid)  # 稳定标识不变
        self.assertEqual(receipt.status, "queued")
        self.assertEqual(q.status(tid), "queued")

        result = q.run_next()
        # a、b 不重复验证；从 c 续跑
        self.assertEqual(v.calls, ["a", "b", "c", "c", "d"])
        self.assertEqual(result.passed_count, 4)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(q.status(tid), "completed")
        # 终态结果稳定
        self.assertIs(q.result(tid), result)

    def test_retry_verifier_override(self):
        # 提交时没有对应验证器 → 未知 protocol 基础设施故障
        q = ItemVerificationQueue()
        tid = q.submit([item("a", material(proto="mystery"))]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.code, INFRA_CODE_UNSUPPORTED)

        class MysteryVerifier(ZKVerifier):
            protocol = "mystery"

            def verify(self, p):
                return False  # 业务拒绝，不是故障

        q.retry(tid, verifiers=MysteryVerifier())
        result = q.run_next()
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.results[0].code, "rejected")

    def test_non_bool_return_is_contract_infra(self):
        q = ItemVerificationQueue(NonBoolVerifier())
        tid = q.submit([item("a"), item("b")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.code, INFRA_CODE_CONTRACT)
        self.assertEqual(ctx.exception.stage, INFRA_STAGE_VERIFY)
        self.assertEqual(ctx.exception.completed, 0)
        self.assertEqual(q.status(tid), "failed")

    def test_read_material_failure(self):
        class BadReadQueue(ItemVerificationQueue):
            def _load_proof(self, task, index, item):
                if item["item_id"] == "b":
                    raise ValueError("cannot deserialize")
                return super()._load_proof(task, index, item)

        q = BadReadQueue(StubVerifier())
        tid = q.submit([item("a"), item("b"), item("c")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.stage, INFRA_STAGE_READ)
        self.assertEqual(ctx.exception.code, INFRA_CODE_MATERIAL)
        self.assertEqual(ctx.exception.index, 1)
        snap = q.get_task(tid)
        self.assertEqual([r.item_id for r in snap.items], ["a"])
        q.retry(tid)  # 接缝仍坏则再次 failed；这里验证可重试即可
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()

    def test_save_item_failure_retains_and_resumes(self):
        class SaveOnceFailQueue(ItemVerificationQueue):
            def __init__(self, *a, **kw):
                super().__init__(*a, **kw)
                self.saved = []

            def _persist_item_progress(self, task, index, item_result):
                if index == 1:
                    raise OSError("disk full")
                self.saved.append(index)

        q = SaveOnceFailQueue(StubVerifier())
        tid = q.submit([item("a"), item("b"), item("c")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.stage, INFRA_STAGE_SAVE)
        self.assertEqual(ctx.exception.code, INFRA_CODE_SAVE)
        # 结论已在内存保留（a 已保存，b 已验证但落盘失败也保留在内存）
        snap = q.get_task(tid)
        self.assertEqual(snap.completed, 2)
        self.assertEqual([r.item_id for r in snap.items], ["a", "b"])

    def test_save_final_failure(self):
        class FinalSaveFailQueue(ItemVerificationQueue):
            def _persist_final_result(self, task, result):
                raise OSError("commit failed")

        v = StubVerifier()
        q = FinalSaveFailQueue(v)
        tid = q.submit([item("a"), item("b")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.stage, INFRA_STAGE_SAVE)
        # 全部项已验证保留
        snap = q.get_task(tid)
        self.assertEqual(snap.completed, 2)
        self.assertEqual([r.item_id for r in snap.items], ["a", "b"])
        # 续跑时跳过全部已完成项，直接再次进入最终保存
        q.retry(tid)
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        self.assertEqual(v.verified, ["a", "b"])  # 未重复验证

    def test_failed_task_blocks_behind_on_next_run(self):
        v = StubVerifier(error_ids={"a2"})
        q = ItemVerificationQueue(v)
        t1 = q.submit([item("a1"), item("a2")]).task_id
        t2 = q.submit([item("b1")]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        self.assertEqual(q.status(t1), "failed")
        # 下一个 queued 任务可以照常执行
        r2 = q.run_next()
        self.assertEqual(r2.task_id, t2)


# ================================================================ 脱敏

class TestSanitization(unittest.TestCase):
    def test_no_material_or_stack_in_locations(self):
        secret_inputs = ["SECRET-INPUT-zzz"]
        secret_body = {"bytes": "SECRET-PROOF-zzz"}

        class LeakyVerifier(ZKVerifier):
            protocol = "groth16"

            def verify(self, p):
                raise RuntimeError("boom SECRET-PROOF-zzz at /internal/verifier.py")

        q = ItemVerificationQueue(LeakyVerifier())
        tid = q.submit(
            [
                item("a", material(inputs=secret_inputs, body=secret_body)),
                item("b", material(inputs=secret_inputs, body=secret_body)),
            ]
        ).task_id
        try:
            q.run_next()
            self.fail("expected infra error")
        except VerificationInfrastructureError as exc:
            text = str(exc)
            self.assertNotIn("SECRET-PROOF-zzz", text)
            self.assertNotIn("SECRET-INPUT-zzz", text)
            self.assertNotIn("/internal/", text)
            self.assertNotIn("Traceback", text)
            # 只保留异常类型名
            self.assertIn("RuntimeError", text)
            # 切断异常因果链：__cause__/__context__ 不携带原始异常，
            # 因而 traceback 中不含内部消息或调用栈
            self.assertIsNone(exc.__cause__)
            import traceback
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            self.assertNotIn("SECRET-PROOF-zzz", tb)
            self.assertNotIn("/internal/", tb)

        snap_text = str(q.get_task(tid).to_dict())
        self.assertNotIn("SECRET-PROOF-zzz", snap_text)
        self.assertNotIn("SECRET-INPUT-zzz", snap_text)

    def test_rejected_message_is_generic(self):
        q = ItemVerificationQueue(StubVerifier(reject_ids={"a"}))
        q.submit([item("a", material(body={"secret": "S"}))])
        result = q.run_next()
        text = str(result.to_dict())
        self.assertNotIn("secret", text)
        self.assertEqual(result.results[0].message, "proof rejected by verifier")


# ================================================================ 与既有流程隔离

class TestIndependence(unittest.TestCase):
    def test_item_flow_does_not_touch_aggregation(self):
        # 逐项验证只调用 verify：一个不会聚合的验证器也能跑任务流程
        class VerifyOnly(ZKVerifier):
            protocol = "groth16"

            def verify(self, p):
                return True

        q = ItemVerificationQueue(VerifyOnly())
        q.submit([item("a"), item("b")])
        result = q.run_next()
        self.assertEqual((result.passed_count, result.failed_count), (2, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
