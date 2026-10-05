"""VerificationTaskQueue 优先级调度测试。

覆盖：
* priority 校验：submit/retry_failed 只接受 0-100 闭区间整数，
  bool/浮点/字符串/越界均抛 InvalidPriorityError；缺省 0（submit）或
  继承源任务（retry_failed）；submit 沿用既有校验顺序、priority 最后；
  非法值不建任务、不改源任务；
* 调度顺序：run_next 取最高 priority，同值按入队先后；取消与终态任务
  不参与；run_task 只消费指定 queued 任务、不改动其他任务位置；
* 复核任务：省略继承源 priority，显式值只作用新任务且排在既有任务
  之后（同优先级按入队先后）；
* schedule 只读快照：entries 仅含 queued 且按执行顺序，
  queue_position 从 1 起、total 等于条目数，空队列 queued_count=0；
  两层 to_dict 固定键序；重复查询不调用验证器、不消费、不改状态结果；
* priority 不进入 proof/public_inputs/结果/进度/对账等既有输出。

直接运行：python tests/test_zk_batch_task_schedule.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchSizeLimitError,
    DuplicateItemIdError,
    EmptyBatchError,
    InvalidItemIdError,
    InvalidPriorityError,
    InvalidProofFormatError,
    MAX_BATCH_ITEMS,
    MAX_TASK_PRIORITY,
    MIN_TASK_PRIORITY,
    NoRetryableItemsError,
    TaskLineageMismatchError,
    TaskNotFoundError,
    TaskScheduleReport,
    TaskStateConflictError,
    VerificationTaskQueue,
    ZKVerifier,
)
from zk_batch.errors import TaskValidationError  # noqa: E402


def material(pid, proto="groth16", circuit="c1", key="k1", inputs=None,
             body=None):
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
    protocol = "groth16"

    def __init__(self, reject_ids=()):
        self._reject = set(reject_ids)
        self.verified = []

    def verify(self, p):
        self.verified.append(p.proof_id)
        return p.proof_id not in self._reject


def consume_all(q):
    """依次 run_next 至队列空，返回消费的 task_id 列表。"""
    ids = []
    while True:
        result = q.run_next()
        if result is None:
            return ids
        ids.append(result.task_id)


# ================================================================ 取值校验

class TestPriorityValidation(unittest.TestCase):
    def test_boundaries_accepted(self):
        q = VerificationTaskQueue(StubVerifier())
        low = q.submit([item("a")], priority=MIN_TASK_PRIORITY)
        high = q.submit([item("b")], priority=MAX_TASK_PRIORITY)
        self.assertEqual(q.status(low.task_id), "queued")
        self.assertEqual(q.status(high.task_id), "queued")
        # 边界任务确实按优先级排列：high 先于 low 消费
        self.assertEqual(consume_all(q), [high.task_id, low.task_id])

    def test_default_is_zero_fifo(self):
        q = VerificationTaskQueue(StubVerifier())
        t1 = q.submit([item("a")]).task_id
        t2 = q.submit([item("b")], priority=None).task_id
        t3 = q.submit([item("c")], priority=0).task_id
        self.assertEqual(consume_all(q), [t1, t2, t3])

    def test_invalid_values_rejected(self):
        q = VerificationTaskQueue(StubVerifier())
        for bad in (True, False, -1, 101, 1.0, 100.0, "5", [], (), object()):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidPriorityError) as ctx:
                    q.submit([item("a")], priority=bad)
                self.assertIs(ctx.exception.priority, bad)

    def test_invalid_priority_leaves_no_task(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(InvalidPriorityError):
            q.submit([item("a")], priority=101)
        # 非法提交不占任务标识：下一个任务仍是 task-1
        tid = q.submit([item("a")]).task_id
        self.assertEqual(tid, "task-1")
        report = q.schedule()
        self.assertEqual([e.task_id for e in report.entries], [tid])

    def test_priority_checked_last_in_submit(self):
        q = VerificationTaskQueue(StubVerifier())
        # 空批次先于 priority
        with self.assertRaises(EmptyBatchError):
            q.submit([], priority=101)
        # 超上限先于 priority
        with self.assertRaises(BatchSizeLimitError):
            q.submit(
                [item(f"i{k}") for k in range(MAX_BATCH_ITEMS + 1)],
                priority=101,
            )
        # 标识非法先于 priority
        with self.assertRaises(InvalidItemIdError):
            q.submit(
                [{"item_id": "", "proof": material("p")}], priority=101
            )
        # 材料格式非法先于 priority
        with self.assertRaises(InvalidProofFormatError):
            q.submit([{"item_id": "a", "proof": {}}], priority=101)
        # 重复标识先于 priority
        with self.assertRaises(DuplicateItemIdError):
            q.submit([item("a"), item("a")], priority=101)

    def test_priority_error_is_validation_error(self):
        self.assertTrue(issubclass(InvalidPriorityError, TaskValidationError))


# ================================================================ 调度顺序

class TestPriorityOrdering(unittest.TestCase):
    def test_highest_priority_first_tie_fifo(self):
        q = VerificationTaskQueue(StubVerifier())
        t_zero = q.submit([item("z")], priority=0).task_id
        t_fifty_a = q.submit([item("a1")], priority=50).task_id
        t_hundred = q.submit([item("h")], priority=100).task_id
        t_fifty_b = q.submit([item("a2")], priority=50).task_id
        self.assertEqual(
            consume_all(q),
            [t_hundred, t_fifty_a, t_fifty_b, t_zero],
        )

    def test_default_zero_is_fifo(self):
        q = VerificationTaskQueue(StubVerifier())
        t1 = q.submit([item("a")]).task_id
        t2 = q.submit([item("b")]).task_id
        t3 = q.submit([item("c")]).task_id
        self.assertEqual(consume_all(q), [t1, t2, t3])

    def test_cancelled_excluded_remaining_reordered(self):
        q = VerificationTaskQueue(StubVerifier())
        low = q.submit([item("low")], priority=1).task_id
        high_a = q.submit([item("ha")], priority=90).task_id
        high_b = q.submit([item("hb")], priority=90).task_id
        q.cancel(high_a)
        # 余下任务仍按优先级+入队先后：high_b 先，low 后
        self.assertEqual(consume_all(q), [high_b, low])

    def test_terminal_tasks_excluded(self):
        q = VerificationTaskQueue(StubVerifier())
        high = q.submit([item("h")], priority=100).task_id
        low = q.submit([item("l")], priority=1).task_id
        q.run_task(high)  # 显式消费高优先级任务至终态
        self.assertEqual(q.status(high), "completed")
        # run_next 不再捞起终态任务
        self.assertEqual(consume_all(q), [low])

    def test_run_task_ignores_priority_and_keeps_others(self):
        q = VerificationTaskQueue(StubVerifier())
        low = q.submit([item("l")], priority=0).task_id
        mid = q.submit([item("m")], priority=50).task_id
        high = q.submit([item("h")], priority=100).task_id
        # run_task 越过更高优先级直接消费指定任务
        self.assertEqual(q.run_task(low).task_id, low)
        # 其他 queued 任务顺序不变
        self.assertEqual([e.task_id for e in q.schedule().entries],
                         [high, mid])
        self.assertEqual(consume_all(q), [high, mid])

    def test_schedule_after_partial_consumption(self):
        q = VerificationTaskQueue(StubVerifier())
        a = q.submit([item("a")], priority=10).task_id
        h = q.submit([item("b")], priority=99).task_id
        c = q.submit([item("c")], priority=10).task_id
        self.assertEqual(q.run_next().task_id, h)
        # 高优先级消费后，剩余两个同值任务恢复入队先后顺序
        entries = q.schedule().entries
        self.assertEqual([e.task_id for e in entries], [a, c])
        self.assertEqual([e.priority for e in entries], [10, 10])
        self.assertEqual([e.queue_position for e in entries], [1, 2])
        self.assertTrue(all(e.total == 2 for e in entries))


# ================================================================ 复核优先级

class TestRetryPriority(unittest.TestCase):
    def test_retry_inherits_source_priority(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit(
            [item("p1"), item("p2"), item("p3")], priority=40
        ).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        # 继承 40：低于后入队的高优先级普通任务，高于默认 0 任务
        high = q.submit([item("hi")], priority=80).task_id
        low = q.submit([item("lo")], priority=0).task_id
        self.assertEqual(
            consume_all(q), [high, receipt.task_id, low]
        )

    def test_retry_explicit_priority_applies_to_new_only(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")], priority=10).task_id
        q.run_next()
        # 显式 70 的复核任务
        r70 = q.retry_failed(
            tid, verifiers=StubVerifier(), priority=70
        ).task_id
        # 省略 priority 的再次复核继承源任务的 10
        r10 = q.retry_failed(tid, verifiers=StubVerifier()).task_id
        p50 = q.submit([item("m")], priority=50).task_id
        p0 = q.submit([item("l")], priority=0).task_id
        # 新任务各按自身优先级；源任务仍为终态、结果不变
        self.assertEqual(consume_all(q), [r70, p50, r10, p0])
        self.assertEqual(q.result(tid).failed_item_ids, ["p2"])

    def test_retry_invalid_priority_checked_after_state_rules(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        # 未知任务先于 priority
        with self.assertRaises(TaskNotFoundError):
            q.retry_failed("ghost", priority=101)
        tid = q.submit([item("p1"), item("p2")]).task_id
        # queued 源任务：状态冲突先于 priority
        with self.assertRaises(TaskStateConflictError):
            q.retry_failed(tid, priority=101)
        q.run_next()
        # 全部通过的任务无复核项：NoRetryableItemsError 先于 priority
        ok = q.submit([item("x")], verifiers=StubVerifier()).task_id
        q.run_next()
        with self.assertRaises(NoRetryableItemsError):
            q.retry_failed(ok, priority=True)
        # 有可复核项时才校验 priority
        for bad in (True, False, -1, 101, 5.0, "9"):
            with self.assertRaises(InvalidPriorityError):
                q.retry_failed(tid, priority=bad)

    def test_retry_invalid_priority_creates_nothing(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")], priority=30).task_id
        q.run_next()
        before = q.schedule().queued_count
        with self.assertRaises(InvalidPriorityError):
            q.retry_failed(tid, priority=999)
        self.assertEqual(q.schedule().queued_count, before)
        # 未占用任务标识：下一个合法复核任务仍是 task-2，且继承 30
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        self.assertEqual(receipt.task_id, "task-2")
        self.assertEqual(
            [e.priority for e in q.schedule().entries], [30]
        )
        # 源任务状态、结果不变
        self.assertEqual(q.status(tid), "completed")
        self.assertEqual(q.result(tid).failed_item_ids, ["p2"])


# ================================================================ schedule

class TestSchedule(unittest.TestCase):
    def test_empty_queue(self):
        q = VerificationTaskQueue(StubVerifier())
        report = q.schedule()
        self.assertIsInstance(report, TaskScheduleReport)
        self.assertEqual(report.queued_count, 0)
        self.assertEqual(report.entries, [])
        self.assertEqual(report.to_dict(), {"queued_count": 0, "entries": []})

    def test_entries_ordered_positioned_and_total(self):
        q = VerificationTaskQueue(StubVerifier())
        t1 = q.submit([item("a")], priority=0).task_id
        t2 = q.submit([item("b")], priority=50).task_id
        t3 = q.submit([item("c")], priority=50).task_id
        t4 = q.submit([item("d")], priority=100).task_id
        report = q.schedule()
        self.assertEqual(report.queued_count, 4)
        self.assertEqual(
            [(e.task_id, e.priority, e.queue_position, e.total)
             for e in report.entries],
            [(t4, 100, 1, 4),
             (t2, 50, 2, 4),
             (t3, 50, 3, 4),
             (t1, 0, 4, 4)],
        )

    def test_to_dict_fixed_key_order(self):
        q = VerificationTaskQueue(StubVerifier())
        q.submit([item("a")], priority=7)
        payload = q.schedule().to_dict()
        self.assertEqual(list(payload.keys()), ["queued_count", "entries"])
        self.assertEqual(
            list(payload["entries"][0].keys()),
            ["task_id", "priority", "queue_position", "total"],
        )
        self.assertEqual(
            payload["entries"][0],
            {"task_id": "task-1", "priority": 7,
             "queue_position": 1, "total": 1},
        )

    def test_only_queued_tasks_listed(self):
        q = VerificationTaskQueue(StubVerifier())
        done = q.submit([item("done")], priority=100).task_id
        canc = q.submit([item("canc")], priority=90).task_id
        wait = q.submit([item("wait")], priority=10).task_id
        q.run_task(done)
        q.cancel(canc)
        report = q.schedule()
        self.assertEqual(report.queued_count, 1)
        self.assertEqual([e.task_id for e in report.entries], [wait])
        self.assertEqual(report.entries[0].total, 1)

    def test_read_only_and_idempotent(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        q.submit([item("a")], priority=5)
        q.submit([item("b")], priority=9)
        r1 = q.schedule().to_dict()
        r2 = q.schedule().to_dict()
        self.assertEqual(r1, r2)
        # 不调用验证器、不消费、不改状态
        self.assertEqual(v.verified, [])
        for tid in ("task-1", "task-2"):
            self.assertEqual(q.status(tid), "queued")
        # 任务仍可按调度顺序消费
        self.assertEqual(q.run_next().task_id, "task-2")

    def test_no_sensitive_data(self):
        q = VerificationTaskQueue(StubVerifier())
        q.submit([
            item("s", material("s", inputs=["SECRET-IN"],
                               body={"x": "SECRET-PROOF"})),
        ], priority=3)
        text = repr(q.schedule().to_dict())
        self.assertNotIn("SECRET-IN", text)
        self.assertNotIn("SECRET-PROOF", text)
        self.assertNotIn("blob", text)
        self.assertNotIn("public_inputs", text)

    def test_priority_absent_from_existing_outputs(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")], priority=55).task_id
        progress = q.progress(tid).to_dict()
        self.assertNotIn("priority", progress)
        result = q.run_next().to_dict()
        self.assertNotIn("priority", result)
        for row in result["results"]:
            self.assertNotIn("priority", row)
        # 复核与对账输出同样不含 priority
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        self.assertNotIn("priority", receipt.to_dict())
        q.run_task(receipt.task_id)
        outcome = q.retry_outcome(tid, receipt.task_id).to_dict()
        self.assertNotIn("priority", outcome)
        for row in outcome["items"]:
            self.assertNotIn("priority", row)


# ================================================================ 不变式

class TestUnchangedSemantics(unittest.TestCase):
    def test_lineage_and_not_found_unchanged(self):
        from zk_batch import TaskNotFoundError
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p1"}))
        tid = q.submit([item("p1")], priority=10).task_id
        q.run_next()
        receipt = q.retry_failed(tid, priority=20)
        other = q.submit([item("x")], priority=30).task_id
        q.run_next()  # 消费 other（30 > 20）
        with self.assertRaises(TaskNotFoundError):
            q.status("ghost")
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(tid, other)
        q.run_task(receipt.task_id)
        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(report.source_task_id, tid)
        self.assertEqual(report.retry_task_id, receipt.task_id)

    def test_reenter_during_processing_still_conflicts(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        tid = q.submit([item("a")], priority=5).task_id
        q.submit([item("b")], priority=1)

        def reenter(proof):
            with self.assertRaises(TaskStateConflictError):
                q.run_next()
            with self.assertRaises(TaskStateConflictError):
                q.run_task(tid)
            return True

        v.verify = reenter
        q.run_next()


if __name__ == "__main__":
    unittest.main(verbosity=2)
