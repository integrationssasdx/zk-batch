"""VerificationTaskQueue.retry_failed（失败项复核）测试。

覆盖：
* completed 源任务：只选 results 中 status=failed 的项，按根任务输入顺序、
  去重，不传已通过项；
* failed 源任务：从 VerificationInfrastructureError.index 对应项起纳入错误
  项及其后尚无结果项，与已有失败项按根任务顺序合并去重；错误无明确项下标
  （result_save 失败）且结果覆盖全部输入时只选失败项；
* 回执 TaskRetrySubmission：source_task_id/task_id/status/summary/
  retried_indexes 与固定 to_dict 键序；重复复核生成不同 task_id、顺序稳定；
* 新任务是普通 queued 任务：沿用执行/进度/结果/错误查询，FIFO，每项只验证
  一次，ItemResult.index 保留根任务下标，复核链上再次失败仍定位最初提交；
* verifiers 只作用于新任务，省略时继承源任务选择，解析/契约异常口径不变；
* 源任务状态、结果对象、错误定位、保存历史不变；
* 三类异常不替代：TaskNotFoundError / TaskStateConflictError /
  NoRetryableItemsError；
* 回执与异常消息不含 proof、public_inputs、调用栈与验证器信息。

直接运行：python tests/test_zk_batch_task_retry.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    ItemResult,
    NoRetryableItemsError,
    TaskNotFoundError,
    TaskStateConflictError,
    VerificationInfrastructureError,
    VerificationTaskQueue,
    ZKVerifier,
)


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
    """按 proof_id 编排通过/拒绝/异常的假验证器。"""

    protocol = "groth16"

    def __init__(self, reject_ids=(), error_ids=()):
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self.verified = []

    def verify(self, p):
        self.verified.append(p.proof_id)
        if p.proof_id in self._errors:
            raise RuntimeError("verifier exploded: " + p.proof_id)
        return p.proof_id not in self._reject


# ================================================================ completed

class TestRetryCompleted(unittest.TestCase):
    def test_only_failed_items_selected_in_root_order(self):
        v = StubVerifier(reject_ids={"z", "m"})
        q = VerificationTaskQueue(v)
        tid = q.submit([item("z"), item("a"), item("m")]).task_id
        source_result = q.run_next()

        receipt = q.retry_failed(tid)
        self.assertEqual(receipt.source_task_id, tid)
        self.assertNotEqual(receipt.task_id, tid)
        self.assertEqual(receipt.status, "queued")
        self.assertEqual(receipt.summary.total, 2)
        self.assertEqual(receipt.summary.item_ids, ["z", "m"])
        # 根任务零起下标，按输入顺序（不排序）
        self.assertEqual(receipt.retried_indexes, [0, 2])
        self.assertEqual(
            receipt.to_dict(),
            {
                "source_task_id": tid,
                "task_id": receipt.task_id,
                "status": "queued",
                "summary": {"total": 2, "item_ids": ["z", "m"]},
                "retried_indexes": [0, 2],
            },
        )
        self.assertEqual(
            list(receipt.to_dict().keys()),
            ["source_task_id", "task_id", "status", "summary",
             "retried_indexes"],
        )
        # 新任务入队但未执行；源结果不受影响
        self.assertEqual(q.status(receipt.task_id), "queued")
        self.assertIs(q.result(tid), source_result)

    def test_rerun_verifies_each_selected_item_once_with_root_index(self):
        v = StubVerifier(reject_ids={"p2"})
        q = VerificationTaskQueue(v)
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        q.run_next()
        new_id = q.retry_failed(tid).task_id

        # 仍被拒：新任务 completed，失败项保留根下标 1
        result = q.run_task(new_id)
        self.assertEqual(result.task_id, new_id)
        self.assertEqual(result.total, 1)  # 总数是入选项数，不是根批总数
        self.assertEqual((result.passed, result.failed), (0, 1))
        self.assertEqual(v.verified,
                         ["p1", "p2", "p3", "p4", "p2"])
        only = result.results[0]
        self.assertEqual(
            (only.index, only.item_id, only.status, only.stage, only.code),
            (1, "p2", "failed", "verify", "rejected"),
        )
        self.assertEqual(result.failed_item_ids, ["p2"])

    def test_passing_override_makes_retry_pass(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        q.run_next()
        new_id = q.retry_failed(tid, verifiers=StubVerifier()).task_id
        result = q.run_task(new_id)
        self.assertEqual((result.total, result.passed, result.failed), (1, 1, 0))
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in result.results],
            [(1, "p2", "passed")],
        )

    def test_all_passed_has_no_retryable_items(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("a"), item("b")]).task_id
        q.run_next()
        with self.assertRaises(NoRetryableItemsError):
            q.retry_failed(tid)


# ================================================================ failed

class TestRetryFailedInfra(unittest.TestCase):
    def test_verifier_fault_selects_error_item_and_unfinished_tail(self):
        q = VerificationTaskQueue()
        # 源任务用会在 p2 爆炸的验证器（提交时逐任务给定）
        broken = StubVerifier(error_ids={"p2"})
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")],
            verifiers=broken,
        ).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        err = ctx.exception
        self.assertEqual((err.index, err.item_id, err.code),
                         (1, "p2", "verifier_fault"))

        receipt = q.retry_failed(tid)
        # 错误项 p2 及其后尚无结果的 p3、p4；已通过的 p1 不选
        self.assertEqual(receipt.summary.item_ids, ["p2", "p3", "p4"])
        self.assertEqual(receipt.retried_indexes, [1, 2, 3])

    def test_failed_selection_runs_with_fixed_verifier(self):
        q = VerificationTaskQueue()
        tid = q.submit(
            [item("p1"), item("p2"), item("p3")],
            verifiers=StubVerifier(error_ids={"p2"}),
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        new_id = q.retry_failed(tid, verifiers=StubVerifier()).task_id
        result = q.run_task(new_id)
        self.assertEqual(result.total, 2)
        self.assertEqual((result.passed, result.failed), (2, 0))
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in result.results],
            [(1, "p2", "passed"), (2, "p3", "passed")],
        )

    def test_prior_failed_items_merged_before_error_in_root_order(self):
        q = VerificationTaskQueue()
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")],
            # p1 被拒（普通失败），p2 验证器爆炸（基础设施错误）
            verifiers=StubVerifier(reject_ids={"p1"}, error_ids={"p2"}),
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        partial = q.result(tid)
        self.assertEqual([r.item_id for r in partial.results], ["p1"])

        receipt = q.retry_failed(tid)
        # 已有失败项 p1 + 错误项 p2 及其后 p3、p4，根任务顺序去重
        self.assertEqual(receipt.summary.item_ids, ["p1", "p2", "p3", "p4"])
        self.assertEqual(receipt.retried_indexes, [0, 1, 2, 3])

    def test_error_at_first_index_selects_all(self):
        q = VerificationTaskQueue()
        tid = q.submit(
            [item("p1"), item("p2")],
            verifiers=StubVerifier(error_ids={"p1"}),
        ).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.index, 0)
        receipt = q.retry_failed(tid)
        self.assertEqual(receipt.retried_indexes, [0, 1])

    def test_unreadable_proof_error_location_persists_through_chain(self):
        broken = {"proof_id": "p2"}  # 执行期读不出
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([
            item("p1"),
            {"item_id": "p2", "proof": broken},
            item("p3"),
        ]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual((ctx.exception.index, ctx.exception.code),
                         (1, "invalid_proof"))

        r1 = q.retry_failed(tid)
        self.assertEqual((r1.summary.item_ids, r1.retried_indexes),
                         (["p2", "p3"], [1, 2]))
        # 材料仍读不出：复核任务在根下标 1 处再次失败，p3 依然没被验证
        with self.assertRaises(VerificationInfrastructureError) as ctx1:
            q.run_task(r1.task_id)
        self.assertEqual(ctx1.exception.index, 1)
        self.assertEqual(ctx1.exception.item_id, "p2")
        self.assertEqual(
            [r.item_id for r in q.result(r1.task_id).results], []
        )

        # 对复核任务再次复核：选择与定位仍锚定最初提交
        r2 = q.retry_failed(r1.task_id)
        self.assertEqual(r2.source_task_id, r1.task_id)
        self.assertEqual((r2.summary.item_ids, r2.retried_indexes),
                         (["p2", "p3"], [1, 2]))
        with self.assertRaises(VerificationInfrastructureError) as ctx2:
            q.run_task(r2.task_id)
        self.assertEqual((ctx2.exception.index, ctx2.exception.item_id),
                         (1, "p2"))

    def test_save_fault_with_full_results_selects_only_failed_items(self):
        def store(result):
            raise OSError("disk full")

        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}), result_store=store
        )
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        err = ctx.exception
        self.assertEqual(err.stage, "result_save")
        self.assertIsNone(err.index)
        self.assertEqual(q.status(tid), "failed")

        # 结果已覆盖全部输入：只选失败项 p2（错误无明确项下标）
        receipt = q.retry_failed(tid)
        self.assertEqual(receipt.summary.item_ids, ["p2"])
        self.assertEqual(receipt.retried_indexes, [1])

    def test_save_fault_with_no_failed_items_is_not_retryable(self):
        def store(result):
            raise OSError("disk full")

        q = VerificationTaskQueue(StubVerifier(), result_store=store)
        tid = q.submit([item("a"), item("b")]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        with self.assertRaises(NoRetryableItemsError):
            q.retry_failed(tid)


# ================================================================ 异常约定

class TestRetryErrors(unittest.TestCase):
    def test_unknown_task(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(TaskNotFoundError):
            q.retry_failed("ghost")

    def test_queued_source_conflicts(self):
        q = VerificationTaskQueue(StubVerifier())
        tid = q.submit([item("a")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.retry_failed(tid)

    def test_processing_source_conflicts(self):
        v = StubVerifier()
        q = VerificationTaskQueue(v)
        tid = q.submit([item("a"), item("b")]).task_id

        def reenter(proof):
            if proof.proof_id == "b":
                with self.assertRaises(TaskStateConflictError):
                    q.retry_failed(tid)
            return True

        v.verify = reenter
        q.run_next()

    def test_three_errors_are_distinct_and_not_substituted(self):
        q = VerificationTaskQueue(StubVerifier())
        with self.assertRaises(TaskNotFoundError):
            q.retry_failed("nope")
        tid = q.submit([item("a")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.retry_failed(tid)
        q.run_next()
        with self.assertRaises(NoRetryableItemsError):
            q.retry_failed(tid)


# ================================================================ 复核任务语义

class TestRetryTaskSemantics(unittest.TestCase):
    def test_repeated_retries_get_distinct_ids_with_stable_order(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit(
            [item("p1"), item("p2"), item("p3")]
        ).task_id
        q.run_next()
        r1 = q.retry_failed(tid)
        r2 = q.retry_failed(tid)
        self.assertNotEqual(r1.task_id, r2.task_id)
        self.assertNotIn(r1.task_id, (tid,))
        self.assertEqual(r1.retried_indexes, r2.retried_indexes)
        self.assertEqual(r1.summary.item_ids, r2.summary.item_ids)
        # 两个独立 queued 任务，先后消费互不影响
        self.assertEqual(q.run_next().task_id, r1.task_id)
        self.assertEqual(q.run_next().task_id, r2.task_id)

    def test_retry_task_respects_fifo_and_normal_state_machine(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        retry_id = q.retry_failed(tid).task_id
        other_id = q.submit([item("x")]).task_id
        # 复核任务先入队，run_next 先消费它
        self.assertEqual(q.run_next().task_id, retry_id)
        self.assertEqual(q.run_next().task_id, other_id)
        # 终态后不可再消费
        with self.assertRaises(TaskStateConflictError):
            q.run_task(retry_id)
        # 未结束取结果冲突（再建一个不消费）
        idle = q.retry_failed(tid).task_id
        with self.assertRaises(TaskStateConflictError):
            q.result(idle)

    def test_progress_and_result_queries_on_retry_task(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        q.run_next()
        new_id = q.retry_failed(tid).task_id
        p = q.progress(new_id)
        self.assertEqual((p.total, p.completed), (1, 0))
        self.assertIsNone(p.result)
        result = q.run_task(new_id)
        p2 = q.progress(new_id)
        self.assertEqual((p2.total, p2.completed), (1, 1))
        self.assertIs(p2.result, result)
        self.assertIs(q.result(new_id), result)  # 幂等

    def test_chain_retry_of_completed_retry_keeps_root_index(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit(
            [item("p1"), item("p2"), item("p3")]
        ).task_id
        q.run_next()
        r1 = q.retry_failed(tid)              # p2 仍被拒 -> completed
        res1 = q.run_task(r1.task_id)
        self.assertEqual(res1.failed_item_ids, ["p2"])
        r2 = q.retry_failed(r1.task_id)
        self.assertEqual(r2.retried_indexes, [1])
        res2 = q.run_task(r2.task_id)
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in res2.results],
            [(1, "p2", "failed")],
        )


# ================================================================ verifiers

class TestRetryVerifiers(unittest.TestCase):
    def test_omitted_verifiers_inherit_source_choice(self):
        # 队列没有默认验证器；源任务提交时给定会拒绝 p2 的验证器。
        q = VerificationTaskQueue()
        tid = q.submit(
            [item("p1"), item("p2")],
            verifiers=StubVerifier(reject_ids={"p2"}),
        ).task_id
        q.run_next()
        new_id = q.retry_failed(tid).task_id  # 省略 -> 继承源选择
        result = q.run_task(new_id)
        # 继承的验证器仍认识 groth16 且仍拒绝 p2（根下标 1）
        self.assertEqual(
            [(r.index, r.item_id, r.status) for r in result.results],
            [(1, "p2", "failed")],
        )

    def test_override_applies_only_to_new_task(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        # 覆盖后新任务通过；源任务再次复核仍可独立进行
        new_id = q.retry_failed(tid, verifiers=StubVerifier()).task_id
        self.assertEqual(q.run_task(new_id).failed, 0)
        again = q.retry_failed(tid)  # 不传覆盖，仍继承源选择
        self.assertEqual(q.run_task(again.task_id).failed, 1)

    def test_bad_verifiers_surface_at_execution_like_submit(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        # 与普通提交同口径：入队不解析，执行期契约错误转为基础设施错误
        new_id = q.retry_failed(tid, verifiers=object()).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_task(new_id)
        self.assertEqual(ctx.exception.code, "verifier_fault")
        self.assertEqual(q.status(new_id), "failed")


# ================================================================ 源任务不变

class TestSourceUnchanged(unittest.TestCase):
    def test_source_status_result_error_and_store_history_unchanged(self):
        saved = []
        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}), result_store=saved.append
        )
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        source_result = q.run_next()
        self.assertEqual(len(saved), 1)

        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        # 复核入队本身不执行、不保存
        self.assertEqual(len(saved), 1)
        self.assertEqual(q.status(tid), "completed")
        self.assertIs(q.result(tid), source_result)
        self.assertIsNone(q.task_error(tid))

        q.run_task(receipt.task_id)
        # 源任务结果对象仍是同一个；保存历史只追加了新任务结果
        self.assertEqual(len(saved), 2)
        self.assertIs(saved[0], source_result)
        self.assertIs(q.result(tid), source_result)
        self.assertEqual(saved[1].task_id, receipt.task_id)

    def test_failed_source_error_object_unchanged(self):
        q = VerificationTaskQueue()
        tid = q.submit(
            [item("p1"), item("p2")],
            verifiers=StubVerifier(error_ids={"p2"}),
        ).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        source_err = ctx.exception
        q.retry_failed(tid, verifiers=StubVerifier())
        self.assertIs(q.task_error(tid), source_err)
        self.assertEqual(q.status(tid), "failed")
        self.assertEqual(
            [r.item_id for r in q.result(tid).results], ["p1"]
        )


# ================================================================ 脱敏

class TestNoLeakage(unittest.TestCase):
    def test_receipts_and_errors_never_contain_material_or_internals(self):
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
        receipt = q.retry_failed(tid)
        blob = str(receipt.to_dict())
        try:
            q.retry_failed("task-999")
        except TaskNotFoundError as exc:
            blob += str(exc.args)
        # completed 全通过场景的 NoRetryableItemsError 文本同样检查
        q2 = VerificationTaskQueue(StubVerifier())
        ok = q2.submit([
            item("x", material("x", inputs=secret_inputs, body=secret_body))
        ]).task_id
        q2.run_next()
        try:
            q2.retry_failed(ok)
        except NoRetryableItemsError as exc:
            blob += str(exc.args)
        self.assertNotIn("SECRET-INPUT-xyz", blob)
        self.assertNotIn("SECRET-PROOF-abc", blob)
        self.assertNotIn("Traceback", blob)
        self.assertNotIn("StubVerifier", blob)


if __name__ == "__main__":
    unittest.main(verbosity=2)
