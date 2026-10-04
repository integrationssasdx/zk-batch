"""VerificationTaskQueue.retry_outcome（复核只读对账）测试。

覆盖：
* completed/failed 源任务与 completed/failed 复核任务的四种组合：
  before_status 只取 failed/unresolved，after_status 只取
  passed/failed/unresolved，outcome 唯一（recovered/still_failed/
  still_unresolved），定位字段（stage/code/message）沿用对应任务；
* retried_indexes 与明细严格沿用 retry_failed 的入选范围，按根任务
  输入顺序排列，index 保留根任务零起下标，未入选项不进明细；
* recovered/still_failed/unresolved 三类 item_ids 按 outcome 与根任务
  顺序归类，数量字段自洽；
* RetryOutcomeReport/RetryOutcomeItem 的 to_dict 固定键序与内容；
* 只读：不调用验证器、不创建任务、不改变状态/结果，重复查询一致；
* 脱敏：不含 proof、public_inputs、调用栈；
* TaskNotFoundError / TaskStateConflictError / TaskLineageMismatchError
  三者互不替代（存在性 -> 终态 -> 直接血缘）。

直接运行：python tests/test_zk_batch_retry_outcome.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    RetryOutcomeItem,
    RetryOutcomeReport,
    TaskLineageMismatchError,
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


REPORT_KEYS = [
    "source_task_id", "retry_task_id", "source_status", "retry_status",
    "retried_indexes", "items",
    "recovered_item_ids", "still_failed_item_ids", "unresolved_item_ids",
    "recovered_count", "still_failed_count", "unresolved_count",
]

ITEM_KEYS = [
    "index", "item_id",
    "before_status", "before_stage", "before_code", "before_message",
    "after_status", "after_stage", "after_code", "after_message",
    "outcome",
]


def _triples(report):
    return [
        (x.index, x.item_id, x.outcome) for x in report.items
    ]


# =============================================== completed 源 / completed 复核

class TestOutcomeCompleted(unittest.TestCase):
    def test_mixed_recovered_and_still_failed(self):
        v = StubVerifier(reject_ids={"p2", "p4"})
        q = VerificationTaskQueue(v)
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier(reject_ids={"p4"}))
        q.run_task(receipt.task_id)

        report = q.retry_outcome(tid, receipt.task_id)
        self.assertIsInstance(report, RetryOutcomeReport)
        self.assertEqual(report.source_task_id, tid)
        self.assertEqual(report.retry_task_id, receipt.task_id)
        self.assertEqual(report.source_status, "completed")
        self.assertEqual(report.retry_status, "completed")
        # 范围严格沿用 retry_failed：只含两个失败项，根下标与顺序一致
        self.assertEqual(report.retried_indexes, [1, 3])
        self.assertEqual(_triples(report), [
            (1, "p2", "recovered"),
            (3, "p4", "still_failed"),
        ])
        self.assertEqual(report.recovered_item_ids, ["p2"])
        self.assertEqual(report.still_failed_item_ids, ["p4"])
        self.assertEqual(report.unresolved_item_ids, [])
        self.assertEqual(
            (report.recovered_count,
             report.still_failed_count,
             report.unresolved_count),
            (1, 1, 0),
        )

        good, bad = report.items
        # recovered：before 沿用源失败定位，after 通过、定位留空
        self.assertEqual(good.before_status, "failed")
        self.assertEqual(good.before_stage, "verify")
        self.assertEqual(good.before_code, "rejected")
        self.assertEqual(good.before_message, "proof rejected by verifier")
        self.assertEqual(good.after_status, "passed")
        self.assertEqual(good.after_stage, "")
        self.assertEqual(good.after_code, "")
        self.assertEqual(good.after_message, "")
        self.assertEqual(good.outcome, "recovered")
        # still_failed：前后均为 verify/rejected 定位
        self.assertEqual(bad.before_status, "failed")
        self.assertEqual(bad.after_status, "failed")
        self.assertEqual(bad.after_stage, "verify")
        self.assertEqual(bad.after_code, "rejected")
        self.assertEqual(bad.after_message, "proof rejected by verifier")
        self.assertEqual(bad.outcome, "still_failed")

    def test_all_recovered(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2", "p4"}))
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        q.run_task(receipt.task_id)
        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(
            [x.outcome for x in report.items], ["recovered", "recovered"]
        )
        self.assertEqual(report.recovered_item_ids, ["p2", "p4"])
        self.assertEqual(report.still_failed_item_ids, [])
        self.assertEqual(report.unresolved_item_ids, [])
        self.assertEqual(
            (report.recovered_count,
             report.still_failed_count,
             report.unresolved_count),
            (2, 0, 0),
        )

    def test_all_still_failed(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid)  # 继承源验证器，p2 仍被拒
        q.run_task(receipt.task_id)
        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(report.recovered_item_ids, [])
        self.assertEqual(report.still_failed_item_ids, ["p2"])
        self.assertEqual(report.unresolved_item_ids, [])
        self.assertEqual(
            (report.recovered_count,
             report.still_failed_count,
             report.unresolved_count),
            (0, 1, 0),
        )

    def test_completed_retry_fails_midway(self):
        # completed 源只含失败项；复核在第二个入选项上基础设施失败：
        # 第一项仍 recovered，错误项与其后项为 still_unresolved。
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2", "p4"}))
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        q.run_next()
        receipt = q.retry_failed(
            tid, verifiers=StubVerifier(error_ids={"p4"})
        )
        with self.assertRaises(VerificationInfrastructureError):
            q.run_task(receipt.task_id)
        self.assertEqual(q.status(receipt.task_id), "failed")

        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(report.retry_status, "failed")
        self.assertEqual(_triples(report), [
            (1, "p2", "recovered"),
            (3, "p4", "still_unresolved"),
        ])
        self.assertEqual(report.recovered_item_ids, ["p2"])
        self.assertEqual(report.still_failed_item_ids, [])
        self.assertEqual(report.unresolved_item_ids, ["p4"])
        self.assertEqual(
            (report.recovered_count,
             report.still_failed_count,
             report.unresolved_count),
            (1, 0, 1),
        )
        unresolved = report.items[1]
        self.assertEqual(unresolved.before_status, "failed")
        self.assertEqual(unresolved.after_status, "unresolved")
        self.assertEqual(unresolved.after_stage, "verify")
        self.assertEqual(unresolved.after_code, "verifier_fault")
        self.assertIn("verifier execution failed", unresolved.after_message)


# =================================================== failed 源（错误项+尾部）

class TestOutcomeFailedSource(unittest.TestCase):
    def _failed_source(self, retry_v):
        # p2 已有失败结果，p4 基础设施错误，p5 尚无结果
        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        )
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4"), item("p5")]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError) as ctx:
            q.run_next()
        self.assertEqual(ctx.exception.index, 3)
        receipt = q.retry_failed(tid, verifiers=retry_v)
        # 入选范围：p2 失败项 + p4 错误项 + p5 无结果项
        self.assertEqual(receipt.retried_indexes, [1, 3, 4])
        return q, tid, receipt

    def test_retry_failed_again(self):
        q, tid, receipt = self._failed_source(
            StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        )
        with self.assertRaises(VerificationInfrastructureError):
            q.run_task(receipt.task_id)

        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(report.source_status, "failed")
        self.assertEqual(report.retry_status, "failed")
        self.assertEqual(report.retried_indexes, [1, 3, 4])
        self.assertEqual(_triples(report), [
            (1, "p2", "still_failed"),
            (3, "p4", "still_unresolved"),
            (4, "p5", "still_unresolved"),
        ])
        self.assertEqual(report.recovered_item_ids, [])
        self.assertEqual(report.still_failed_item_ids, ["p2"])
        self.assertEqual(report.unresolved_item_ids, ["p4", "p5"])
        self.assertEqual(
            (report.recovered_count,
             report.still_failed_count,
             report.unresolved_count),
            (0, 1, 2),
        )

        failed_item, err_item, tail_item = report.items
        # before：p2 沿用源失败定位；p4/p5 源侧无结论，before 定位留空
        self.assertEqual(failed_item.before_status, "failed")
        self.assertEqual(failed_item.before_code, "rejected")
        for it in (err_item, tail_item):
            self.assertEqual(it.before_status, "unresolved")
            self.assertEqual(it.before_stage, "")
            self.assertEqual(it.before_code, "")
            self.assertEqual(it.before_message, "")
            self.assertEqual(it.after_status, "unresolved")
            self.assertEqual(it.after_stage, "verify")
            self.assertEqual(it.after_code, "verifier_fault")
            # 错误项与其后无结果项沿用同一错误定位
            self.assertEqual(it.after_message, err_item.after_message)

    def test_retry_completes_recovers_unresolved(self):
        # 源 failed、复核 completed：源侧 unresolved 项在复核后得出结论
        q, tid, receipt = self._failed_source(
            StubVerifier(reject_ids={"p5"})  # p2/p4 通过，p5 仍失败
        )
        q.run_task(receipt.task_id)
        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(_triples(report), [
            (1, "p2", "recovered"),       # failed -> passed
            (3, "p4", "recovered"),       # unresolved -> passed
            (4, "p5", "still_failed"),    # unresolved -> failed
        ])
        self.assertEqual(report.recovered_item_ids, ["p2", "p4"])
        self.assertEqual(report.still_failed_item_ids, ["p5"])
        self.assertEqual(report.unresolved_item_ids, [])
        # unresolved -> failed 时 before 留空、after 为 rejected 定位
        tail = report.items[2]
        self.assertEqual(tail.before_status, "unresolved")
        self.assertEqual(tail.after_status, "failed")
        self.assertEqual(tail.after_code, "rejected")


# =========================================================== 固定键序/序列化

class TestOutcomeToDict(unittest.TestCase):
    def test_fixed_key_order_and_contents(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        q.run_task(receipt.task_id)
        report = q.retry_outcome(tid, receipt.task_id)

        self.assertIsInstance(report.items[0], RetryOutcomeItem)
        data = report.to_dict()
        self.assertEqual(list(data), REPORT_KEYS)
        self.assertEqual(list(data["items"][0]), ITEM_KEYS)
        self.assertEqual(data, {
            "source_task_id": tid,
            "retry_task_id": receipt.task_id,
            "source_status": "completed",
            "retry_status": "completed",
            "retried_indexes": [1],
            "items": [{
                "index": 1,
                "item_id": "p2",
                "before_status": "failed",
                "before_stage": "verify",
                "before_code": "rejected",
                "before_message": "proof rejected by verifier",
                "after_status": "passed",
                "after_stage": "",
                "after_code": "",
                "after_message": "",
                "outcome": "recovered",
            }],
            "recovered_item_ids": ["p2"],
            "still_failed_item_ids": [],
            "unresolved_item_ids": [],
            "recovered_count": 1,
            "still_failed_count": 0,
            "unresolved_count": 0,
        })

    def test_counts_and_status_domains(self):
        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        )
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4"), item("p5")]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        receipt = q.retry_failed(
            tid, verifiers=StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        )
        with self.assertRaises(VerificationInfrastructureError):
            q.run_task(receipt.task_id)
        data = q.retry_outcome(tid, receipt.task_id).to_dict()

        self.assertEqual(
            len(data["items"]), len(data["retried_indexes"])
        )
        self.assertEqual(
            len(data["items"]),
            data["recovered_count"]
            + data["still_failed_count"]
            + data["unresolved_count"],
        )
        for row in data["items"]:
            self.assertIn(row["before_status"], ("failed", "unresolved"))
            self.assertIn(
                row["after_status"], ("passed", "failed", "unresolved")
            )
            self.assertIn(
                row["outcome"],
                ("recovered", "still_failed", "still_unresolved"),
            )
            # after 状态与 outcome 一一对应
            self.assertEqual(
                row["outcome"],
                {
                    "passed": "recovered",
                    "failed": "still_failed",
                    "unresolved": "still_unresolved",
                }[row["after_status"]],
            )

    def test_no_proof_material_or_traceback(self):
        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        )
        tid = q.submit([
            item("p1"), item("p2"), item("p3"), item("p4")
        ]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        receipt = q.retry_failed(
            tid, verifiers=StubVerifier(error_ids={"p4"})
        )
        with self.assertRaises(VerificationInfrastructureError):
            q.run_task(receipt.task_id)
        text = str(q.retry_outcome(tid, receipt.task_id).to_dict())
        for leaked in ("blob", "public_inputs", "pi", "Traceback",
                       "site-packages", "File \""):
            self.assertNotIn(leaked, text)


# =============================================================== 只读与幂等

class TestReadOnlyIdempotent(unittest.TestCase):
    def test_does_not_call_verifier_or_create_tasks_or_mutate(self):
        source_v = StubVerifier(reject_ids={"p2"})
        retry_v = StubVerifier()
        q = VerificationTaskQueue(source_v)
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=retry_v)
        q.run_task(receipt.task_id)

        source_result = q.result(tid)
        retry_result = q.result(receipt.task_id)
        before_calls = list(retry_v.verified)
        # 队列中已无 queued 任务
        self.assertIsNone(q.run_next())

        report1 = q.retry_outcome(tid, receipt.task_id)
        report2 = q.retry_outcome(tid, receipt.task_id)

        # 不调用验证器
        self.assertEqual(retry_v.verified, before_calls)
        self.assertEqual(source_v.verified, ["p1", "p2"])
        # 不创建任务：仍无 queued 任务
        self.assertIsNone(q.run_next())
        # 不改变状态与结果（同一对象）
        self.assertEqual(q.status(tid), "completed")
        self.assertEqual(q.status(receipt.task_id), "completed")
        self.assertIs(q.result(tid), source_result)
        self.assertIs(q.result(receipt.task_id), retry_result)
        self.assertIsNone(q.task_error(receipt.task_id))
        # 重复查询一致
        self.assertEqual(report1.to_dict(), report2.to_dict())
        self.assertEqual(
            report1.retried_indexes, report2.retried_indexes
        )

    def test_save_fault_full_coverage_has_no_unresolved(self):
        def store(result):
            raise OSError("disk full")

        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}), result_store=store
        )
        tid = q.submit([item("p1"), item("p2"), item("p3")]).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        with self.assertRaises(VerificationInfrastructureError):
            q.run_task(receipt.task_id)
        # 复核结果已覆盖唯一入选项（保存失败 index=None）：无未决项
        report = q.retry_outcome(tid, receipt.task_id)
        self.assertEqual(report.unresolved_item_ids, [])
        self.assertEqual(report.recovered_item_ids, ["p2"])
        self.assertEqual(report.retry_status, "failed")


# =========================================================== 血缘与状态错误

class TestOutcomeErrors(unittest.TestCase):
    def _terminal_pair(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        q.run_task(receipt.task_id)
        return q, tid, receipt.task_id

    def test_unknown_tasks(self):
        q, tid, retry_id = self._terminal_pair()
        with self.assertRaises(TaskNotFoundError):
            q.retry_outcome("ghost", retry_id)
        with self.assertRaises(TaskNotFoundError):
            q.retry_outcome(tid, "ghost")
        with self.assertRaises(TaskNotFoundError):
            q.retry_outcome("ghost-a", "ghost-b")

    def test_non_terminal_tasks_conflict(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        q.run_task(receipt.task_id)

        # 源任务 queued（与终态复核任务配对）
        queued_source = q.submit([item("x")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.retry_outcome(queued_source, receipt.task_id)

        # 复核任务 queued（源任务已终态）
        queued_retry = q.retry_failed(tid)
        with self.assertRaises(TaskStateConflictError):
            q.retry_outcome(tid, queued_retry.task_id)

    def test_processing_source_conflict_within_callback(self):
        q = VerificationTaskQueue(StubVerifier())
        # 同队列内先造一对已终态的源/复核任务作为配对对象
        sid = q.submit(
            [item("s1"), item("s2")],
            verifiers=StubVerifier(reject_ids={"s2"}),
        ).task_id
        q.run_next()
        other_retry = q.retry_failed(sid, verifiers=StubVerifier())
        q.run_task(other_retry.task_id)

        # 新源任务进入 processing，回调内对账应报状态冲突而非血缘错误
        tid = q.submit([item("a")]).task_id

        def reenter(proof):
            self.assertRaisesRegex(
                TaskStateConflictError, "reconciled",
                q.retry_outcome, tid, other_retry.task_id,
            )
            return True

        q._default_verifiers.verify = reenter
        q.run_next()

    def test_lineage_mismatch_variants(self):
        q, tid, retry_id = self._terminal_pair()

        # 无关的终态任务
        other = q.submit([item("z")]).task_id
        q.run_task(other)
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(tid, other)
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(other, retry_id)
        # 反向（复核任务作源、源任务作复核）
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(retry_id, tid)
        # 同一任务
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(tid, tid)

        # 孙任务：自建一条复核后仍失败的链，使二次复核可创建
        cid = q.submit(
            [item("c1"), item("c2")],
            verifiers=StubVerifier(reject_ids={"c2"}),
        ).task_id
        q.run_next()
        child = q.retry_failed(cid)  # 继承验证器，c2 仍失败
        q.run_task(child.task_id)
        grandchild = q.retry_failed(
            child.task_id, verifiers=StubVerifier()
        )
        q.run_task(grandchild.task_id)
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(cid, grandchild.task_id)
        # 直接血缘本身仍可对账
        report = q.retry_outcome(child.task_id, grandchild.task_id)
        self.assertEqual(report.source_task_id, child.task_id)
        self.assertEqual(report.retried_indexes, [1])

    def test_state_conflict_precedence_over_lineage(self):
        q = VerificationTaskQueue(StubVerifier(reject_ids={"p2"}))
        tid = q.submit([item("p1"), item("p2")]).task_id
        q.run_next()
        receipt = q.retry_failed(tid, verifiers=StubVerifier())
        q.run_task(receipt.task_id)
        # 无关的 queued 任务：状态冲突先于血缘不匹配
        stranger = q.submit([item("z")]).task_id
        with self.assertRaises(TaskStateConflictError):
            q.retry_outcome(tid, stranger)
        with self.assertRaises(TaskStateConflictError):
            q.retry_outcome(stranger, receipt.task_id)

    def test_errors_mismatch_messages_are_sanitary(self):
        q, tid, retry_id = self._terminal_pair()
        try:
            q.retry_outcome(tid, "ghost")
        except TaskNotFoundError as exc:
            self.assertNotIn("blob", str(exc))
        try:
            q.retry_outcome(tid, tid)
        except TaskLineageMismatchError as exc:
            msg = str(exc)
            self.assertIn(tid, msg)
            self.assertNotIn("blob", msg)


# ======================================================= 复核的复核（链式）

class TestOutcomeChainedRetry(unittest.TestCase):
    def test_each_hop_uses_its_own_source(self):
        q = VerificationTaskQueue(
            StubVerifier(reject_ids={"p2"}, error_ids={"p4"})
        )
        tid = q.submit(
            [item("p1"), item("p2"), item("p3"), item("p4")]
        ).task_id
        with self.assertRaises(VerificationInfrastructureError):
            q.run_next()
        # 第一轮复核仍在 p4 失败：p2 留失败结果、p4 未决
        first = q.retry_failed(tid)
        with self.assertRaises(VerificationInfrastructureError):
            q.run_task(first.task_id)
        # 用全通过验证器做第二轮复核（first 的直接 retry_failed）
        second = q.retry_failed(first.task_id, verifiers=StubVerifier())
        q.run_task(second.task_id)

        report = q.retry_outcome(first.task_id, second.task_id)
        # 下标始终是根任务下标；p2 由失败恢复、p4 由未决恢复
        self.assertEqual(_triples(report), [
            (1, "p2", "recovered"),
            (3, "p4", "recovered"),
        ])
        self.assertEqual(report.unresolved_item_ids, [])
        # 第二轮不是根任务的直接复核
        with self.assertRaises(TaskLineageMismatchError):
            q.retry_outcome(tid, second.task_id)
        # 第一轮相对根任务的对账仍反映 failed/failed
        first_report = q.retry_outcome(tid, first.task_id)
        self.assertEqual(first_report.retry_status, "failed")
        self.assertEqual(first_report.unresolved_item_ids, ["p4"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
