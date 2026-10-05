"""VerificationQueue 按聚合组复核测试。

覆盖：
* reverify_groups 的选择/顺序/材料保留/异常优先级/失败无副作用；
* 新作业走详细验证流水线、可 cancel、验证器覆盖与默认继承；
* 重复复核标识不同，按组谱系与 reverify_failures 谱系互不覆盖；
* group_reverify_outcome 的组级/逐证明细、recovered/still_failed 判定、
  数量与输入序、只读幂等、脱敏与固定键序；
* UnknownJobError / ResultUnavailableError /
  GroupReverifyLineageMismatchError 三类异常互不替代，且与
  ReverifyLineageMismatchError 互不替代。

直接运行：python tests/test_zk_batch_group_reverify.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    GroupReverifyLineageMismatchError,
    GroupReverifySubmission,
    IncompatibleAggregationError,
    InvalidGroupSelectionError,
    ResultUnavailableError,
    ReverifyLineageMismatchError,
    UnknownFailedGroupError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerificationQueue,
    ZKVerifier,
)

G1 = "groth16:c1:g1"
G2 = "groth16:c1:g2"
G3 = "groth16:c1:g3"
G4 = "groth16:c1:g4"


def proof(pid, key, inputs=None, body=None):
    return {
        "proof_id": pid,
        "protocol": "groth16",
        "circuit_id": "c1",
        "aggregation_key": key,
        "public_inputs": inputs if inputs is not None else ["pi"],
        "proof": body if body is not None else ("blob", pid),
    }


# 源批次（故意让通过组排第一、失败组交错，以检验源批次序）：
#   g2 整组通过；g1 回退后 a2 被拒；g3 聚合调用报错（单证 not_run）；
#   g4 聚合验证被拒但回退单证全过（组级归责）。
SOURCE_PROOFS = [
    proof("e1", "g2"),
    proof("a1", "g1"),
    proof("b1", "g3"),
    proof("a2", "g1"),
    proof("d1", "g4"),
    proof("b2", "g3"),
    proof("d2", "g4"),
]


class SourceVerifier(ZKVerifier):
    """编排四种组形态的源验证器。"""

    protocol = "groth16"

    def __init__(self):
        self.calls = {"aggregate": 0, "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        if proofs[0].aggregation_key == "g3":
            raise RuntimeError("aggregator down")
        return ["AGG"]

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        return proofs[0].aggregation_key not in ("g1", "g4")

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        return p.proof_id != "a2"


class FixedVerifier(ZKVerifier):
    """全部修复：聚合调用与整组验证均通过。"""

    protocol = "groth16"

    def __init__(self):
        self.calls = {"aggregate": 0, "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        return ["AGG"]

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        return True

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        return True


class MixedRetryVerifier(SourceVerifier):
    """复核时 g1 仍失败（行为同源），其余组修复。"""

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        return proofs[0].aggregation_key != "g1"

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        return p.proof_id != "a2"


class G1StillFailingVerifier(FixedVerifier):
    """聚合调用全部正常；仅 g1 整组验证被拒、a2 单证被拒，其余修复。"""

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        return proofs[0].aggregation_key != "g1"

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        return p.proof_id != "a2"


def completed_source(verifier=None):
    q = VerificationQueue(verifier or SourceVerifier())
    jid = q.enqueue({"batch_id": "B", "proofs": [dict(p) for p in SOURCE_PROOFS]})
    q.run_next()
    return q, jid


# ============================================================ 提交校验

class TestReverifyGroupsValidation(unittest.TestCase):
    def test_unknown_job(self):
        q, _ = completed_source()
        with self.assertRaises(UnknownJobError):
            q.reverify_groups("ghost", [G1])

    def test_not_completed_raises_unavailable(self):
        q = VerificationQueue(SourceVerifier())
        j = q.enqueue({"batch_id": "B", "proofs": [proof("p1", "g1")]})
        with self.assertRaises(ResultUnavailableError):
            q.reverify_groups(j, [G1])  # queued
        q.cancel(j)
        with self.assertRaises(ResultUnavailableError):
            q.reverify_groups(j, [G1])  # cancelled

    def test_invalid_selection_shapes(self):
        q, j = completed_source()
        for bad in (None, "g1", {"g1"}, {"g1": 1}, (g for g in ()), 123):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidGroupSelectionError):
                    q.reverify_groups(j, bad)

    def test_invalid_empty_selection(self):
        q, j = completed_source()
        for bad in ([], ()):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidGroupSelectionError):
                    q.reverify_groups(j, bad)

    def test_invalid_elements(self):
        q, j = completed_source()
        for bad in ([""], ["  " if False else ""], [""], ["g1", ""],
                    [1], [None], [G1, None], [b"g1"]):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidGroupSelectionError):
                    q.reverify_groups(j, bad)

    def test_duplicate_elements(self):
        q, j = completed_source()
        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, [G1, G3, G1])
        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, (G1, G1))

    def test_unknown_failed_group(self):
        q, j = completed_source()
        # 标识合法但源批次里不存在
        with self.assertRaises(UnknownFailedGroupError):
            q.reverify_groups(j, ["groth16:c1:nope"])
        # 存在于源批次但整组通过（g2）、不在失败定位中
        with self.assertRaises(UnknownFailedGroupError):
            q.reverify_groups(j, [G2])
        # 混合：失败组 + 非失败组仍拒绝
        with self.assertRaises(UnknownFailedGroupError):
            q.reverify_groups(j, [G1, G2])

    def test_error_precedence(self):
        q, j = completed_source()
        # 源不存在优先于选择非法
        with self.assertRaises(UnknownJobError):
            q.reverify_groups("ghost", [])
        # 源未 completed 优先于选择非法 / 未知分组
        q2 = VerificationQueue(SourceVerifier())
        queued = q2.enqueue(
            {"batch_id": "B", "proofs": [proof("p1", "g1")]}
        )
        with self.assertRaises(ResultUnavailableError):
            q2.reverify_groups(queued, [])
        with self.assertRaises(ResultUnavailableError):
            q2.reverify_groups(queued, ["nope"])
        # 选择非法优先于未知分组
        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, ["", "groth16:c1:nope"])
        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, [G1, G1, "groth16:c1:nope"])

    def test_failure_creates_no_job_and_leaves_source_untouched(self):
        q, j = completed_source()
        source_result = q.result(j)
        source_report = q.report(j)
        for bad in ([], [""], [G1, G1], ["groth16:c1:nope"], [G2]):
            with self.assertRaises(Exception):
                q.reverify_groups(j, bad)
            # 没有遗留 queued 作业
            self.assertIsNone(q.run_next())
            # 源作业状态、结果、报告均为同一对象
            self.assertEqual(q.status(j), "completed")
            self.assertIs(q.result(j), source_result)
            self.assertIs(q.report(j), source_report)


# ============================================================ 选证与回执

class TestReverifyGroupsSelection(unittest.TestCase):
    def test_submission_fields_and_status(self):
        q, j = completed_source()
        submission = q.reverify_groups(j, [G1], FixedVerifier())
        self.assertIsInstance(submission, GroupReverifySubmission)
        self.assertEqual(submission.source_job_id, j)
        self.assertNotEqual(submission.job_id, j)
        self.assertEqual(submission.status, "queued")
        self.assertEqual(submission.selected_group_ids, [G1])
        self.assertEqual(submission.selected_proof_ids, ["a1", "a2"])
        self.assertEqual(q.status(submission.job_id), "queued")

    def test_selects_whole_groups_in_source_batch_order(self):
        q, j = completed_source()
        # 请求顺序与源批次序相反，且跨组交错：选证仍严格按源批次序
        submission = q.reverify_groups(j, [G4, G3, G1], FixedVerifier())
        # 分组清单按输入
        self.assertEqual(submission.selected_group_ids, [G4, G3, G1])
        # 证明清单按源批次序（a1,b1,a2,d1,b2,d2 中的选中项）
        self.assertEqual(
            submission.selected_proof_ids,
            ["a1", "b1", "a2", "d1", "b2", "d2"],
        )

    def test_tuple_input_accepted(self):
        q, j = completed_source()
        submission = q.reverify_groups(j, (G3,), FixedVerifier())
        self.assertEqual(submission.selected_group_ids, [G3])
        self.assertEqual(submission.selected_proof_ids, ["b1", "b2"])

    def test_new_job_runs_pipeline_with_grouping_and_batch_id(self):
        q, j = completed_source()
        fixed = FixedVerifier()
        submission = q.reverify_groups(j, [G4, G1], fixed)
        result = q.run_next()
        # g1(a1,a2) 与 g4(d1,d2) 各成一组，共 4 证，batch_id 不变
        self.assertEqual(result.batch_id, "B")
        self.assertEqual(result.aggregate_count, 2)
        self.assertEqual(result.passed, 4)
        self.assertEqual(result.failed, 0)
        self.assertEqual(fixed.calls["aggregate"], 2)
        # 复核报告分组按子批次首次出现序：g1 在 g4 前
        report = q.report(submission.job_id)
        self.assertEqual([g.group_id for g in report.groups], [G1, G4])
        self.assertEqual(report.groups[0].proof_ids, ["a1", "a2"])
        self.assertEqual(report.groups[1].proof_ids, ["d1", "d2"])
        self.assertIsNone(q.run_next())

    def test_materials_and_group_keys_preserved_as_is(self):
        seen = {}

        class Capturing(FixedVerifier):
            def aggregate(self, proofs):
                for p in proofs:
                    seen[p.proof_id] = p
                return super().aggregate(proofs)

        sentinel_inputs = [{"x": 1}, [9, 8]]
        sentinel_proof = object()
        q = VerificationQueue(SourceVerifier())
        proofs = [dict(p) for p in SOURCE_PROOFS]
        for p in proofs:
            if p["proof_id"] == "a2":
                p["public_inputs"] = sentinel_inputs
                p["proof"] = sentinel_proof
        j = q.enqueue({"batch_id": "B", "proofs": proofs})
        q.run_next()
        submission = q.reverify_groups(j, [G1], Capturing())
        q.run_next()
        self.assertEqual(set(seen), {"a1", "a2"})
        self.assertEqual(seen["a2"].group_key, ("groth16", "c1", "g1"))
        self.assertIs(seen["a2"].public_inputs, sentinel_inputs)
        self.assertIs(seen["a2"].proof, sentinel_proof)
        # 组内序保留（子批次内从 0 起）
        self.assertEqual([p.index for p in (seen["a1"], seen["a2"])], [0, 1])

    def test_default_verifiers_used_when_omitted(self):
        q, j = completed_source()  # 默认仍是 SourceVerifier
        submission = q.reverify_groups(j, [G1])
        result = q.run_next()
        # g1 行为不变：a2 仍被拒
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.failures[0].proof_id, "a2")

    def test_new_job_can_cancel(self):
        q, j = completed_source()
        submission = q.reverify_groups(j, [G1], FixedVerifier())
        self.assertTrue(q.cancel(submission.job_id))
        self.assertEqual(q.status(submission.job_id), "cancelled")
        self.assertIsNone(q.run_next())
        with self.assertRaises(ResultUnavailableError):
            q.result(submission.job_id)
        # 源作业不受影响
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.result(j).failed, 5)

    def test_source_job_untouched_after_retry_completed(self):
        q, j = completed_source()
        source_result = q.result(j)
        source_report = q.report(j)
        submission = q.reverify_groups(j, [G1, G3, G4], FixedVerifier())
        q.run_next()
        self.assertEqual(q.status(j), "completed")
        self.assertIs(q.result(j), source_result)
        self.assertIs(q.report(j), source_report)
        self.assertEqual(
            sorted(f.proof_id for f in q.result(j).failures),
            ["a2", "b1", "b2", "d1", "d2"],
        )
        self.assertEqual(
            [g.group_id for g in q.report(j).groups], [G2, G1, G3, G4]
        )

    def test_repeated_reverify_gets_distinct_jobs_and_lineage(self):
        q, j = completed_source()
        s1 = q.reverify_groups(j, [G1], FixedVerifier())
        s2 = q.reverify_groups(j, [G1], FixedVerifier())
        self.assertNotEqual(s1.job_id, s2.job_id)
        q.run_next()
        q.run_next()
        # 两次复核互不覆盖，都能与源作业对账
        self.assertEqual(
            q.group_reverify_outcome(j, s1.job_id).recovered_group_ids, [G1]
        )
        self.assertEqual(
            q.group_reverify_outcome(j, s2.job_id).recovered_group_ids, [G1]
        )

    def test_retry_execution_error_removes_job(self):
        q, j = completed_source()

        class Incompatible(FixedVerifier):
            def aggregate(self, proofs):
                raise IncompatibleAggregationError("no")

        submission = q.reverify_groups(j, [G1], Incompatible())
        with self.assertRaises(IncompatibleAggregationError):
            q.run_next()
        with self.assertRaises(UnknownJobError):
            q.status(submission.job_id)
        # 源作业不受影响
        self.assertEqual(q.status(j), "completed")


# ============================================================ 对账：异常

class TestGroupReverifyOutcomeErrors(unittest.TestCase):
    def test_unknown_job(self):
        q, j = completed_source()
        s = q.reverify_groups(j, [G1], FixedVerifier())
        q.run_next()
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome("ghost", s.job_id)
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome(j, "ghost")
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome("ghost", "ghost")

    def test_not_completed_raises_unavailable(self):
        q, j = completed_source()
        s = q.reverify_groups(j, [G1], FixedVerifier())
        with self.assertRaises(ResultUnavailableError):
            q.group_reverify_outcome(j, s.job_id)  # retry queued
        q.cancel(s.job_id)
        with self.assertRaises(ResultUnavailableError):
            q.group_reverify_outcome(j, s.job_id)  # retry cancelled
        # 源作业未 completed：同一队列中的 queued 源 + 已完成子作业
        q3, done = completed_source()
        queued3 = q3.enqueue(
            {"batch_id": "Bx", "proofs": [proof("p1", "g1")]}
        )
        s3 = q3.reverify_groups(done, [G1], FixedVerifier())
        q3.run_next()
        with self.assertRaises(ResultUnavailableError):
            q3.group_reverify_outcome(queued3, s3.job_id)

    def test_lineage_mismatch_plain_and_reversed(self):
        q, j = completed_source()
        # 普通入队的 completed 作业不是按组复核作业
        other = q.enqueue({"batch_id": "B2", "proofs": [proof("p9", "g9")]})
        q.run_next()
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, other)
        s = q.reverify_groups(j, [G1], FixedVerifier())
        q.run_next()
        # 参数对调
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(s.job_id, j)

    def test_two_lineage_kinds_do_not_substitute(self):
        q, j = completed_source()
        # reverify_failures 的子作业不能用于按组对账
        rf = q.reverify_failures(j, FixedVerifier())
        q.run_next()
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, rf)
        # reverify_groups 的子作业不能用于逐证对账
        s = q.reverify_groups(j, [G1], FixedVerifier())
        q.run_next()
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(j, s.job_id)
        # 各自的正向对账都正常（逐证清单按源批次原序）
        self.assertEqual(
            q.reverify_outcome(j, rf).recovered_proof_ids,
            ["b1", "a2", "d1", "b2", "d2"],
        )
        self.assertEqual(
            q.group_reverify_outcome(j, s.job_id).recovered_group_ids, [G1]
        )

    def test_lineage_mismatch_other_source_and_chain(self):
        q, j = completed_source()
        s1 = q.reverify_groups(j, [G1], FixedVerifier())
        q.run_next()
        # 对复核作业再按组复核（g1 在修复后已通过，先制造仍失败的复核）
        s_still = q.reverify_groups(j, [G1], MixedRetryVerifier())
        q.run_next()
        s2 = q.reverify_groups(s_still.job_id, [G1], FixedVerifier())
        q.run_next()
        # s2 是 s_still 的直接按组复核，不是 j 的
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, s2.job_id)
        report = q.group_reverify_outcome(s_still.job_id, s2.job_id)
        self.assertEqual([g.group_id for g in report.groups], [G1])
        # s1 与 s_still 互不为父子
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(s1.job_id, s2.job_id)

    def test_removed_retry_job_becomes_unknown(self):
        q, j = completed_source()

        class Stranger(ZKVerifier):
            protocol = "plonk"

            def aggregate(self, proofs):
                return []

            def verify_aggregate(self, proofs, aggregated):
                return True

            def verify(self, p):
                return True

        s = q.reverify_groups(j, [G1], Stranger())  # 不认识 groth16
        with self.assertRaises(UnsupportedProofSystemError):
            q.run_next()
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome(j, s.job_id)


# ============================================================ 对账：内容

class TestGroupReverifyOutcomeContent(unittest.TestCase):
    def test_statuses_and_input_order_and_counts(self):
        q, j = completed_source()
        # g1 仍失败，g3/g4 修复；请求顺序刻意与源批次序不同
        s = q.reverify_groups(j, [G4, G3, G1], G1StillFailingVerifier())
        q.run_next()
        report = q.group_reverify_outcome(j, s.job_id)
        self.assertEqual(report.source_job_id, j)
        self.assertEqual(report.retry_job_id, s.job_id)
        self.assertEqual(report.source_status, "completed")
        self.assertEqual(report.retry_status, "completed")
        # groups 严格按输入序
        self.assertEqual([g.group_id for g in report.groups], [G4, G3, G1])
        self.assertEqual(report.recovered_group_ids, [G4, G3])
        self.assertEqual(report.still_failed_group_ids, [G1])
        self.assertEqual(report.recovered_count, 2)
        self.assertEqual(report.still_failed_count, 1)

    def test_group_and_proof_details(self):
        q, j = completed_source()
        s = q.reverify_groups(j, [G3, G1], FixedVerifier())
        q.run_next()
        report = q.group_reverify_outcome(j, s.job_id)
        by_id = {g.group_id: g for g in report.groups}
        self.assertEqual([g.group_id for g in report.groups], [G3, G1])

        g3 = by_id[G3]
        self.assertEqual(g3.outcome, "recovered")
        self.assertEqual(g3.proof_ids, ["b1", "b2"])
        # 前：聚合调用报错，未进入聚合验证，未回退
        self.assertEqual(g3.before_aggregate_call_status, "error")
        self.assertEqual(g3.before_aggregate_verify_status, "not_run")
        self.assertFalse(g3.before_fell_back)
        # 后：整组通过
        self.assertEqual(g3.after_aggregate_call_status, "succeeded")
        self.assertEqual(g3.after_aggregate_verify_status, "passed")
        self.assertFalse(g3.after_fell_back)
        self.assertEqual(
            [(d.proof_id, d.before_status, d.before_message,
              d.after_status, d.after_message) for d in g3.proofs],
            [
                ("b1", "not_run", "RuntimeError: aggregator down",
                 "passed", ""),
                ("b2", "not_run", "RuntimeError: aggregator down",
                 "passed", ""),
            ],
        )

        g1 = by_id[G1]
        self.assertEqual(g1.outcome, "recovered")
        self.assertEqual(g1.proof_ids, ["a1", "a2"])
        self.assertEqual(g1.before_aggregate_call_status, "succeeded")
        self.assertEqual(g1.before_aggregate_verify_status, "rejected")
        self.assertTrue(g1.before_fell_back)
        self.assertEqual(g1.after_aggregate_verify_status, "passed")
        self.assertFalse(g1.after_fell_back)
        self.assertEqual(
            [(d.proof_id, d.before_status, d.after_status)
             for d in g1.proofs],
            [("a1", "passed", "passed"), ("a2", "rejected", "passed")],
        )
        self.assertEqual(g1.proofs[1].before_message,
                         "proof rejected by verifier")
        self.assertEqual(g1.proofs[1].after_message, "")

    def test_group_wide_blame_counts_as_still_failed(self):
        # g4：单证全过但整组聚合验证被拒（组级归责）；修复后 recovered，
        # 复核仍归责时 still_failed（即使逐证 status 全为 passed）。
        q, j = completed_source()

        class StillBlameG4(SourceVerifier):
            def verify_aggregate(self, proofs, aggregated):
                self.calls["verify_aggregate"] += 1
                return proofs[0].aggregation_key != "g4"

        s = q.reverify_groups(j, [G4], StillBlameG4())
        q.run_next()
        report = q.group_reverify_outcome(j, s.job_id)
        g4 = report.groups[0]
        self.assertEqual([g.group_id for g in report.groups], [G4])
        self.assertEqual(g4.outcome, "still_failed")
        self.assertEqual(report.still_failed_group_ids, [G4])
        self.assertEqual(report.recovered_group_ids, [])
        self.assertTrue(g4.before_fell_back)
        self.assertTrue(g4.after_fell_back)
        self.assertEqual(g4.after_aggregate_verify_status, "rejected")
        # 逐证前后均 passed：组仍失败来自组级归责
        self.assertEqual(
            [(d.proof_id, d.before_status, d.after_status)
             for d in g4.proofs],
            [("d1", "passed", "passed"), ("d2", "passed", "passed")],
        )

    def test_still_failed_group_details(self):
        q, j = completed_source()
        submission = q.reverify_groups(j, [G1, G3], G1StillFailingVerifier())
        q.run_next()
        report = q.group_reverify_outcome(j, submission.job_id)
        g1 = next(g for g in report.groups if g.group_id == G1)
        self.assertEqual(g1.outcome, "still_failed")
        # 复核后 g1 仍回退、聚合验证仍被拒，a2 仍被拒
        self.assertTrue(g1.after_fell_back)
        self.assertEqual(g1.after_aggregate_verify_status, "rejected")
        self.assertEqual(
            [(d.proof_id, d.after_status) for d in g1.proofs],
            [("a1", "passed"), ("a2", "rejected")],
        )

    def test_to_dict_fixed_key_order_and_no_material(self):
        q, j = completed_source()
        s = q.reverify_groups(j, [G1], FixedVerifier())
        q.run_next()
        payload = q.group_reverify_outcome(j, s.job_id).to_dict()
        self.assertEqual(
            list(payload),
            [
                "source_job_id", "retry_job_id", "source_status",
                "retry_status", "groups", "recovered_group_ids",
                "still_failed_group_ids", "recovered_count",
                "still_failed_count",
            ],
        )
        self.assertEqual(
            list(payload["groups"][0]),
            [
                "group_id", "proof_ids", "outcome",
                "before_aggregate_call_status",
                "before_aggregate_verify_status", "before_fell_back",
                "after_aggregate_call_status",
                "after_aggregate_verify_status", "after_fell_back",
                "proofs",
            ],
        )
        self.assertEqual(
            list(payload["groups"][0]["proofs"][0]),
            ["proof_id", "before_status", "before_message",
             "after_status", "after_message"],
        )
        # 不暴露证明材料或 public_inputs
        self.assertNotIn("proof", payload)
        self.assertNotIn("public_inputs", payload)

    def test_messages_never_contain_proof_or_inputs(self):
        secret_inputs = ["SECRET-INPUT-xyz"]
        secret_proof = {"secret": "SECRET-PROOF-abc"}

        class SecretSource(SourceVerifier):
            def verify(self, p):
                if p.proof_id == "a2":
                    raise RuntimeError("explode")
                return True

        q = VerificationQueue(SecretSource())
        proofs = []
        for pid, key in [("a1", "g1"), ("a2", "g1")]:
            item = proof(pid, key, inputs=secret_inputs, body=secret_proof)
            proofs.append(item)
        j = q.enqueue({"batch_id": "B", "proofs": proofs})
        q.run_next()
        s = q.reverify_groups(j, [G1], FixedVerifier())
        q.run_next()
        text = str(q.group_reverify_outcome(j, s.job_id).to_dict())
        self.assertNotIn("SECRET-INPUT-xyz", text)
        self.assertNotIn("SECRET-PROOF-abc", text)

    def test_read_only_and_idempotent(self):
        fixed = FixedVerifier()
        q = VerificationQueue(SourceVerifier())
        j = q.enqueue(
            {"batch_id": "B", "proofs": [dict(p) for p in SOURCE_PROOFS]}
        )
        q.run_next()
        s = q.reverify_groups(j, [G1, G3, G4], fixed)
        q.run_next()
        calls = dict(fixed.calls, verify=list(fixed.calls["verify"]))

        first = q.group_reverify_outcome(j, s.job_id)
        second = q.group_reverify_outcome(j, s.job_id)
        self.assertEqual(first.to_dict(), second.to_dict())
        # 不再次调用验证器、不改变作业状态
        self.assertEqual(fixed.calls["aggregate"], calls["aggregate"])
        self.assertEqual(
            fixed.calls["verify_aggregate"], calls["verify_aggregate"]
        )
        self.assertEqual(fixed.calls["verify"], calls["verify"])
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.status(s.job_id), "completed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
