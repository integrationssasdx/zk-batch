"""VerificationQueue.reverify_groups / group_reverify_outcome 测试。

覆盖：
* 提交回执字段与两清单顺序（selected_group_ids 按输入、
  selected_proof_ids 按输入组序 + 组内源批次原序）；
* 组内全部证明入选（不只失败证明），batch_id/材料/分组键/组内序保留；
* 参数校验（非空列表或元组、元素非空字符串、不重复）与异常优先级；
* UnknownJobError / ResultUnavailableError / InvalidGroupSelectionError /
  UnknownFailedGroupError 的顺序与互不替代；失败不建作业、不改源作业；
* 重复复核不同标识、分组谱系与 reverify_failures 谱系互不替代；
* 新作业走完整详细流水线、可取消、可覆盖验证器、执行错误被移除；
* group_reverify_outcome 的 recovered/still_failed、前后组级与逐证状态、
  输入组序、to_dict 固定键序、脱敏、只读幂等。

直接运行：python tests/test_zk_batch_group_reverify.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    GroupReverifyLineageMismatchError,
    InvalidGroupSelectionError,
    ResultUnavailableError,
    ReverifyLineageMismatchError,
    UnknownFailedGroupError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerificationQueue,
    ZKVerifier,
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


G_K1 = "groth16:c1:k1"
G_K2 = "groth16:c1:k2"
G_K3 = "groth16:c1:k3"
G_GHOST = "groth16:c1:nope"


class StubVerifier(ZKVerifier):
    """可编排行为的假验证器（与既有复核测试同款）。"""

    protocol = "groth16"

    def __init__(
        self,
        aggregate_ok=True,
        reject_ids=(),
        error_ids=(),
        agg_verify=True,
        agg_call_error=False,
    ):
        self._aggregate_ok = aggregate_ok
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._agg_verify = agg_verify
        self._agg_call_error = agg_call_error
        self.calls = {"aggregate": 0, "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        if not self._aggregate_ok:
            from zk_batch import IncompatibleAggregationError

            raise IncompatibleAggregationError("cannot aggregate this group")
        if self._agg_call_error:
            raise RuntimeError("aggregate exploded")
        return ["AGG"]

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        return self._agg_verify

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        if p.proof_id in self._errors:
            raise ValueError("bad proof bytes")
        return p.proof_id not in self._reject


class FixExceptP3(StubVerifier):
    """聚合验证只对含 p3 的组失败（p3 回退后仍异常）。"""

    def __init__(self):
        super().__init__(error_ids={"p3"})

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        return all(p.proof_id != "p3" for p in proofs)


class SourceVerifier(StubVerifier):
    """源作业验证器：k1(含 p2)、k2(含 p3) 聚合失败并回退，k3 直接通过。

    回退后 p2 被拒、p3 验证异常；失败组恰为 k1、k2，k3 的 p5 全过。
    """

    def __init__(self):
        super().__init__(reject_ids={"p2"}, error_ids={"p3"})

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        ids = {p.proof_id for p in proofs}
        return not ({"p2", "p3"} & ids)


class TestReverifyGroupsSubmission(unittest.TestCase):
    def _completed_job(self, q, proofs, batch_id="B"):
        jid = q.enqueue({"batch_id": batch_id, "proofs": proofs})
        return jid, q.run_next()

    def _source(self, verifier=None):
        """k1: p1 通过、p2 被拒；k2: p3 异常、p4 通过；k3: p5 全过。

        批次中组首次出现序为 k1、k2、k3；失败组只有 k1、k2。
        """
        q = VerificationQueue(
            verifier
            or SourceVerifier()
        )
        proofs = [
            proof("p1", key="k1"),
            proof("p2", key="k1"),
            proof("p3", key="k2"),
            proof("p4", key="k2"),
            proof("p5", key="k3"),
        ]
        j, result = self._completed_job(q, proofs)
        self.assertEqual(result.failed, 2)
        return q, j, proofs

    # --------------------------------------------------------- 回执与选取

    def test_submission_fields_and_queued_status(self):
        q, j, _ = self._source()
        sub = q.reverify_groups(j, [G_K2, G_K1])
        self.assertEqual(sub.source_job_id, j)
        self.assertNotEqual(sub.job_id, j)
        self.assertEqual(sub.status, "queued")
        self.assertEqual(q.status(sub.job_id), "queued")
        self.assertEqual(sub.selected_group_ids, [G_K2, G_K1])
        # 组按输入顺序，组内按源批次原序；k2 全组 p3,p4 先于 k1 全组 p1,p2
        self.assertEqual(
            sub.selected_proof_ids, ["p3", "p4", "p1", "p2"]
        )

    def test_to_dict_fixed_key_order(self):
        q, j, _ = self._source()
        payload = q.reverify_groups(j, [G_K1]).to_dict()
        self.assertEqual(
            list(payload),
            [
                "source_job_id",
                "job_id",
                "status",
                "selected_group_ids",
                "selected_proof_ids",
            ],
        )

    def test_tuple_accepted(self):
        q, j, _ = self._source()
        sub = q.reverify_groups(j, (G_K1, G_K2))
        self.assertEqual(sub.selected_group_ids, [G_K1, G_K2])
        self.assertEqual(sub.selected_proof_ids, ["p1", "p2", "p3", "p4"])

    def test_selects_all_group_members_not_only_failures(self):
        q, j, _ = self._source()
        sub = q.reverify_groups(j, [G_K1])
        # p1 在源作业中通过，仍因同组复核而入选
        self.assertEqual(sub.selected_proof_ids, ["p1", "p2"])
        q.run_next()
        group = q.report(sub.job_id).groups[0]
        self.assertEqual(group.group_id, G_K1)
        self.assertEqual(group.proof_ids, ["p1", "p2"])

    def test_group_internal_order_follows_source_batch(self):
        # 组成员在源批次中与其他组交错：相对顺序必须保留
        q = VerificationQueue(
            StubVerifier(agg_verify=False, reject_ids={"p2"})
        )
        proofs = [
            proof("p1", key="k1"),
            proof("p3", key="k2"),
            proof("p2", key="k1"),
        ]
        j, _ = self._completed_job(q, proofs)
        sub = q.reverify_groups(j, [G_K1])
        self.assertEqual(sub.selected_proof_ids, ["p1", "p2"])

    def test_batch_id_materials_and_group_key_preserved(self):
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
        j, _ = self._completed_job(q, proofs, batch_id="BATCH-42")
        sub = q.reverify_groups(j, [G_K1], Capturing())
        result = q.run_next()
        self.assertEqual(result.batch_id, "BATCH-42")
        self.assertEqual(set(seen), {"p1", "p2"})
        self.assertIs(seen["p2"].public_inputs, sentinel_inputs)
        self.assertIs(seen["p2"].proof, sentinel_proof)
        self.assertEqual(seen["p2"].group_id, G_K1)

    # --------------------------------------------------------- 参数校验

    def test_invalid_group_ids_shape(self):
        q, j, _ = self._source()
        for bad in (None, [], (), "groth16:c1:k1", {"a": 1}, 123, {"x"}):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidGroupSelectionError):
                    q.reverify_groups(j, bad)

    def test_invalid_group_id_elements(self):
        q, j, _ = self._source()
        for bad in ([""], [G_K1, ""], [1], [None], [G_K1, G_K1], (G_K2, G_K2)):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidGroupSelectionError):
                    q.reverify_groups(j, bad)

    def test_unknown_failed_group(self):
        q, j, _ = self._source()
        # 批次中存在但未失败的组
        with self.assertRaises(UnknownFailedGroupError):
            q.reverify_groups(j, [G_K3])
        # 批次中根本不存在的组
        with self.assertRaises(UnknownFailedGroupError) as ctx:
            q.reverify_groups(j, [G_GHOST])
        self.assertEqual(ctx.exception.group_id, G_GHOST)
        # 混合：合法失败组 + 非失败组仍拒绝
        with self.assertRaises(UnknownFailedGroupError):
            q.reverify_groups(j, [G_K1, G_K3])

    # ----------------------------------------------------- 异常顺序/原子性

    def test_error_precedence(self):
        q, j, _ = self._source()
        queued = q.enqueue({"batch_id": "B", "proofs": [proof("p9")]})

        # 1) 源不存在优先于一切（即使选择非法、即使组也不存在）
        with self.assertRaises(UnknownJobError):
            q.reverify_groups("ghost", [])
        with self.assertRaises(UnknownJobError):
            q.reverify_groups("ghost", [G_GHOST])
        # 2) 未 completed 优先于选择非法/组不存在
        with self.assertRaises(ResultUnavailableError):
            q.reverify_groups(queued, [])
        with self.assertRaises(ResultUnavailableError):
            q.reverify_groups(queued, [G_GHOST])
        # 3) 选择非法优先于“合法分组不属失败定位”
        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, ["", G_GHOST])
        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, [G_GHOST, G_GHOST])

    def test_failure_creates_no_job_and_leaves_source_untouched(self):
        q, j, _ = self._source()
        before = q.result(j)
        jobs_before = list(q._jobs)  # 内部快照：验证失败不新增作业

        with self.assertRaises(InvalidGroupSelectionError):
            q.reverify_groups(j, [])
        with self.assertRaises(UnknownFailedGroupError):
            q.reverify_groups(j, [G_K3])

        self.assertEqual(list(q._jobs), jobs_before)
        self.assertIsNone(q.run_next())  # 没有留下 queued 作业
        self.assertEqual(q.status(j), "completed")
        self.assertIs(q.result(j), before)

    # ----------------------------------------------------------- 流水线衔接

    def test_new_job_runs_full_pipeline_and_groups_by_source_order(self):
        fixed = StubVerifier()
        q, j, _ = self._source()
        sub = q.reverify_groups(j, [G_K2, G_K1], fixed)
        result = q.run_next()
        # 入选 4 证，按子批次分组首次出现序为 k2、k1
        self.assertEqual(result.failed, 0)
        self.assertEqual(result.passed, 4)
        self.assertEqual(result.aggregate_count, 2)
        report = q.report(sub.job_id)
        self.assertIs(report.result, result)
        self.assertEqual([g.group_id for g in report.groups], [G_K2, G_K1])
        self.assertEqual(report.groups[0].proof_ids, ["p3", "p4"])
        self.assertEqual(report.groups[1].proof_ids, ["p1", "p2"])
        self.assertEqual(fixed.calls["verify"], [])  # 聚合通过、未回退

    def test_new_job_can_cancel(self):
        q, j, _ = self._source()
        sub_id = q.reverify_groups(j, [G_K1]).job_id
        self.assertTrue(q.cancel(sub_id))
        self.assertEqual(q.status(sub_id), "cancelled")
        self.assertIsNone(q.run_next())
        with self.assertRaises(ResultUnavailableError):
            q.result(sub_id)
        self.assertEqual(q.status(j), "completed")

    def test_default_verifiers_used_when_omitted(self):
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        proofs = [proof("p1"), proof("p2")]
        j, _ = self._completed_job(q, proofs)
        sub = q.reverify_groups(j, [G_K1])  # 沿用 flaky -> p2 仍失败
        result = q.run_next()
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.failures[0].proof_id, "p2")
        # 源作业不变
        self.assertEqual(q.result(j).failed, 1)

    def test_failed_new_job_is_removed_and_lineage_cleared(self):
        class OtherProtocol(StubVerifier):
            protocol = "plonk"

        q, j, _ = self._source()
        sub = q.reverify_groups(j, [G_K1], OtherProtocol())
        with self.assertRaises(UnsupportedProofSystemError):
            q.run_next()
        with self.assertRaises(UnknownJobError):
            q.status(sub.job_id)
        # 谱系随之清理，对账报告 UnknownJobError 而非谱系错误
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome(j, sub.job_id)

    # ----------------------------------------------------------- 谱系分离

    def test_repeated_group_reverify_gets_distinct_ids(self):
        q, j, _ = self._source()
        s1 = q.reverify_groups(j, [G_K1])
        q.run_next()
        s2 = q.reverify_groups(j, [G_K1])
        q.run_next()
        self.assertNotEqual(s1.job_id, s2.job_id)
        # 两次都是源作业的直接分组复核，谱系互不覆盖
        self.assertEqual(
            q.group_reverify_outcome(j, s1.job_id).recovered_group_ids, []
        )
        self.assertEqual(
            q.group_reverify_outcome(j, s2.job_id).still_failed_group_ids,
            [G_K1],
        )

    def test_group_lineage_separated_from_failures_lineage(self):
        q, j, _ = self._source()
        # 普通失败复核作业不能用于分组对账
        plain = q.reverify_failures(j)
        q.run_next()
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, plain)
        # 分组复核作业不能用于普通对账
        grouped = q.reverify_groups(j, [G_K1]).job_id
        q.run_next()
        with self.assertRaises(ReverifyLineageMismatchError):
            q.reverify_outcome(j, grouped)

    def test_cross_source_group_lineage_mismatch(self):
        # 同一队列内两个独立源作业：各自的分组复核作业不能交叉对账
        flaky1 = StubVerifier(agg_verify=False, reject_ids={"a2"})
        q = VerificationQueue(flaky1)
        j1, _ = self._completed_job(
            q, [proof("a1"), proof("a2")], batch_id="B1"
        )
        flaky2 = StubVerifier(agg_verify=False, reject_ids={"b2"})
        q._default_verifiers = flaky2  # noqa: SLF001 - 切换队列默认验证器
        j2, _ = self._completed_job(
            q, [proof("b1"), proof("b2")], batch_id="B2"
        )
        s1 = q.reverify_groups(j1, [G_K1])
        q.run_next()
        s2 = q.reverify_groups(j2, [G_K1])
        q.run_next()
        # 双方都 completed，但谱系指向不同源作业 -> 谱系错误而非不存在
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j1, s2.job_id)
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j2, s1.job_id)
        # 各自正向对账正常
        self.assertEqual(
            q.group_reverify_outcome(j1, s1.job_id).still_failed_group_ids,
            [G_K1],
        )
        self.assertEqual(
            q.group_reverify_outcome(j2, s2.job_id).still_failed_group_ids,
            [G_K1],
        )


class TestGroupReverifyOutcome(unittest.TestCase):
    def _completed_job(self, q, proofs, batch_id="B"):
        jid = q.enqueue({"batch_id": batch_id, "proofs": proofs})
        return jid, q.run_next()

    def _source(self, verifier=None):
        """k1: p1 通过、p2 被拒；k2: p3 异常、p4 通过；k3: p5 全过。"""
        q = VerificationQueue(verifier or SourceVerifier())
        proofs = [
            proof("p1", key="k1"),
            proof("p2", key="k1"),
            proof("p3", key="k2"),
            proof("p4", key="k2"),
            proof("p5", key="k3"),
        ]
        j, result = self._completed_job(q, proofs)
        self.assertEqual(result.failed, 2)
        return q, j, proofs

    def _run_group_reverify(self, q, j, group_ids, verifier):
        sub = q.reverify_groups(j, group_ids, verifier)
        q.run_next()
        return sub.job_id

    # ------------------------------------------------------------- 异常约定

    def test_unknown_job(self):
        q, j, _ = self._source()
        rj = self._run_group_reverify(q, j, [G_K1], StubVerifier())
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome("ghost", rj)
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome(j, "ghost")
        with self.assertRaises(UnknownJobError):
            q.group_reverify_outcome("ghost", "ghost")

    def test_not_completed_raises_unavailable(self):
        q, j, _ = self._source()
        sub = q.reverify_groups(j, [G_K1])  # 仍 queued
        with self.assertRaises(ResultUnavailableError):
            q.group_reverify_outcome(j, sub.job_id)
        q.cancel(sub.job_id)
        with self.assertRaises(ResultUnavailableError):
            q.group_reverify_outcome(j, sub.job_id)
        # 源作业未 completed
        q2 = VerificationQueue(StubVerifier(agg_verify=False, reject_ids={"p9"}))
        done = q2.enqueue({"batch_id": "B", "proofs": [proof("p9")]})
        q2.run_next()
        rj = q2.reverify_groups(done, [G_K1]).job_id
        q2.run_next()
        queued = q2.enqueue({"batch_id": "B", "proofs": [proof("p1")]})
        with self.assertRaises(ResultUnavailableError):
            q2.group_reverify_outcome(queued, rj)

    def test_lineage_mismatch_variants(self):
        q, j, _ = self._source()
        # 普通入队作业
        other, _ = self._completed_job(q, [proof("p9", key="k9")])
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, other)
        # 普通 reverify_failures 谱系
        plain = q.reverify_failures(j)
        q.run_next()
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, plain)
        # 参数对调
        rj = self._run_group_reverify(q, j, [G_K1], StubVerifier())
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(rj, j)

    # --------------------------------------------------------- recovered 明细

    def test_recovered_group_before_after_detail(self):
        q, j, _ = self._source()
        rj = self._run_group_reverify(q, j, [G_K1], StubVerifier())

        report = q.group_reverify_outcome(j, rj)
        self.assertEqual(report.source_job_id, j)
        self.assertEqual(report.retry_job_id, rj)
        self.assertEqual(report.source_status, "completed")
        self.assertEqual(report.retry_status, "completed")
        self.assertEqual(report.recovered_group_ids, [G_K1])
        self.assertEqual(report.still_failed_group_ids, [])
        self.assertEqual((report.recovered_count, report.still_failed_count), (1, 0))

        group = report.groups[0]
        self.assertEqual(group.group_id, G_K1)
        self.assertEqual(group.outcome, "recovered")
        self.assertEqual(group.proof_ids, ["p1", "p2"])
        # 源组：聚合调用成功、聚合验证被拒、发生回退
        self.assertEqual(group.before_aggregate_call_status, "succeeded")
        self.assertEqual(group.before_aggregate_verify_status, "rejected")
        self.assertTrue(group.before_fell_back)
        # 复核组：聚合直接通过、未回退
        self.assertEqual(group.after_aggregate_call_status, "succeeded")
        self.assertEqual(group.after_aggregate_verify_status, "passed")
        self.assertFalse(group.after_fell_back)

        p1, p2 = group.proofs
        self.assertEqual(p1.proof_id, "p1")
        self.assertEqual(p1.before_status, "passed")
        self.assertEqual(p1.before_message, "")
        self.assertEqual(p1.after_status, "passed")
        self.assertEqual(p1.after_message, "")
        self.assertEqual(p2.proof_id, "p2")
        self.assertEqual(p2.before_status, "rejected")
        self.assertEqual(p2.before_message, "proof rejected by verifier")
        self.assertEqual(p2.after_status, "passed")
        self.assertEqual(p2.after_message, "")

    def test_still_failed_group_detail(self):
        q, j, _ = self._source()
        # 沿用队列默认的失败验证器复核 k2 -> p3 仍异常
        sub = q.reverify_groups(j, [G_K2])
        q.run_next()

        report = q.group_reverify_outcome(j, sub.job_id)
        self.assertEqual(report.recovered_group_ids, [])
        self.assertEqual(report.still_failed_group_ids, [G_K2])
        self.assertEqual((report.recovered_count, report.still_failed_count), (0, 1))

        group = report.groups[0]
        self.assertEqual(group.outcome, "still_failed")
        self.assertEqual(group.proof_ids, ["p3", "p4"])
        self.assertEqual(group.before_aggregate_call_status, "succeeded")
        self.assertEqual(group.before_aggregate_verify_status, "rejected")
        self.assertTrue(group.before_fell_back)
        self.assertEqual(group.after_aggregate_verify_status, "rejected")
        self.assertTrue(group.after_fell_back)
        p3, p4 = group.proofs
        self.assertEqual(p3.before_status, "error")
        self.assertEqual(p3.before_message, "ValueError: bad proof bytes")
        self.assertEqual(p3.after_status, "error")
        self.assertEqual(p3.after_message, "ValueError: bad proof bytes")
        # 同组通过的证明状态照实给出
        self.assertEqual(p4.before_status, "passed")
        self.assertEqual(p4.after_status, "passed")

    def test_mixed_groups_follow_input_order(self):
        q, j, _ = self._source()
        # k2 修复（全过）、k1 仍失败（FixExceptP3 不含 p2 拒识？见下）
        # FixExceptP3 只对含 p3 的组聚合失败；k1 聚合通过 -> recovered，
        # k2 回退后 p3 异常 -> still_failed。
        rj = self._run_group_reverify(q, j, [G_K1, G_K2], FixExceptP3())
        report = q.group_reverify_outcome(j, rj)
        self.assertEqual([g.group_id for g in report.groups], [G_K1, G_K2])
        self.assertEqual([g.outcome for g in report.groups],
                         ["recovered", "still_failed"])
        self.assertEqual(report.recovered_group_ids, [G_K1])
        self.assertEqual(report.still_failed_group_ids, [G_K2])
        self.assertEqual((report.recovered_count, report.still_failed_count), (1, 1))

        # 提交顺序反过来时 groups 也反过来；归类清单仍按输入顺序
        rj2 = self._run_group_reverify(q, j, [G_K2, G_K1], FixExceptP3())
        report2 = q.group_reverify_outcome(j, rj2)
        self.assertEqual([g.group_id for g in report2.groups], [G_K2, G_K1])
        self.assertEqual(report2.recovered_group_ids, [G_K1])
        self.assertEqual(report2.still_failed_group_ids, [G_K2])

    def test_aggregate_call_error_group_can_recover(self):
        # 源：k1 聚合调用直接炸 -> 组内 not_run、stage=aggregate 的组级失败；
        # 复核换正常验证器 -> 聚合通过，整组 recovered。
        q = VerificationQueue(StubVerifier(agg_call_error=True))
        proofs = [proof("p1", key="k1"), proof("p2", key="k1")]
        j, result = self._completed_job(q, proofs)
        self.assertEqual({f.group_id for f in result.failures}, {G_K1})

        rj = self._run_group_reverify(q, j, [G_K1], StubVerifier())
        report = q.group_reverify_outcome(j, rj)
        group = report.groups[0]
        self.assertEqual(group.outcome, "recovered")
        self.assertEqual(group.before_aggregate_call_status, "error")
        self.assertEqual(group.before_aggregate_verify_status, "not_run")
        self.assertFalse(group.before_fell_back)
        self.assertEqual(group.after_aggregate_call_status, "succeeded")
        self.assertEqual(group.after_aggregate_verify_status, "passed")
        self.assertFalse(group.after_fell_back)
        for item, pid in zip(group.proofs, ("p1", "p2")):
            self.assertEqual(item.proof_id, pid)
            self.assertEqual(item.before_status, "not_run")
            self.assertEqual(item.before_message, "RuntimeError: aggregate exploded")
            self.assertEqual(item.after_status, "passed")
            self.assertEqual(item.after_message, "")

    # ----------------------------------------------------------- 序列化/脱敏

    def test_to_dict_fixed_key_order_and_no_materials(self):
        q, j, _ = self._source()
        rj = self._run_group_reverify(q, j, [G_K1], StubVerifier())
        payload = q.group_reverify_outcome(j, rj).to_dict()
        self.assertEqual(
            list(payload),
            [
                "source_job_id",
                "retry_job_id",
                "source_status",
                "retry_status",
                "groups",
                "recovered_group_ids",
                "still_failed_group_ids",
                "recovered_count",
                "still_failed_count",
            ],
        )
        group = payload["groups"][0]
        self.assertEqual(
            list(group),
            [
                "group_id",
                "outcome",
                "proof_ids",
                "before_aggregate_call_status",
                "before_aggregate_verify_status",
                "before_fell_back",
                "after_aggregate_call_status",
                "after_aggregate_verify_status",
                "after_fell_back",
                "proofs",
            ],
        )
        self.assertEqual(
            list(group["proofs"][0]),
            [
                "proof_id",
                "before_status",
                "before_message",
                "after_status",
                "after_message",
            ],
        )
        # 不暴露证明材料、public_inputs、调用栈或内部字段
        blob = repr(payload)
        self.assertNotIn("public_inputs", blob)
        self.assertNotIn("'proof'", blob)
        self.assertNotIn("Traceback", blob)

    # ------------------------------------------------------------- 只读幂等

    def test_read_only_and_idempotent(self):
        fixed = StubVerifier()
        q, j, _ = self._source()
        rj = self._run_group_reverify(q, j, [G_K1, G_K2], fixed)
        calls = dict(fixed.calls, verify=list(fixed.calls["verify"]))

        first = q.group_reverify_outcome(j, rj)
        second = q.group_reverify_outcome(j, rj)
        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual(fixed.calls["aggregate"], calls["aggregate"])
        self.assertEqual(fixed.calls["verify_aggregate"], calls["verify_aggregate"])
        self.assertEqual(fixed.calls["verify"], calls["verify"])
        self.assertEqual(q.status(j), "completed")
        self.assertEqual(q.status(rj), "completed")
        # 源作业结果与报告对象不变
        source_result = q.result(j)
        source_report = q.report(j)
        q.group_reverify_outcome(j, rj)
        self.assertIs(q.result(j), source_result)
        self.assertIs(q.report(j), source_report)

    def test_chain_group_reverify_lineage(self):
        # 分组复核作业自身仍可再按组复核；谱系以新作业为源
        flaky = StubVerifier(agg_verify=False, reject_ids={"p2"})
        q = VerificationQueue(flaky)
        proofs = [proof("p1"), proof("p2")]
        j, _ = self._completed_job(q, proofs)
        s1 = q.reverify_groups(j, [G_K1])
        q.run_next()  # 仍失败
        s2 = q.reverify_groups(s1.job_id, [G_K1], StubVerifier())
        q.run_next()  # 修复
        # s2 是 s1 的直接分组复核，不是 j 的
        with self.assertRaises(GroupReverifyLineageMismatchError):
            q.group_reverify_outcome(j, s2.job_id)
        report = q.group_reverify_outcome(s1.job_id, s2.job_id)
        self.assertEqual(report.recovered_group_ids, [G_K1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
