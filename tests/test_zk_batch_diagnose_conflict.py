"""diagnose_aggregation_conflict / AggregationConflictReport 测试。

覆盖：三层定位（单证 -> 两两组合 -> 整组，每层无冲突才进入下一层）、
源序保持（组内 proof_ids、两两组合 i<j、conflicted_proof_ids 合并去重）、
只调用 aggregate（不调用 verify_aggregate/verify、不判断返回值）、
IncompatibleAggregationError 记冲突而其他异常原样传播、group_id 两种
异常且不调用 aggregate、批次校验优先级与 verify_batch 一致、未知
protocol 与契约违约、只读（不改变 batch、无 proof/public_inputs 泄漏）、
to_dict 固定键序与重复诊断确定性。

直接运行：python tests/test_zk_batch_diagnose_conflict.py
"""

import copy
import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    AggregationConflictReport,
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidAggregationGroupSelectionError,
    InvalidProofError,
    UnknownAggregationGroupError,
    UnsupportedProofSystemError,
    VerifierContractError,
    ZKVerifier,
    diagnose_aggregation_conflict,
)


def proof(pid, proto="groth16", circuit="c1", key="k1", inputs=None,
          body=None):
    return {
        "proof_id": pid,
        "protocol": proto,
        "circuit_id": circuit,
        "aggregation_key": key,
        "public_inputs": inputs if inputs is not None else ["pi", pid],
        "proof": body if body is not None else ("blob", pid),
    }


GROUP_ID = "groth16:c1:k1"


class ConflictRuleVerifier(ZKVerifier):
    """按编排规则决定哪些集合不能聚合的假验证器。

    bad_single：单证即冲突的 proof_id 集合；
    bad_pairs：两两冲突的无序对集合（元素为二元组/集合）；
    bad_whole：整组（>2 个成员）冲突；
    explode_for / explode_exc：某精确集合抛非聚合冲突异常。
    """

    protocol = "groth16"

    def __init__(self, bad_single=(), bad_pairs=(), bad_whole=False,
                 explode_for=None, explode_exc=None):
        self._bad_single = set(bad_single)
        self._bad_pairs = {frozenset(p) for p in bad_pairs}
        self._bad_whole = bad_whole
        self._explode_for = None if explode_for is None else tuple(explode_for)
        self._explode_exc = explode_exc or RuntimeError("aggregator down")
        self.aggregate_calls = []

    def aggregate(self, proofs):
        ids = tuple(p.proof_id for p in proofs)
        self.aggregate_calls.append(list(ids))
        if self._explode_for is not None and ids == self._explode_for:
            raise self._explode_exc
        if len(ids) == 1 and ids[0] in self._bad_single:
            raise IncompatibleAggregationError(f"single {ids[0]} incompatible")
        if len(ids) == 2 and frozenset(ids) in self._bad_pairs:
            raise IncompatibleAggregationError(f"pair {sorted(ids)} incompatible")
        if self._bad_whole and len(ids) > 2:
            raise IncompatibleAggregationError("whole group incompatible")
        return ["AGG", len(ids)]

    def verify_aggregate(self, proofs, aggregated):
        raise AssertionError("diagnose must not call verify_aggregate")

    def verify(self, p):
        raise AssertionError("diagnose must not call verify")


class ReturnValueIgnoredVerifier(ZKVerifier):
    """aggregate 返回非约定值也不影响诊断：返回值被完全忽略。"""

    protocol = "groth16"

    def __init__(self):
        self.calls = 0

    def aggregate(self, proofs):
        self.calls += 1
        # 故意返回 bool / None / 自引用对象等“非聚合证明”值
        return [True, False, None][self.calls % 3]

    def verify_aggregate(self, proofs, aggregated):
        raise AssertionError("must not be called")

    def verify(self, p):
        raise AssertionError("must not be called")


class MissingAggregateVerifier(ZKVerifier):
    protocol = "groth16"

    def verify_aggregate(self, proofs, aggregated):
        return True

    def verify(self, p):
        return True


def batch_of(pids, **kw):
    return {"batch_id": "B", "proofs": [proof(pid, **kw) for pid in pids]}


# ================================================================ 报告形状

class TestReportShape(unittest.TestCase):
    def test_fixed_key_order(self):
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2"]), ConflictRuleVerifier(), GROUP_ID
        )
        self.assertIsInstance(report, AggregationConflictReport)
        self.assertEqual(
            list(report.to_dict().keys()),
            [
                "group_id",
                "proof_ids",
                "status",
                "scope",
                "conflict_sets",
                "conflicted_proof_ids",
            ],
        )

    def test_to_dict_copies_lists(self):
        report = diagnose_aggregation_conflict(
            batch_of(["p1"]), ConflictRuleVerifier(bad_pairs=()), GROUP_ID
        )
        d1 = report.to_dict()
        d2 = report.to_dict()
        self.assertEqual(d1, d2)
        self.assertIsNot(d1["proof_ids"], d2["proof_ids"])
        self.assertIsNot(d1["conflict_sets"], d2["conflict_sets"])

    def test_no_material_fields_leak(self):
        verifier = ConflictRuleVerifier(bad_pairs=[("p1", "p2")])
        text = str(
            diagnose_aggregation_conflict(
                batch_of(["p1", "p2", "p3"]), verifier, GROUP_ID
            ).to_dict()
        )
        self.assertNotIn("blob", text)
        self.assertNotIn("public_inputs", text)


# ================================================================ 无冲突

class TestNoConflict(unittest.TestCase):
    def test_all_three_layers_checked(self):
        verifier = ConflictRuleVerifier()
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3", "p4"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "no_conflict")
        self.assertEqual(report.scope, "none")
        self.assertEqual(report.conflict_sets, [])
        self.assertEqual(report.conflicted_proof_ids, [])
        # 4 单证 + 6 两两 + 1 整组 = 11 次，顺序为层内源序
        self.assertEqual(
            verifier.aggregate_calls,
            [
                ["p1"], ["p2"], ["p3"], ["p4"],
                ["p1", "p2"], ["p1", "p3"], ["p1", "p4"],
                ["p2", "p3"], ["p2", "p4"], ["p3", "p4"],
                ["p1", "p2", "p3", "p4"],
            ],
        )

    def test_single_member_group_checks_single_and_whole(self):
        verifier = ConflictRuleVerifier()
        report = diagnose_aggregation_conflict(
            batch_of(["solo"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "no_conflict")
        self.assertEqual(report.scope, "none")
        self.assertEqual(verifier.aggregate_calls, [["solo"], ["solo"]])

    def test_return_value_is_not_validated(self):
        verifier = ReturnValueIgnoredVerifier()
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2"]), verifier, GROUP_ID
        )
        # 2 单证 + 1 两两 + 1 整组 = 4 次；返回 True/False/None 均不报冲突
        self.assertEqual(verifier.calls, 4)
        self.assertEqual(report.status, "no_conflict")


# ================================================================ 单证层

class TestSingleConflict(unittest.TestCase):
    def test_single_conflict_stops_after_single_layer(self):
        verifier = ConflictRuleVerifier(bad_single={"p3"})
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3", "p4"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "single")
        self.assertEqual(report.scope, "single")
        self.assertEqual(report.conflict_sets, [["p3"]])
        self.assertEqual(report.conflicted_proof_ids, ["p3"])
        # 仍按源序检查完全部单证，但不进入两两/整组层
        self.assertEqual(
            verifier.aggregate_calls,
            [["p1"], ["p2"], ["p3"], ["p4"]],
        )

    def test_multiple_single_conflicts_in_source_order(self):
        verifier = ConflictRuleVerifier(bad_single={"p4", "p1"})
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3", "p4"]), verifier, GROUP_ID
        )
        self.assertEqual(report.conflict_sets, [["p1"], ["p4"]])
        self.assertEqual(report.conflicted_proof_ids, ["p1", "p4"])
        self.assertEqual(
            verifier.aggregate_calls,
            [["p1"], ["p2"], ["p3"], ["p4"]],
        )


# ================================================================ 两两层

class TestPairConflict(unittest.TestCase):
    def test_pairs_checked_in_source_position_order(self):
        verifier = ConflictRuleVerifier()
        diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3"]), verifier, GROUP_ID
        )
        # 3 单证 + (1,2)(1,3)(2,3) + 整组
        self.assertEqual(
            verifier.aggregate_calls,
            [
                ["p1"], ["p2"], ["p3"],
                ["p1", "p2"], ["p1", "p3"], ["p2", "p3"],
                ["p1", "p2", "p3"],
            ],
        )

    def test_pair_set_order_follows_positions_not_lexicographic(self):
        # 批次原序 z -> a -> m；冲突对 {a,z} 在位置 0,1，记录为 [z, a]
        verifier = ConflictRuleVerifier(bad_pairs=[("a", "z")])
        report = diagnose_aggregation_conflict(
            batch_of(["z", "a", "m"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "pair")
        self.assertEqual(report.scope, "pair")
        self.assertEqual(report.conflict_sets, [["z", "a"]])
        self.assertEqual(report.conflicted_proof_ids, ["z", "a"])
        # 两两层完整跑过（含不冲突的对），但整组层不再执行
        self.assertNotIn(["z", "a", "m"], verifier.aggregate_calls)
        self.assertEqual(len(verifier.aggregate_calls), 3 + 3)

    def test_all_pair_conflicts_collected_and_merged_in_proof_order(self):
        verifier = ConflictRuleVerifier(
            bad_pairs=[("p2", "p4"), ("p1", "p2"), ("p4", "p3")]
        )
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3", "p4"]), verifier, GROUP_ID
        )
        # i<j 序：(1,2) (1,3) (1,4) (2,3) (2,4) (3,4)
        self.assertEqual(
            report.conflict_sets,
            [["p1", "p2"], ["p2", "p4"], ["p3", "p4"]],
        )
        # p2、p4 重复出现，conflicted_proof_ids 按 proof_ids 序去重
        self.assertEqual(
            report.conflicted_proof_ids, ["p1", "p2", "p3", "p4"]
        )

    def test_single_layer_clean_is_required_to_reach_pairs(self):
        # 单证层有冲突时，两两组合完全不探测
        verifier = ConflictRuleVerifier(
            bad_single={"p2"}, bad_pairs=[("p1", "p3")]
        )
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "single")
        self.assertEqual(
            verifier.aggregate_calls, [["p1"], ["p2"], ["p3"]]
        )


# ================================================================ 整组层

class TestHigherOrderConflict(unittest.TestCase):
    def test_higher_order_contains_only_whole_group(self):
        verifier = ConflictRuleVerifier(bad_whole=True)
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3", "p4"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "higher_order")
        self.assertEqual(report.scope, "higher_order")
        self.assertEqual(
            report.conflict_sets, [["p1", "p2", "p3", "p4"]]
        )
        self.assertEqual(
            report.conflicted_proof_ids, ["p1", "p2", "p3", "p4"]
        )
        # 完整经过单证层与两两层，最后整组一次
        self.assertEqual(len(verifier.aggregate_calls), 11)
        self.assertEqual(
            verifier.aggregate_calls[-1], ["p1", "p2", "p3", "p4"]
        )

    def test_pair_conflict_blocks_whole_group_check(self):
        verifier = ConflictRuleVerifier(
            bad_whole=True, bad_pairs=[("p1", "p4")]
        )
        report = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3", "p4"]), verifier, GROUP_ID
        )
        self.assertEqual(report.status, "pair")
        self.assertNotIn(
            ["p1", "p2", "p3", "p4"], verifier.aggregate_calls
        )


# ================================================================ group_id 异常

class TestGroupSelection(unittest.TestCase):
    def setUp(self):
        self.batch = batch_of(
            ["a1", "b1"], key="A"
        )
        self.batch["proofs"].append(proof("c1", key="C"))

    def test_invalid_group_id_values(self):
        verifier = ConflictRuleVerifier()
        for bad in ("", None, 1, 0, True, False, ["x"], ("y",), b"g:1:1"):
            with self.subTest(bad=bad):
                with self.assertRaises(
                    InvalidAggregationGroupSelectionError
                ):
                    diagnose_aggregation_conflict(
                        self.batch, verifier, bad
                    )
        # 任何非法值都不触发 aggregate
        self.assertEqual(verifier.aggregate_calls, [])

    def test_unknown_group_does_not_call_aggregate(self):
        verifier = ConflictRuleVerifier()
        with self.assertRaises(UnknownAggregationGroupError) as cm:
            diagnose_aggregation_conflict(
                self.batch, verifier, "groth16:c1:MISSING"
            )
        self.assertEqual(cm.exception.group_id, "groth16:c1:MISSING")
        self.assertEqual(verifier.aggregate_calls, [])

    def test_group_id_uses_composite_group_key(self):
        # 分组键不同的两个组，按完整 group_id 区分、各自只取本组证明
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("a1", key="A"), proof("a2", key="A"), proof("a3", key="A"),
                proof("b1", key="B"), proof("b2", key="B"), proof("b3", key="B"),
            ],
        }
        verifier = ConflictRuleVerifier(bad_whole=True)
        report_a = diagnose_aggregation_conflict(
            batch, verifier, "groth16:c1:A"
        )
        self.assertEqual(report_a.group_id, "groth16:c1:A")
        self.assertEqual(report_a.proof_ids, ["a1", "a2", "a3"])
        self.assertEqual(report_a.status, "higher_order")
        self.assertEqual(
            report_a.conflict_sets, [["a1", "a2", "a3"]]
        )
        report_b = diagnose_aggregation_conflict(
            batch, verifier, "groth16:c1:B"
        )
        self.assertEqual(report_b.proof_ids, ["b1", "b2", "b3"])


# ================================================================ 批次与验证器异常

class TestValidationAndVerifierErrors(unittest.TestCase):
    def test_batch_validation_priority_unchanged(self):
        verifier = ConflictRuleVerifier()
        with self.assertRaises(EmptyBatchError):
            diagnose_aggregation_conflict(
                {"batch_id": "B", "proofs": []}, verifier, GROUP_ID
            )
        # 空批次先于 group_id 校验
        with self.assertRaises(EmptyBatchError):
            diagnose_aggregation_conflict(
                {"batch_id": "B", "proofs": []}, verifier, ""
            )
        with self.assertRaises(InvalidProofError):
            diagnose_aggregation_conflict(
                {"batch_id": "B", "proofs": [{"proof_id": "x"}]},
                verifier, GROUP_ID,
            )
        with self.assertRaises(DuplicateProofIdError):
            diagnose_aggregation_conflict(
                batch_of(["dup", "dup"]), verifier, GROUP_ID
            )
        # 批次校验失败不调用 aggregate
        self.assertEqual(verifier.aggregate_calls, [])

    def test_unknown_protocol(self):
        batch = batch_of(["p1"], proto="plonk")
        with self.assertRaises(UnsupportedProofSystemError) as cm:
            diagnose_aggregation_conflict(
                batch, ConflictRuleVerifier(), "plonk:c1:k1"
            )
        self.assertEqual(cm.exception.protocol, "plonk")

    def test_unknown_protocol_does_not_call_aggregate(self):
        verifier = ConflictRuleVerifier()
        with self.assertRaises(UnsupportedProofSystemError):
            diagnose_aggregation_conflict(
                batch_of(["p1"], proto="plonk"),
                verifier, "plonk:c1:k1",
            )
        self.assertEqual(verifier.aggregate_calls, [])

    def test_contract_violation_missing_aggregate(self):
        with self.assertRaises(VerifierContractError):
            diagnose_aggregation_conflict(
                batch_of(["p1"]), MissingAggregateVerifier(), GROUP_ID
            )

    def test_non_incompatible_exception_propagates_as_is(self):
        # 单证层的非聚合异常原样抛出，不包装、不记冲突
        verifier = ConflictRuleVerifier(
            explode_for=["p2"], explode_exc=ValueError("bad bytes")
        )
        with self.assertRaises(ValueError) as cm:
            diagnose_aggregation_conflict(
                batch_of(["p1", "p2", "p3"]), verifier, GROUP_ID
            )
        self.assertEqual(str(cm.exception), "bad bytes")
        # 异常后立即停止：p3 及后续层不再调用
        self.assertEqual(
            verifier.aggregate_calls, [["p1"], ["p2"]]
        )

    def test_non_incompatible_exception_in_pair_layer_propagates(self):
        verifier = ConflictRuleVerifier(
            explode_for=["p1", "p3"], explode_exc=RuntimeError("pair boom")
        )
        with self.assertRaises(RuntimeError):
            diagnose_aggregation_conflict(
                batch_of(["p1", "p2", "p3"]), verifier, GROUP_ID
            )
        # 单证 3 次 + (1,2) + (1,3) 抛错；不再继续
        self.assertEqual(
            verifier.aggregate_calls,
            [["p1"], ["p2"], ["p3"], ["p1", "p2"], ["p1", "p3"]],
        )

    def test_mapping_and_instance_verifier_forms(self):
        batch = batch_of(["p1", "p2"])
        r1 = diagnose_aggregation_conflict(
            batch, ConflictRuleVerifier(), GROUP_ID
        )
        r2 = diagnose_aggregation_conflict(
            batch, {"groth16": ConflictRuleVerifier()}, GROUP_ID
        )
        r3 = diagnose_aggregation_conflict(
            batch, [ConflictRuleVerifier()], GROUP_ID
        )
        self.assertTrue(
            r1.status == r2.status == r3.status == "no_conflict"
        )


# ================================================================ 只读与确定性

class TestReadOnlyAndDeterminism(unittest.TestCase):
    def test_batch_is_not_mutated(self):
        batch = batch_of(["p1", "p2", "p3"])
        snapshot = copy.deepcopy(batch)
        diagnose_aggregation_conflict(
            batch,
            ConflictRuleVerifier(bad_pairs=[("p1", "p3")]),
            GROUP_ID,
        )
        self.assertEqual(batch, snapshot)

    def test_proof_objects_passed_are_the_same_batches_entries(self):
        captured = []

        class Capturing(ConflictRuleVerifier):
            def aggregate(self, proofs):
                captured.extend(proofs)
                return super().aggregate(proofs)

        batch = batch_of(["p1", "p2"])
        diagnose_aggregation_conflict(batch, Capturing(), GROUP_ID)
        # 不复制、不重排：传入的 Proof 是批次证明归一化后的同序对象
        self.assertEqual(
            [p.proof_id for p in captured[:2]], ["p1", "p2"]
        )

    def test_repeated_diagnosis_is_identical(self):
        d1 = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3"]),
            ConflictRuleVerifier(bad_pairs=[("p1", "p3"), ("p2", "p3")]),
            GROUP_ID,
        ).to_dict()
        d2 = diagnose_aggregation_conflict(
            batch_of(["p1", "p2", "p3"]),
            ConflictRuleVerifier(bad_pairs=[("p1", "p3"), ("p2", "p3")]),
            GROUP_ID,
        ).to_dict()
        self.assertEqual(d1, d2)
        self.assertEqual(
            d1,
            {
                "group_id": GROUP_ID,
                "proof_ids": ["p1", "p2", "p3"],
                "status": "pair",
                "scope": "pair",
                "conflict_sets": [["p1", "p3"], ["p2", "p3"]],
                "conflicted_proof_ids": ["p1", "p2", "p3"],
            },
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
