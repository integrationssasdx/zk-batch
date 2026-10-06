"""diagnose_aggregation_conflict 的测试：逐层聚合冲突定位。

直接运行：python tests/test_zk_batch_diagnose.py
"""

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

GROUP_ID = "groth16:c1:k1"


def proof(pid, proto="groth16", circuit="c1", key="k1", inputs=None, body=None):
    return {
        "proof_id": pid,
        "protocol": proto,
        "circuit_id": circuit,
        "aggregation_key": key,
        "public_inputs": inputs if inputs is not None else ["pi"],
        "proof": body if body is not None else ("blob", pid),
    }


def batch(proofs, batch_id="B"):
    return {"batch_id": batch_id, "proofs": proofs}


class ConflictVerifier(ZKVerifier):
    """按探针组成编排 aggregate 行为的假验证器。

    * ``bad_singles`` 中的 proof_id 单独聚合时抛 IncompatibleAggregationError；
    * ``bad_pairs`` 中的 frozenset 两两聚合时抛 IncompatibleAggregationError；
    * ``bad_group=True`` 时三元及以上（即整组层）抛 IncompatibleAggregationError；
    * ``explode`` 中的 id 元组抛 RuntimeError（非不兼容异常）；
    * ``incompatible_subclass`` 时抛 IncompatibleAggregationError 的子类。
    """

    protocol = "groth16"

    def __init__(
        self,
        bad_singles=(),
        bad_pairs=(),
        bad_group=False,
        explode=(),
        subclass_on=(),
    ):
        self._bad_singles = set(bad_singles)
        self._bad_pairs = {frozenset(pair) for pair in bad_pairs}
        self._bad_group = bad_group
        self._explode = {tuple(key) for key in explode}
        self._subclass_on = {tuple(key) for key in subclass_on}
        # 每次 aggregate 收到的 proof_id 元组，按调用顺序记录
        self.aggregate_calls = []
        self.verify_aggregate_calls = 0
        self.verify_calls = []

    def aggregate(self, proofs):
        ids = tuple(p.proof_id for p in proofs)
        self.aggregate_calls.append(ids)
        if ids in self._explode:
            raise RuntimeError(f"boom at {ids}")
        if ids in self._subclass_on:
            raise _IncompatibleSubtype(f"subclass incompatibility at {ids}")
        if len(ids) == 1 and ids[0] in self._bad_singles:
            raise IncompatibleAggregationError(f"{ids[0]} incompatible alone")
        if len(ids) == 2 and frozenset(ids) in self._bad_pairs:
            raise IncompatibleAggregationError(f"pair {ids} incompatible")
        if len(ids) >= 3 and self._bad_group:
            raise IncompatibleAggregationError("whole group incompatible")
        return ["AGG", ids]

    def verify_aggregate(self, proofs, aggregated):
        self.verify_aggregate_calls += 1
        return True

    def verify(self, p):
        self.verify_calls.append(p.proof_id)
        return True


class _IncompatibleSubtype(IncompatibleAggregationError):
    """IncompatibleAggregationError 的子类，也应被诊断捕获。"""


class IncompleteVerifier(ZKVerifier):
    protocol = "groth16"
    # aggregate 未实现


# ================================================================ 无冲突

class TestNoConflict(unittest.TestCase):
    def test_no_conflict_report(self):
        v = ConflictVerifier()
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertIsInstance(report, AggregationConflictReport)
        self.assertEqual(report.group_id, GROUP_ID)
        self.assertEqual(report.proof_ids, ["p1", "p2", "p3"])
        self.assertEqual(report.status, "no_conflict")
        self.assertEqual(report.scope, "none")
        self.assertEqual(report.conflict_sets, [])
        self.assertEqual(report.conflicted_proof_ids, [])

    def test_no_conflict_probes_every_layer_once(self):
        v = ConflictVerifier()
        diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        # 单证 3 次 + 两两 3 次 + 整组 1 次
        self.assertEqual(
            v.aggregate_calls,
            [
                ("p1",), ("p2",), ("p3",),
                ("p1", "p2"), ("p1", "p3"), ("p2", "p3"),
                ("p1", "p2", "p3"),
            ],
        )
        # 定位不调用验证入口
        self.assertEqual(v.verify_aggregate_calls, 0)
        self.assertEqual(v.verify_calls, [])

    def test_singleton_group_probes_single_and_group(self):
        v = ConflictVerifier()
        report = diagnose_aggregation_conflict(
            batch([proof("solo")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "no_conflict")
        # 无两两层；整组层与单证同集合，仍各调用一次
        self.assertEqual(v.aggregate_calls, [("solo",), ("solo",)])


# ================================================================ 单证层

class TestSingleConflict(unittest.TestCase):
    def test_single_conflict_fields(self):
        v = ConflictVerifier(bad_singles={"p2"})
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "single")
        self.assertEqual(report.scope, "single")
        self.assertEqual(report.conflict_sets, [["p2"]])
        self.assertEqual(report.conflicted_proof_ids, ["p2"])
        self.assertEqual(report.proof_ids, ["p1", "p2", "p3"])

    def test_multiple_single_conflicts_in_batch_order(self):
        v = ConflictVerifier(bad_singles={"p1", "p3"})
        report = diagnose_aggregation_conflict(
            batch([proof("p2"), proof("p1"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "single")
        # 冲突集合按枚举（批次原序）记录
        self.assertEqual(report.conflict_sets, [["p1"], ["p3"]])
        # conflicted_proof_ids 按 proof_ids 顺序合并去重
        self.assertEqual(report.conflicted_proof_ids, ["p1", "p3"])

    def test_single_layer_stops_before_pairs(self):
        # 即使整组/两两也会不兼容，单证层命中后不再进入下一层
        v = ConflictVerifier(
            bad_singles={"p1"}, bad_pairs={("p1", "p2")}, bad_group=True
        )
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "single")
        self.assertEqual(v.aggregate_calls, [("p1",), ("p2",), ("p3",)])


# ================================================================ 两两层

class TestPairConflict(unittest.TestCase):
    def test_pair_conflict_fields_and_inner_order(self):
        v = ConflictVerifier(bad_pairs={("p1", "p2")})
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "pair")
        self.assertEqual(report.scope, "pair")
        self.assertEqual(report.conflict_sets, [["p1", "p2"]])
        self.assertEqual(report.conflicted_proof_ids, ["p1", "p2"])
        # 单证 3 + 两两 3，整组不再探测
        self.assertEqual(len(v.aggregate_calls), 6)

    def test_pairs_enumerated_by_source_positions(self):
        # 批次源序 p3, p1, p2：冲突对 (p3,p1) 在 (p3,p2) 与 (p1,p2) 之前
        v = ConflictVerifier(bad_pairs={("p3", "p1"), ("p3", "p2")})
        report = diagnose_aggregation_conflict(
            batch([proof("p3"), proof("p1"), proof("p2")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "pair")
        # 集合间按源序下标枚举；集合内保持源序
        self.assertEqual(
            report.conflict_sets, [["p3", "p1"], ["p3", "p2"]]
        )
        # conflicted_proof_ids 按 proof_ids 顺序合并去重
        self.assertEqual(report.conflicted_proof_ids, ["p3", "p1", "p2"])

    def test_conflicted_ids_follow_proof_ids_order_not_set_order(self):
        # 冲突对为 (p1,p2)，批次源序 p2 在前：conflicted 仍按 proof_ids
        v = ConflictVerifier(bad_pairs={("p1", "p2")})
        report = diagnose_aggregation_conflict(
            batch([proof("p3"), proof("p2"), proof("p1")]), v, GROUP_ID
        )
        self.assertEqual(report.conflict_sets, [["p2", "p1"]])
        self.assertEqual(report.conflicted_proof_ids, ["p2", "p1"])

    def test_all_three_pairs_conflict(self):
        v = ConflictVerifier(
            bad_pairs={("p1", "p2"), ("p1", "p3"), ("p2", "p3")}
        )
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "pair")
        self.assertEqual(
            report.conflict_sets,
            [["p1", "p2"], ["p1", "p3"], ["p2", "p3"]],
        )
        self.assertEqual(report.conflicted_proof_ids, ["p1", "p2", "p3"])

    def test_pair_layer_stops_before_group(self):
        v = ConflictVerifier(bad_pairs={("p2", "p3")}, bad_group=True)
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3"), proof("p4")]),
            v,
            GROUP_ID,
        )
        self.assertEqual(report.status, "pair")
        self.assertEqual(report.conflict_sets, [["p2", "p3"]])
        # 单证 4 + 两两 6；整组不调用
        self.assertEqual(len(v.aggregate_calls), 10)


# ================================================================ 整组层

class TestHigherOrderConflict(unittest.TestCase):
    def test_higher_order_report(self):
        v = ConflictVerifier(bad_group=True)
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "higher_order")
        self.assertEqual(report.scope, "higher_order")
        # higher_order 只含整组 proof_ids
        self.assertEqual(report.conflict_sets, [["p1", "p2", "p3"]])
        self.assertEqual(report.conflicted_proof_ids, ["p1", "p2", "p3"])
        # 单证 3 + 两两 3 + 整组 1
        self.assertEqual(len(v.aggregate_calls), 7)
        # 最后一次调用即整组
        self.assertEqual(v.aggregate_calls[-1], ("p1", "p2", "p3"))

    def test_higher_order_preserves_source_order(self):
        v = ConflictVerifier(bad_group=True)
        report = diagnose_aggregation_conflict(
            batch([proof("z"), proof("a"), proof("m")]), v, GROUP_ID
        )
        self.assertEqual(
            report.conflict_sets, [["z", "a", "m"]]
        )
        self.assertEqual(report.conflicted_proof_ids, ["z", "a", "m"])


# ================================================================ group_id 校验

class TestGroupSelection(unittest.TestCase):
    def test_invalid_group_id_variants(self):
        v = ConflictVerifier()
        for bad in (None, "", 123, b"groth16:c1:k1", ("g",), ["g"]):
            with self.subTest(bad=bad):
                with self.assertRaises(
                    InvalidAggregationGroupSelectionError
                ) as ctx:
                    diagnose_aggregation_conflict(
                        batch([proof("p1")]), v, bad
                    )
                self.assertEqual(ctx.exception.group_id, bad)
                # 非法 group_id 不调用 aggregate
                self.assertEqual(v.aggregate_calls, [])

    def test_unknown_group(self):
        v = ConflictVerifier()
        with self.assertRaises(UnknownAggregationGroupError) as ctx:
            diagnose_aggregation_conflict(
                batch([proof("p1")]), v, "groth16:c1:nope"
            )
        self.assertEqual(ctx.exception.group_id, "groth16:c1:nope")
        self.assertEqual(v.aggregate_calls, [])

    def test_selects_only_requested_group(self):
        v = ConflictVerifier(bad_group=True)
        data = batch(
            [
                proof("a1", key="k1"),
                proof("b1", key="k2"),
                proof("a2", key="k1"),
                proof("b2", key="k2"),
            ]
        )
        report = diagnose_aggregation_conflict(
            data, v, "groth16:c1:k2"
        )
        self.assertEqual(report.proof_ids, ["b1", "b2"])
        # 所有探针只含 k2 成员
        for ids in v.aggregate_calls:
            self.assertTrue(set(ids) <= {"b1", "b2"})

    def test_group_id_matches_protocol_circuit_key_format(self):
        class PlonkConflictVerifier(ConflictVerifier):
            protocol = "plonk"

        v = PlonkConflictVerifier()
        data = batch([proof("p1", proto="plonk", circuit="c9", key="kX")])
        report = diagnose_aggregation_conflict(
            data, v, "plonk:c9:kX"
        )
        self.assertEqual(report.group_id, "plonk:c9:kX")
        self.assertEqual(report.status, "no_conflict")


# ================================================================ 异常优先级

class TestValidationPriority(unittest.TestCase):
    def test_batch_errors_before_group_selection(self):
        v = ConflictVerifier()
        # 空批次先于非法/未知 group_id
        with self.assertRaises(EmptyBatchError):
            diagnose_aggregation_conflict({"batch_id": "B", "proofs": []}, v, "")
        with self.assertRaises(EmptyBatchError):
            diagnose_aggregation_conflict(
                {"batch_id": "B", "proofs": []}, v, "groth16:c1:missing"
            )
        # 字段错误先于 group_id 校验
        bad = proof("p1")
        del bad["circuit_id"]
        with self.assertRaises(InvalidProofError):
            diagnose_aggregation_conflict(batch([bad]), v, "")
        # 重复 proof_id 先于 group_id 校验
        with self.assertRaises(DuplicateProofIdError):
            diagnose_aggregation_conflict(
                batch([proof("dup"), proof("dup")]), v, ""
            )

    def test_invalid_group_before_unknown_group_and_protocol(self):
        v = ConflictVerifier()
        # 非法 group_id 在验证器解析之前：无验证器也先报选择非法
        with self.assertRaises(InvalidAggregationGroupSelectionError):
            diagnose_aggregation_conflict(
                batch([proof("p1")]), {}, ""
            )
        # 未知 group 在未知 protocol 之前（不解析验证器即可判定）
        with self.assertRaises(UnknownAggregationGroupError):
            diagnose_aggregation_conflict(
                batch([proof("p1", proto="mystery")]), {}, "groth16:c1:nope"
            )

    def test_unknown_protocol(self):
        v = ConflictVerifier()
        with self.assertRaises(UnsupportedProofSystemError) as ctx:
            diagnose_aggregation_conflict(
                batch([proof("p1", proto="mystery")]), v, "mystery:c1:k1"
            )
        self.assertEqual(ctx.exception.protocol, "mystery")
        self.assertEqual(v.aggregate_calls, [])

    def test_contract_error_missing_aggregate(self):
        v = IncompleteVerifier()
        with self.assertRaises(VerifierContractError):
            diagnose_aggregation_conflict(
                batch([proof("p1")]), v, GROUP_ID
            )

    def test_verifiers_list_and_mapping_forms(self):
        data = batch([proof("p1"), proof("p2")])
        list_report = diagnose_aggregation_conflict(
            data, [ConflictVerifier()], GROUP_ID
        )
        dict_report = diagnose_aggregation_conflict(
            data, {"groth16": ConflictVerifier()}, GROUP_ID
        )
        self.assertEqual(list_report.status, "no_conflict")
        self.assertEqual(dict_report.status, "no_conflict")


# ================================================================ 异常传播

class TestExceptionPropagation(unittest.TestCase):
    def test_non_incompatible_exception_at_single_propagates(self):
        v = ConflictVerifier(explode={("p2",)})
        with self.assertRaises(RuntimeError):
            diagnose_aggregation_conflict(
                batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
            )
        # p1 正常、p2 抛出后停止：不继续枚举
        self.assertEqual(v.aggregate_calls, [("p1",), ("p2",)])

    def test_non_incompatible_exception_at_pair_propagates(self):
        v = ConflictVerifier(bad_pairs={("p1", "p3")}, explode={("p2", "p3")})
        # 枚举顺序：(p1,p2) ok, (p1,p3) 不兼容记冲突 —— 记录后本层继续，
        # (p2,p3) 抛 RuntimeError 原样传播。
        with self.assertRaises(RuntimeError):
            diagnose_aggregation_conflict(
                batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
            )

    def test_incompatible_subclass_is_caught(self):
        v = ConflictVerifier(bad_pairs={("p1", "p2")}, subclass_on={("p2",)})
        # p2 单证层抛 IncompatibleAggregationError 子类：记为单证冲突
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertEqual(report.status, "single")
        self.assertEqual(report.conflict_sets, [["p2"]])


# ================================================================ 只读与幂等

class TestReadOnlyAndIdempotent(unittest.TestCase):
    def test_batch_not_mutated(self):
        import copy

        data = batch(
            [
                proof("p1", inputs=[{"x": 1}]),
                proof("p2", inputs=[{"y": 2}]),
                proof("p3"),
            ]
        )
        snapshot = copy.deepcopy(data)
        v = ConflictVerifier(bad_pairs={("p1", "p3")})
        diagnose_aggregation_conflict(data, v, GROUP_ID)
        self.assertEqual(data, snapshot)

    def test_repeated_diagnosis_equal(self):
        data = batch([proof("p1"), proof("p2"), proof("p3"), proof("p4")])
        v1 = ConflictVerifier(bad_singles={"p2"}, bad_pairs={("p1", "p3")})
        r1 = diagnose_aggregation_conflict(data, v1, GROUP_ID)
        v2 = ConflictVerifier(bad_singles={"p2"}, bad_pairs={("p1", "p3")})
        r2 = diagnose_aggregation_conflict(data, v2, GROUP_ID)
        self.assertEqual(r1.to_dict(), r2.to_dict())

    def test_only_aggregate_is_called_never_verify(self):
        v = ConflictVerifier(bad_group=True)
        diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        self.assertGreater(len(v.aggregate_calls), 0)
        self.assertEqual(v.verify_aggregate_calls, 0)
        self.assertEqual(v.verify_calls, [])


# ================================================================ to_dict

class TestToDict(unittest.TestCase):
    def test_fixed_key_order_no_conflict(self):
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2")]), ConflictVerifier(), GROUP_ID
        )
        payload = report.to_dict()
        self.assertEqual(
            list(payload),
            [
                "group_id",
                "proof_ids",
                "status",
                "scope",
                "conflict_sets",
                "conflicted_proof_ids",
            ],
        )
        self.assertEqual(payload["group_id"], GROUP_ID)
        self.assertEqual(payload["proof_ids"], ["p1", "p2"])
        self.assertEqual(payload["status"], "no_conflict")
        self.assertEqual(payload["scope"], "none")
        self.assertEqual(payload["conflict_sets"], [])
        self.assertEqual(payload["conflicted_proof_ids"], [])
        # 不含证明材料、调用栈或验证器信息
        self.assertNotIn("proof", payload)
        self.assertNotIn("public_inputs", payload)

    def test_pair_payload_and_list_copies(self):
        v = ConflictVerifier(bad_pairs={("p1", "p2")})
        report = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        )
        payload = report.to_dict()
        self.assertEqual(payload["status"], "pair")
        self.assertEqual(payload["scope"], "pair")
        self.assertEqual(payload["conflict_sets"], [["p1", "p2"]])
        self.assertEqual(payload["conflicted_proof_ids"], ["p1", "p2"])
        # to_dict 返回副本，修改不影响报告
        payload["conflict_sets"].append(["p9"])
        payload["proof_ids"].append("p9")
        self.assertEqual(report.conflict_sets, [["p1", "p2"]])
        self.assertEqual(report.proof_ids, ["p1", "p2", "p3"])

    def test_higher_order_payload(self):
        v = ConflictVerifier(bad_group=True)
        payload = diagnose_aggregation_conflict(
            batch([proof("p1"), proof("p2"), proof("p3")]), v, GROUP_ID
        ).to_dict()
        self.assertEqual(payload["status"], "higher_order")
        self.assertEqual(payload["scope"], "higher_order")
        self.assertEqual(payload["conflict_sets"], [["p1", "p2", "p3"]])
        self.assertEqual(payload["conflicted_proof_ids"], ["p1", "p2", "p3"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
