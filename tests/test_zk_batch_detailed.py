"""verify_batch_detailed / BatchVerificationReport 测试。

覆盖五种组内情形（聚合成功、聚合异常、聚合拒绝、回退后部分失败、
单证全通过但聚合失败），两入口异常一致性、result 同值、to_dict 固定
键序、消息不含 proof/public_inputs 与确定性。

直接运行：python tests/test_zk_batch_detailed.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchVerificationReport,
    BatchVerificationResult,
    DuplicateProofIdError,
    EmptyBatchError,
    GroupVerificationReport,
    IncompatibleAggregationError,
    InvalidProofError,
    ProofVerificationDetail,
    UnsupportedProofSystemError,
    VerifierContractError,
    ZKVerifier,
    verify_batch,
    verify_batch_detailed,
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


class StubVerifier(ZKVerifier):
    """可编排行为的假验证器（与基线冒烟测试同款）。"""

    protocol = "groth16"

    def __init__(
        self,
        aggregate_ok=True,
        agg_result=None,
        reject_ids=(),
        error_ids=(),
        agg_verify=True,
        agg_error=None,
        agg_exc=None,
    ):
        self._aggregate_ok = aggregate_ok
        self._agg_result = agg_result if agg_result is not None else ["AGG"]
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._agg_verify = agg_verify
        self._agg_error = agg_error  # 传给 verify_aggregate 异常的消息
        self._agg_exc = agg_exc      # 显式指定 verify_aggregate 抛出的异常
        self.calls = {"aggregate": 0, "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        if not self._aggregate_ok:
            raise IncompatibleAggregationError("cannot aggregate this group")
        return self._agg_result

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        if self._agg_exc is not None:
            raise self._agg_exc
        if self._agg_error is not None:
            raise RuntimeError(self._agg_error)
        return self._agg_verify

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        if p.proof_id in self._errors:
            raise ValueError("bad proof bytes")
        return p.proof_id not in self._reject


class EmptyMessageError(Exception):
    """str(exc) 为空串的异常。"""


class BoomAggregate(StubVerifier):
    def __init__(self, exc=None):
        super().__init__()
        self._exc = exc if exc is not None else RuntimeError("aggregator down")

    def aggregate(self, proofs):
        self.calls["aggregate"] += 1
        raise self._exc


BATCH = {
    "batch_id": "B-1",
    "proofs": [proof("p1"), proof("p2"), proof("p3")],
}


# ================================================================ 模型与结构

class TestReportShape(unittest.TestCase):
    def test_report_types_and_top_level_keys(self):
        report = verify_batch_detailed(BATCH, StubVerifier())
        self.assertIsInstance(report, BatchVerificationReport)
        self.assertIsInstance(report.result, BatchVerificationResult)
        self.assertEqual(list(report.to_dict().keys()), ["result", "groups"])

    def test_group_and_detail_key_order(self):
        report = verify_batch_detailed(
            {"batch_id": "B", "proofs": [proof("p1")]}, StubVerifier()
        )
        g = report.groups[0]
        self.assertIsInstance(g, GroupVerificationReport)
        self.assertEqual(
            list(g.to_dict().keys()),
            [
                "group_id",
                "proof_ids",
                "aggregate_call_status",
                "aggregate_verify_status",
                "fell_back",
                "proofs",
            ],
        )
        self.assertEqual(
            list(g.proofs[0].to_dict().keys()),
            ["proof_id", "status", "message"],
        )
        self.assertIsInstance(g.proofs[0], ProofVerificationDetail)

    def test_result_dict_key_order(self):
        report = verify_batch_detailed(BATCH, StubVerifier())
        self.assertEqual(
            list(report.result.to_dict().keys()),
            ["batch_id", "aggregate_count", "passed", "failed", "failures"],
        )


# ================================================================ 五种组情形

class TestGroupScenarios(unittest.TestCase):
    def test_aggregate_success(self):
        v = StubVerifier()
        report = verify_batch_detailed(BATCH, v)
        self.assertEqual(len(report.groups), 1)
        g = report.groups[0]
        self.assertEqual(g.group_id, "groth16:c1:k1")
        self.assertEqual(g.proof_ids, ["p1", "p2", "p3"])
        self.assertEqual(g.aggregate_call_status, "succeeded")
        self.assertEqual(g.aggregate_verify_status, "passed")
        self.assertFalse(g.fell_back)
        self.assertEqual(
            [(d.proof_id, d.status, d.message) for d in g.proofs],
            [("p1", "passed", ""), ("p2", "passed", ""), ("p3", "passed", "")],
        )
        # 聚合通过不回退单证
        self.assertEqual(v.calls["verify"], [])

    def test_aggregate_exception_marks_error_and_not_run(self):
        report = verify_batch_detailed(BATCH, BoomAggregate())
        g = report.groups[0]
        self.assertEqual(g.aggregate_call_status, "error")
        self.assertEqual(g.aggregate_verify_status, "not_run")
        self.assertFalse(g.fell_back)
        self.assertEqual(g.proof_ids, ["p1", "p2", "p3"])
        self.assertEqual(
            [(d.proof_id, d.status) for d in g.proofs],
            [("p1", "not_run"), ("p2", "not_run"), ("p3", "not_run")],
        )
        self.assertTrue(
            all(d.message == "RuntimeError: aggregator down" for d in g.proofs)
        )
        # failures 归责 aggregate 阶段
        self.assertTrue(
            all(f.stage == "aggregate" for f in report.result.failures)
        )

    def test_aggregate_exception_empty_str_message_keeps_type_name_only(self):
        report = verify_batch_detailed(
            {"batch_id": "B", "proofs": [proof("p1")]},
            BoomAggregate(exc=EmptyMessageError()),
        )
        self.assertEqual(report.groups[0].proofs[0].message, "EmptyMessageError")

    def test_aggregate_rejected_falls_back(self):
        # 回退后单证全部通过
        v = StubVerifier(agg_verify=False)
        report = verify_batch_detailed(BATCH, v)
        g = report.groups[0]
        self.assertEqual(g.aggregate_call_status, "succeeded")
        self.assertEqual(g.aggregate_verify_status, "rejected")
        self.assertTrue(g.fell_back)
        self.assertEqual(v.calls["verify"], ["p1", "p2", "p3"])
        self.assertEqual([d.status for d in g.proofs], ["passed"] * 3)
        self.assertTrue(all(d.message == "" for d in g.proofs))
        # 单证全过但聚合被拒：failures 归责 aggregate_verify
        self.assertEqual(report.result.failed, 3)
        self.assertTrue(
            all(f.stage == "aggregate_verify" for f in report.result.failures)
        )
        self.assertTrue(
            all(f.code == "rejected" for f in report.result.failures)
        )

    def test_aggregate_verify_exception_falls_back_all_pass(self):
        report = verify_batch_detailed(BATCH, StubVerifier(agg_error="boom"))
        g = report.groups[0]
        self.assertEqual(g.aggregate_call_status, "succeeded")
        self.assertEqual(g.aggregate_verify_status, "error")
        self.assertTrue(g.fell_back)
        self.assertEqual([d.status for d in g.proofs], ["passed"] * 3)
        self.assertTrue(
            all(f.stage == "aggregate_verify" for f in report.result.failures)
        )
        self.assertTrue(
            all(f.code == "verify_error" for f in report.result.failures)
        )

    def test_fallback_partial_failure_keeps_order_and_messages(self):
        v = StubVerifier(agg_verify=False, reject_ids={"p2"}, error_ids={"p3"})
        report = verify_batch_detailed(BATCH, v)
        g = report.groups[0]
        self.assertTrue(g.fell_back)
        self.assertEqual(g.aggregate_verify_status, "rejected")
        details = [(d.proof_id, d.status, d.message) for d in g.proofs]
        self.assertEqual(
            details,
            [
                ("p1", "passed", ""),
                ("p2", "rejected", "proof rejected by verifier"),
                ("p3", "error", "ValueError: bad proof bytes"),
            ],
        )
        # proof_ids 与 proofs 均保持原序
        self.assertEqual([d.proof_id for d in g.proofs], ["p1", "p2", "p3"])
        # 计数
        self.assertEqual(report.result.passed, 1)
        self.assertEqual(report.result.failed, 2)

    def test_fallback_partial_failure_after_verify_error(self):
        # 聚合验证本身抛异常，回退后仍有单证失败
        v = StubVerifier(
            agg_error="aggregate verifier exploded",
            reject_ids={"p2"},
            error_ids={"p3"},
        )
        report = verify_batch_detailed(BATCH, v)
        g = report.groups[0]
        self.assertEqual(g.aggregate_verify_status, "error")
        self.assertTrue(g.fell_back)
        statuses = [d.status for d in g.proofs]
        self.assertEqual(statuses, ["passed", "rejected", "error"])
        # 回退存在单证失败 -> failures 只归责到失败单证（single_verify）
        stages = {f.stage for f in report.result.failures}
        self.assertEqual(stages, {"single_verify"})
        self.assertEqual(
            sorted(f.proof_id for f in report.result.failures), ["p2", "p3"]
        )


# ================================================================ 分组与顺序

class TestGroupOrder(unittest.TestCase):
    def test_groups_follow_first_appearance_with_inner_original_order(self):
        v = StubVerifier(agg_verify=False, reject_ids={"a1"})
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("z1", key="k2"),
                proof("a1", key="k1"),
                proof("m2", key="k1"),
                proof("z2", key="k2"),
            ],
        }
        report = verify_batch_detailed(batch, v)
        self.assertEqual(
            [g.group_id for g in report.groups],
            ["groth16:c1:k2", "groth16:c1:k1"],
        )
        g_k2, g_k1 = report.groups
        self.assertEqual(g_k2.proof_ids, ["z1", "z2"])
        self.assertEqual(
            [d.proof_id for d in g_k2.proofs], ["z1", "z2"]
        )
        self.assertEqual(g_k1.proof_ids, ["a1", "m2"])
        self.assertEqual(
            [(d.proof_id, d.status) for d in g_k1.proofs],
            [("a1", "rejected"), ("m2", "passed")],
        )

    def test_groups_keyed_by_protocol_circuit_key(self):
        v = StubVerifier()
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("p1", circuit="cA", key="kX"),
                proof("p2", circuit="cA", key="kY"),
                proof("p3", circuit="cB", key="kX"),
            ],
        }
        report = verify_batch_detailed(batch, v)
        self.assertEqual(
            [g.group_id for g in report.groups],
            ["groth16:cA:kX", "groth16:cA:kY", "groth16:cB:kX"],
        )
        self.assertEqual(report.result.aggregate_count, 3)


# ================================================================ 两入口一致性

class TestEntryEquivalence(unittest.TestCase):
    def _assert_result_equal(self, batch, verifier_factory):
        r1 = verify_batch(batch, verifier_factory())
        r2 = verify_batch_detailed(batch, verifier_factory()).result
        self.assertEqual(r1.to_dict(), r2.to_dict())
        return r1, r2

    def test_result_identical_all_pass(self):
        self._assert_result_equal(BATCH, StubVerifier)

    def test_result_identical_partial_fallback(self):
        self._assert_result_equal(
            BATCH,
            lambda: StubVerifier(
                agg_verify=False, reject_ids={"p2"}, error_ids={"p3"}
            ),
        )

    def test_result_identical_aggregate_exception(self):
        self._assert_result_equal(BATCH, BoomAggregate)

    def test_result_identical_all_pass_but_aggregate_fails(self):
        self._assert_result_equal(
            BATCH, lambda: StubVerifier(agg_verify=False)
        )

    def test_result_counts_multi_group(self):
        batch = {
            "batch_id": 77,
            "proofs": [
                proof("a", key="k1"),
                proof("b", key="k2"),
                proof("c", key="k1"),
            ],
        }
        r1, r2 = self._assert_result_equal(batch, StubVerifier)
        self.assertEqual(r1.batch_id, 77)
        self.assertEqual(r1.aggregate_count, 2)
        self.assertEqual(r1.passed, 3)
        self.assertEqual(r1.failed, 0)

    def _assert_raises_same(self, batch, verifier_factory, exc_type):
        with self.assertRaises(exc_type):
            verify_batch(batch, verifier_factory())
        with self.assertRaises(exc_type):
            verify_batch_detailed(batch, verifier_factory())

    def test_empty_batch_same_error(self):
        self._assert_raises_same({}, StubVerifier, EmptyBatchError)
        self._assert_raises_same(
            {"batch_id": "b", "proofs": []}, StubVerifier, EmptyBatchError
        )
        self._assert_raises_same(
            {"proofs": [proof("p1")]}, StubVerifier, EmptyBatchError
        )

    def test_invalid_fields_same_error(self):
        bad = proof("p1")
        del bad["circuit_id"]
        self._assert_raises_same(
            {"batch_id": "b", "proofs": [bad]}, StubVerifier, InvalidProofError
        )

    def test_duplicate_id_same_error(self):
        batch = {"batch_id": "b", "proofs": [proof("dup"), proof("dup")]}
        self._assert_raises_same(batch, StubVerifier, DuplicateProofIdError)

    def test_unknown_protocol_same_error(self):
        batch = {"batch_id": "b", "proofs": [proof("p1", proto="mystery")]}
        self._assert_raises_same(
            batch, StubVerifier, UnsupportedProofSystemError
        )

    def test_incompatible_aggregation_raises_and_no_report(self):
        batch = {"batch_id": "b", "proofs": [proof("p1"), proof("p2")]}
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch(batch, StubVerifier(aggregate_ok=False))
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch_detailed(batch, StubVerifier(aggregate_ok=False))

    def test_contract_error_same(self):
        class Incomplete(ZKVerifier):
            protocol = "halo2"

        batch = {"batch_id": "b", "proofs": [proof("p1", proto="halo2")]}
        self._assert_raises_same(batch, Incomplete, VerifierContractError)


# ================================================================ 消息与确定性

class TestMessagesAndDeterminism(unittest.TestCase):
    def test_proof_and_public_inputs_never_in_messages(self):
        secret_inputs = ["SECRET-INPUT-xyz"]
        secret_proof = {"secret": "SECRET-PROOF-abc"}
        v = StubVerifier(agg_verify=False, reject_ids={"p1"}, error_ids={"p2"})
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("p1", inputs=secret_inputs, body=secret_proof),
                proof("p2", inputs=secret_inputs, body=secret_proof),
            ],
        }
        report = verify_batch_detailed(batch, v)
        text = str(report.to_dict())
        self.assertNotIn("SECRET-INPUT-xyz", text)
        self.assertNotIn("SECRET-PROOF-abc", text)
        # result.failures 的消息同样不含
        for f in report.result.failures:
            self.assertNotIn("SECRET-INPUT-xyz", f.message)
            self.assertNotIn("SECRET-PROOF-abc", f.message)

    def test_deterministic_same_fields_and_order(self):
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("z", key="k2"),
                proof("a", key="k1"),
                proof("m", key="k1"),
            ],
        }
        factory = lambda: StubVerifier(  # noqa: E731
            agg_verify=False, reject_ids={"z", "a"}, error_ids={"m"}
        )
        first = verify_batch_detailed(batch, factory()).to_dict()
        second = verify_batch_detailed(batch, factory()).to_dict()
        self.assertEqual(first, second)
        # 组按首次出现序（k2 先），不受 failures 的 proof_id 排序影响
        self.assertEqual(
            [g["group_id"] for g in first["groups"]],
            ["groth16:c1:k2", "groth16:c1:k1"],
        )
        # result.failures 仍按 proof_id 排序
        self.assertEqual(
            [f["proof_id"] for f in first["result"]["failures"]],
            ["a", "m", "z"],
        )
        # 组内明细保持批次原序
        k1 = first["groups"][1]
        self.assertEqual(
            [(d["proof_id"], d["status"]) for d in k1["proofs"]],
            [("a", "rejected"), ("m", "error")],
        )

    def test_to_dict_is_plain_serializable_data(self):
        report = verify_batch_detailed(
            BATCH,
            StubVerifier(agg_verify=False, reject_ids={"p2"}, error_ids={"p3"}),
        ).to_dict()
        self.assertEqual(set(report), {"result", "groups"})
        self.assertEqual(set(report["result"]),
                         {"batch_id", "aggregate_count", "passed", "failed",
                          "failures"})
        g = report["groups"][0]
        self.assertEqual(
            set(g),
            {
                "group_id", "proof_ids", "aggregate_call_status",
                "aggregate_verify_status", "fell_back", "proofs",
            },
        )
        self.assertEqual(set(g["proofs"][0]), {"proof_id", "status", "message"})
        self.assertIsInstance(g["fell_back"], bool)


if __name__ == "__main__":
    unittest.main(verbosity=2)
