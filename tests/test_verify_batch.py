"""verify_batch 的行为测试：异常优先级、分组聚合、失败定位与排序。"""

from __future__ import annotations

import unittest

from zk_batch import (
    Batch,
    BatchVerificationResult,
    DefaultVerifier,
    Failure,
    IncompatibleAggregationError,
    Proof,
    STAGE_AGGREGATE,
    STAGE_AGGREGATE_VERIFY,
    STAGE_NORMALIZE,
    STAGE_SINGLE_VERIFY,
    UnsupportedProofSystemError,
    VerifierContractError,
    ZKVerifier,
    verify_batch,
)
from zk_batch.errors import (
    DuplicateProofIdError,
    EmptyBatchError,
    InvalidProofError,
)
from zk_batch.verifier import (
    CODE_AGGREGATE_ERROR,
    CODE_AGGREGATE_VERIFY_ERROR,
    CODE_NORMALIZE_ERROR,
    CODE_VERIFY_ERROR,
    CODE_VERIFY_FAILED,
)


def p(pid: str, proof=b"ok", inputs=(1,), *, protocol="groth16",
      circuit="c1", agg_key="k1") -> dict:
    return {
        "proof_id": pid,
        "protocol": protocol,
        "circuit_id": circuit,
        "aggregation_key": agg_key,
        "public_inputs": inputs,
        "proof": proof,
    }


def batch(proofs, bid="b1"):
    return {"batch_id": bid, "proofs": proofs}


class PrecedenceErrorsTest(unittest.TestCase):
    def setUp(self):
        self.registry = ZKVerifier()
        self.registry.register("groth16", DefaultVerifier())

    # 1) EmptyBatchError
    def test_empty_list(self):
        with self.assertRaises(EmptyBatchError):
            verify_batch(batch([]), self.registry)

    def test_missing_proofs_key(self):
        with self.assertRaises(EmptyBatchError):
            verify_batch({"batch_id": "b"}, self.registry)

    def test_empty_batch_object(self):
        with self.assertRaises(EmptyBatchError):
            verify_batch(Batch("b", ()), self.registry)

    # 2) InvalidProofError
    def test_batch_not_mapping(self):
        with self.assertRaises(InvalidProofError):
            verify_batch(42, self.registry)

    def test_proofs_wrong_type(self):
        with self.assertRaises(InvalidProofError):
            verify_batch({"batch_id": "b", "proofs": "nope"}, self.registry)

    def test_missing_batch_id(self):
        with self.assertRaises(InvalidProofError):
            verify_batch({"proofs": [p("a")]}, self.registry)

    def test_bad_batch_id_type(self):
        with self.assertRaises(InvalidProofError):
            verify_batch({"batch_id": 9, "proofs": [p("a")]}, self.registry)

    def test_proof_not_mapping(self):
        with self.assertRaises(InvalidProofError):
            verify_batch(batch([1]), self.registry)

    def test_proof_missing_field(self):
        raw = p("a")
        del raw["proof"]
        with self.assertRaises(InvalidProofError) as ctx:
            verify_batch(batch([raw]), self.registry)
        self.assertEqual(ctx.exception.field, "proof")
        self.assertEqual(ctx.exception.proof_id, "a")

    def test_proof_bad_string_field(self):
        raw = p("a")
        raw["circuit_id"] = 7
        with self.assertRaises(InvalidProofError):
            verify_batch(batch([raw]), self.registry)

    def test_proof_empty_string_field(self):
        raw = p("a")
        raw["aggregation_key"] = ""
        with self.assertRaises(InvalidProofError):
            verify_batch(batch([raw]), self.registry)

    # 3) DuplicateProofIdError
    def test_duplicate_proof_id(self):
        with self.assertRaises(DuplicateProofIdError) as ctx:
            verify_batch(batch([p("a"), p("a")]), self.registry)
        self.assertEqual(ctx.exception.proof_id, "a")

    def test_duplicate_takes_precedence_over_unknown_protocol(self):
        raw = [
            p("a", protocol="mystery"),
            p("a", protocol="mystery"),
        ]
        with self.assertRaises(DuplicateProofIdError):
            verify_batch(batch(raw), self.registry)

    # 4) IncompatibleAggregationError（DefaultVerifier：组内 proof 长度不一致）
    def test_incompatible_aggregation_propagates(self):
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch(
                batch([p("a", b"aa"), p("b", b"bbb")]), self.registry
            )

    # 5) UnsupportedProofSystemError
    def test_unknown_protocol(self):
        with self.assertRaises(UnsupportedProofSystemError) as ctx:
            verify_batch(batch([p("a", protocol="snork")]), self.registry)
        self.assertEqual(ctx.exception.protocol, "snork")

    # 6) VerifierContractError
    def test_missing_method(self):
        class Broken:
            def normalize(self, proof):
                return proof

        registry = ZKVerifier()
        registry.register("groth16", Broken())
        with self.assertRaises(VerifierContractError) as ctx:
            verify_batch(batch([p("a")]), registry)
        self.assertEqual(ctx.exception.method, "aggregate")

    def test_non_bool_verify_aggregate(self):
        class Weird(DefaultVerifier):
            def verify_aggregate(self, aggregate, proofs, public_inputs):
                return "yes"  # type: ignore[return-value]

        registry = ZKVerifier()
        registry.register("groth16", Weird())
        with self.assertRaises(VerifierContractError):
            verify_batch(batch([p("a")]), registry)

    def test_non_bool_single_verify(self):
        class Flip(DefaultVerifier):
            def verify_aggregate(self, aggregate, proofs, public_inputs):
                return False

            def verify(self, public_inputs, proof):
                return 1  # type: ignore[return-value]

        registry = ZKVerifier()
        registry.register("groth16", Flip())
        with self.assertRaises(VerifierContractError):
            verify_batch(batch([p("a")]), registry)


class GroupingAndAggregationTest(unittest.TestCase):
    def setUp(self):
        self.registry = ZKVerifier()
        self.registry.register("groth16", DefaultVerifier())

    def test_all_pass_single_group(self):
        result = verify_batch(
            batch([p("b"), p("a"), p("c")]), self.registry
        )
        self.assertIsInstance(result, BatchVerificationResult)
        self.assertEqual(result.batch_id, "b1")
        self.assertEqual(result.aggregate_count, 1)
        self.assertEqual(result.passed, ("a", "b", "c"))
        self.assertEqual(result.failed, ())
        self.assertEqual(result.failures, ())

    def test_groups_formed_on_all_three_keys(self):
        data = [
            p("p1", protocol="groth16", circuit="c1", agg_key="k1"),
            p("p2", protocol="groth16", circuit="c1", agg_key="k2"),
            p("p3", protocol="groth16", circuit="c2", agg_key="k1"),
            p("p4", protocol="groth16", circuit="c1", agg_key="k1"),
        ]
        result = verify_batch(batch(data), self.registry)
        self.assertEqual(result.aggregate_count, 3)
        self.assertEqual(result.passed, ("p1", "p2", "p3", "p4"))

    def test_aggregate_failure_falls_back_to_single(self):
        # 同组、等长；其中一个 invalid 使聚合验证失败，单证验证定位。
        data = [p("good1", b"aaaa"), p("bad1", b"invalid"[:7] + b"!"),
                p("good2", b"bbbb")]
        # b"invalid" 恰好 7 字节，保持长度一致需要全部 7 字节
        data = [p("good1", b"aaaaaaa"), p("bad1", b"invalid"),
                p("good2", b"bbbbbbb")]
        result = verify_batch(batch(data), self.registry)
        self.assertEqual(result.aggregate_count, 1)
        self.assertEqual(result.passed, ("good1", "good2"))
        self.assertEqual(result.failed, ("bad1",))
        failure = result.failures[0]
        self.assertEqual(
            failure,
            Failure("bad1", "groth16:c1:k1", STAGE_SINGLE_VERIFY,
                    CODE_VERIFY_FAILED, "verifier returned False"),
        )

    def test_normalize_failure_isolated(self):
        bad = p("bad", b"")  # DefaultVerifier 要求非空 bytes
        good = p("good", b"aa")
        result = verify_batch(batch([bad, good]), self.registry)
        self.assertEqual(result.passed, ("good",))
        self.assertEqual(result.failed, ("bad",))
        failure = result.failures[0]
        self.assertEqual(failure.stage, STAGE_NORMALIZE)
        self.assertEqual(failure.code, CODE_NORMALIZE_ERROR)
        self.assertEqual(failure.group_id, "groth16:c1:k1")
        self.assertIn("non-empty bytes", failure.message)

    def test_non_aggregatable_protocol_skips_aggregate(self):
        registry = ZKVerifier()
        registry.register("plain", DefaultVerifier(), aggregatable=False)
        calls = []

        class V(DefaultVerifier):
            def aggregate(self, proofs):
                calls.append("aggregate")
                return super().aggregate(proofs)

        registry2 = ZKVerifier()
        registry2.register("plain", V(), aggregatable=False)
        data = [
            p("a", b"aa", protocol="plain"),
            p("b", b"invalid", protocol="plain"),
        ]
        result = verify_batch(batch(data), registry2)
        self.assertEqual(calls, [])
        self.assertEqual(result.aggregate_count, 0)
        self.assertEqual(result.passed, ("a",))
        self.assertEqual(result.failed, ("b",))
        self.assertEqual(result.failures[0].stage, STAGE_SINGLE_VERIFY)

    def test_aggregate_generic_error_fails_group_closed(self):
        class Boom(DefaultVerifier):
            def aggregate(self, proofs):
                raise RuntimeError("aggregator down")

        registry = ZKVerifier()
        registry.register("groth16", Boom())
        result = verify_batch(batch([p("a", b"aa"), p("b", b"bb")]), registry)
        self.assertEqual(result.passed, ())
        self.assertEqual(result.failed, ("a", "b"))
        self.assertTrue(
            all(f.stage == STAGE_AGGREGATE for f in result.failures)
        )
        self.assertTrue(
            all(f.code == CODE_AGGREGATE_ERROR for f in result.failures)
        )

    def test_verify_aggregate_error_degrades_to_single(self):
        class BoomOnce(DefaultVerifier):
            def verify_aggregate(self, aggregate, proofs, public_inputs):
                raise RuntimeError("agg verifier bug")

        registry = ZKVerifier()
        registry.register("groth16", BoomOnce())
        result = verify_batch(
            batch([p("a", b"aaaaaaa"), p("b", b"invalid")]), registry
        )
        by_id = {f.proof_id: f for f in result.failures}
        # b 单证复现失败 → single_verify
        self.assertEqual(by_id["b"].stage, STAGE_SINGLE_VERIFY)
        # a 单证通过但聚合态异常 → fail-closed 的 aggregate_verify
        self.assertEqual(by_id["a"].stage, STAGE_AGGREGATE_VERIFY)
        self.assertEqual(by_id["a"].code, CODE_AGGREGATE_VERIFY_ERROR)
        self.assertIn("agg verifier bug", by_id["a"].message)

    def test_single_verify_exception_becomes_verify_error(self):
        class Raisey(DefaultVerifier):
            def verify_aggregate(self, aggregate, proofs, public_inputs):
                return False

            def verify(self, public_inputs, proof):
                raise RuntimeError("pairing engine crash")

        registry = ZKVerifier()
        registry.register("groth16", Raisey())
        result = verify_batch(batch([p("a", b"aa")]), registry)
        self.assertEqual(result.failed, ("a",))
        self.assertEqual(result.failures[0].stage, STAGE_SINGLE_VERIFY)
        self.assertEqual(result.failures[0].code, CODE_VERIFY_ERROR)

    def test_results_sorted_by_proof_id(self):
        data = [p("z", b""), p("a", b"invalid"), p("m", b"mmmmmmm"),
                p("b", b"")]
        result = verify_batch(batch(data), self.registry)
        self.assertEqual(result.failed, ("a", "b", "z"))
        self.assertEqual(result.passed, ("m",))
        self.assertEqual(
            tuple(f.proof_id for f in result.failures), ("a", "b", "z")
        )

    def test_proof_and_inputs_passed_through_unchanged(self):
        seen = {}

        class Recorder(DefaultVerifier):
            def aggregate(self, proofs):
                seen["proofs"] = proofs
                return super().aggregate(proofs)

            def verify_aggregate(self, aggregate, proofs, public_inputs):
                seen["public_inputs"] = public_inputs
                return super().verify_aggregate(aggregate, proofs, public_inputs)

        registry = ZKVerifier()
        registry.register("groth16", Recorder())
        sentinel_inputs = [10, 20, 30]
        blob = b"\x01\x02"

        raw = {
            "proof_id": "a",
            "protocol": "groth16",
            "circuit_id": "c",
            "aggregation_key": "k",
            "public_inputs": sentinel_inputs,
            "proof": blob,
        }
        verify_batch(batch([raw]), registry)
        self.assertIs(seen["public_inputs"][0], sentinel_inputs)
        self.assertEqual(seen["proofs"][0].proof, blob)

    def test_batch_object_input(self):
        b = Batch("obj", (
            Proof("a", "groth16", "c", "k", [1], b"aa"),
            Proof("b", "groth16", "c", "k", [2], b"bb"),
        ))
        result = verify_batch(b, self.registry)
        self.assertEqual(result.batch_id, "obj")
        self.assertEqual(result.passed, ("a", "b"))

    def test_default_registry_groth16(self):
        result = verify_batch(batch([p("a", b"aa")]))
        self.assertEqual(result.passed, ("a",))


if __name__ == "__main__":
    unittest.main()
