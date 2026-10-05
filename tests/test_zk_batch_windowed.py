"""verify_batch_windowed / WindowedBatchVerificationReport 测试。

覆盖窗口切分（连续、末窗可短、不重排不混组）、max_group_size 校验、
与批量入口一致的异常类型、窗内聚合/回退语义、result 计数口径与
to_dict 固定键序。

直接运行：python tests/test_zk_batch_windowed.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    BatchVerificationResult,
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidAggregationLimitError,
    InvalidProofError,
    MAX_BATCH_ITEMS,
    ProofVerificationDetail,
    UnsupportedProofSystemError,
    VerifierContractError,
    WindowedBatchVerificationReport,
    WindowVerificationReport,
    ZKVerifier,
    verify_batch_windowed,
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
    """可编排行为的假验证器（与详细报告测试同款，另记录聚合入参）。"""

    protocol = "groth16"

    def __init__(
        self,
        aggregate_ok=True,
        reject_ids=(),
        error_ids=(),
        agg_verify=True,
        agg_exc=None,
        incompatible=False,
    ):
        self._aggregate_ok = aggregate_ok
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._agg_verify = agg_verify
        self._agg_exc = agg_exc
        self._incompatible = incompatible
        self.calls = {"aggregate": [], "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        self.calls["aggregate"].append([p.proof_id for p in proofs])
        if self._incompatible:
            raise IncompatibleAggregationError("cannot aggregate this group")
        if not self._aggregate_ok:
            raise RuntimeError("aggregator down")
        return ["AGG"]

    def verify_aggregate(self, proofs, aggregated):
        self.calls["verify_aggregate"] += 1
        if self._agg_exc is not None:
            raise self._agg_exc
        return self._agg_verify

    def verify(self, p):
        self.calls["verify"].append(p.proof_id)
        if p.proof_id in self._errors:
            raise ValueError("bad proof bytes")
        return p.proof_id not in self._reject


def batch_of(*proofs, batch_id="B-1"):
    return {"batch_id": batch_id, "proofs": list(proofs)}


# ================================================================ 报告结构

class TestReportShape(unittest.TestCase):
    def test_report_types_and_top_level_keys(self):
        report = verify_batch_windowed(
            batch_of(proof("p1")), StubVerifier(), 1
        )
        self.assertIsInstance(report, WindowedBatchVerificationReport)
        self.assertIsInstance(report.result, BatchVerificationResult)
        self.assertIsInstance(report.windows[0], WindowVerificationReport)
        self.assertEqual(list(report.to_dict().keys()), ["result", "windows"])

    def test_window_key_order(self):
        report = verify_batch_windowed(
            batch_of(proof("p1")), StubVerifier(), 1
        )
        self.assertEqual(
            list(report.windows[0].to_dict().keys()),
            [
                "group_id",
                "window_index",
                "proof_ids",
                "aggregate_call_status",
                "aggregate_verify_status",
                "fell_back",
                "proofs",
            ],
        )
        detail = report.windows[0].proofs[0]
        self.assertIsInstance(detail, ProofVerificationDetail)
        self.assertEqual(
            list(detail.to_dict().keys()), ["proof_id", "status", "message"]
        )

    def test_to_dict_excludes_proof_material(self):
        report = verify_batch_windowed(
            batch_of(proof("p1", body={"secret": 1}, inputs=["x"])),
            StubVerifier(),
            1,
        )
        text = repr(report.to_dict())
        self.assertNotIn("secret", text)
        self.assertNotIn("blob", text)


# ================================================================ 窗口切分

class TestWindowSplitting(unittest.TestCase):
    def test_contiguous_windows_with_short_tail(self):
        verifier = StubVerifier()
        batch = batch_of(*(proof(f"p{i}") for i in range(1, 6)))
        report = verify_batch_windowed(batch, verifier, 2)
        self.assertEqual(
            [(w.window_index, w.proof_ids) for w in report.windows],
            [(1, ["p1", "p2"]), (2, ["p3", "p4"]), (3, ["p5"])],
        )
        # aggregate 按窗收窗内证明，不重排
        self.assertEqual(
            verifier.calls["aggregate"],
            [["p1", "p2"], ["p3", "p4"], ["p5"]],
        )
        self.assertEqual(report.result.aggregate_count, 3)

    def test_window_size_one(self):
        report = verify_batch_windowed(
            batch_of(proof("p1"), proof("p2")), StubVerifier(), 1
        )
        self.assertEqual(
            [w.proof_ids for w in report.windows], [["p1"], ["p2"]]
        )
        self.assertEqual(report.result.aggregate_count, 2)

    def test_window_covering_whole_group(self):
        report = verify_batch_windowed(
            batch_of(proof("p1"), proof("p2")), StubVerifier(), 10
        )
        self.assertEqual(len(report.windows), 1)
        self.assertEqual(report.windows[0].proof_ids, ["p1", "p2"])
        self.assertEqual(report.result.aggregate_count, 1)

    def test_groups_by_first_appearance_no_mixing(self):
        # 交错输入：同组证明不相邻，仍按分组首次出现序、组内源序切窗
        batch = batch_of(
            proof("a1", key="k1"),
            proof("b1", key="k2"),
            proof("a2", key="k1"),
            proof("b2", key="k2"),
            proof("a3", key="k1"),
        )
        verifier = StubVerifier()
        report = verify_batch_windowed(batch, verifier, 2)
        self.assertEqual(
            [(w.group_id, w.window_index, w.proof_ids) for w in report.windows],
            [
                ("groth16:c1:k1", 1, ["a1", "a2"]),
                ("groth16:c1:k1", 2, ["a3"]),
                ("groth16:c1:k2", 1, ["b1", "b2"]),
            ],
        )
        self.assertEqual(
            verifier.calls["aggregate"],
            [["a1", "a2"], ["a3"], ["b1", "b2"]],
        )

    def test_distinct_protocol_groups(self):
        class OtherVerifier(StubVerifier):
            protocol = "plonk"

        batch = batch_of(
            proof("g1", proto="groth16"),
            proof("p1", proto="plonk"),
            proof("g2", proto="groth16"),
        )
        report = verify_batch_windowed(
            batch, [StubVerifier(), OtherVerifier()], 2
        )
        self.assertEqual(
            [w.group_id for w in report.windows],
            ["groth16:c1:k1", "plonk:c1:k1"],
        )
        self.assertEqual(report.result.aggregate_count, 2)


# ============================================================ 容量参数校验

class TestMaxGroupSizeValidation(unittest.TestCase):
    def test_invalid_values_raise(self):
        for bad in (0, -1, MAX_BATCH_ITEMS + 1, True, False, "2", 2.0, None):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidAggregationLimitError):
                    verify_batch_windowed(
                        batch_of(proof("p1")), StubVerifier(), bad
                    )

    def test_invalid_limit_never_calls_verifier(self):
        verifier = StubVerifier()
        with self.assertRaises(InvalidAggregationLimitError):
            verify_batch_windowed(batch_of(proof("p1")), verifier, 0)
        self.assertEqual(verifier.calls["aggregate"], [])
        self.assertEqual(verifier.calls["verify_aggregate"], 0)
        self.assertEqual(verifier.calls["verify"], [])

    def test_boundary_values_accepted(self):
        verify_batch_windowed(batch_of(proof("p1")), StubVerifier(), 1)
        verify_batch_windowed(
            batch_of(proof("p1")), StubVerifier(), MAX_BATCH_ITEMS
        )

    def test_error_carries_limit(self):
        with self.assertRaises(InvalidAggregationLimitError) as ctx:
            verify_batch_windowed(batch_of(proof("p1")), StubVerifier(), 0)
        self.assertEqual(ctx.exception.limit, 0)


# ==================================================== 与批量入口一致的异常

class TestErrorParity(unittest.TestCase):
    def test_empty_batch(self):
        with self.assertRaises(EmptyBatchError):
            verify_batch_windowed(
                {"batch_id": "B", "proofs": []}, StubVerifier(), 2
            )
        with self.assertRaises(EmptyBatchError):
            verify_batch_windowed(
                {"proofs": [proof("p1")]}, StubVerifier(), 2
            )

    def test_invalid_proof_fields(self):
        bad = proof("p1")
        del bad["circuit_id"]
        with self.assertRaises(InvalidProofError):
            verify_batch_windowed(batch_of(bad), StubVerifier(), 2)

    def test_duplicate_proof_id(self):
        with self.assertRaises(DuplicateProofIdError):
            verify_batch_windowed(
                batch_of(proof("p1"), proof("p1")), StubVerifier(), 2
            )

    def test_unknown_protocol(self):
        with self.assertRaises(UnsupportedProofSystemError):
            verify_batch_windowed(
                batch_of(proof("p1", proto="unknown")), StubVerifier(), 2
            )

    def test_contract_violation(self):
        class NoAggregate(ZKVerifier):
            protocol = "groth16"

            def verify_aggregate(self, proofs, aggregated):
                return True

            def verify(self, p):
                return True

        with self.assertRaises(VerifierContractError):
            verify_batch_windowed(batch_of(proof("p1")), NoAggregate(), 2)

    def test_incompatible_aggregation_propagates(self):
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch_windowed(
                batch_of(proof("p1"), proof("p2")),
                StubVerifier(incompatible=True),
                2,
            )


# ================================================================ 窗内语义

class TestWindowSemantics(unittest.TestCase):
    def test_aggregate_error_located_per_window(self):
        # 第二个窗口聚合抛普通异常：只该窗记聚合失败，其他窗不受影响
        class Flaky(StubVerifier):
            def aggregate(self, proofs):
                ids = [p.proof_id for p in proofs]
                self.calls["aggregate"].append(ids)
                if ids == ["p3", "p4"]:
                    raise RuntimeError("aggregator down")
                return ["AGG"]

        batch = batch_of(*(proof(f"p{i}") for i in range(1, 6)))
        report = verify_batch_windowed(batch, Flaky(), 2)

        self.assertEqual(
            [
                (w.aggregate_call_status, w.aggregate_verify_status,
                 w.fell_back)
                for w in report.windows
            ],
            [
                ("succeeded", "passed", False),
                ("error", "not_run", False),
                ("succeeded", "passed", False),
            ],
        )
        self.assertEqual(
            [d.status for d in report.windows[1].proofs],
            ["not_run", "not_run"],
        )
        result = report.result
        self.assertEqual(result.batch_id, "B-1")
        self.assertEqual(result.aggregate_count, 3)
        self.assertEqual(result.passed, 3)
        self.assertEqual(result.failed, 2)
        self.assertEqual(
            [(f.proof_id, f.stage, f.code) for f in result.failures],
            [
                ("p3", "aggregate", "verify_error"),
                ("p4", "aggregate", "verify_error"),
            ],
        )
        self.assertEqual(
            result.failures[0].message, "RuntimeError: aggregator down"
        )

    def test_fallback_within_window_only(self):
        # 第一窗聚合验证被拒：仅窗内按序回退；第二窗不受影响
        class RejectFirst(StubVerifier):
            def __init__(self):
                super().__init__(reject_ids=("p2",))
                self._n = 0

            def verify_aggregate(self, proofs, aggregated):
                self.calls["verify_aggregate"] += 1
                self._n += 1
                return self._n != 1  # 第一窗 False，第二窗 True

        verifier = RejectFirst()
        batch = batch_of(*(proof(f"p{i}") for i in range(1, 5)))
        report = verify_batch_windowed(batch, verifier, 2)

        self.assertEqual(
            [(w.aggregate_verify_status, w.fell_back) for w in report.windows],
            [("rejected", True), ("passed", False)],
        )
        # 回退只覆盖第一窗成员，按窗内原序
        self.assertEqual(verifier.calls["verify"], ["p1", "p2"])
        self.assertEqual(
            [(d.proof_id, d.status) for d in report.windows[0].proofs],
            [("p1", "passed"), ("p2", "rejected")],
        )
        self.assertEqual(
            [(f.proof_id, f.stage, f.code) for f in report.result.failures],
            [("p2", "single_verify", "rejected")],
        )
        self.assertEqual(report.result.passed, 3)
        self.assertEqual(report.result.failed, 1)

    def test_single_verify_error_code(self):
        verifier = StubVerifier(agg_verify=False, error_ids=("p1",))
        report = verify_batch_windowed(
            batch_of(proof("p1"), proof("p2")), verifier, 2
        )
        failure = report.result.failures[0]
        self.assertEqual(failure.stage, "single_verify")
        self.assertEqual(failure.code, "verify_error")
        self.assertEqual(failure.message, "ValueError: bad proof bytes")
        self.assertEqual(report.windows[0].proofs[0].status, "error")

    def test_verify_aggregate_exception_falls_back(self):
        verifier = StubVerifier(agg_exc=RuntimeError("agg verify down"))
        report = verify_batch_windowed(
            batch_of(proof("p1"), proof("p2")), verifier, 2
        )
        window = report.windows[0]
        self.assertEqual(window.aggregate_verify_status, "error")
        self.assertTrue(window.fell_back)
        # 单证全过但整组失败：按聚合验证阶段归责到窗内每证
        self.assertEqual(
            [(f.proof_id, f.stage, f.code) for f in report.result.failures],
            [
                ("p1", "aggregate_verify", "verify_error"),
                ("p2", "aggregate_verify", "verify_error"),
            ],
        )

    def test_failures_sorted_by_proof_id_across_windows(self):
        # 两个窗口各有一个被拒证明；failures 按 proof_id 排序
        class RejectAll(StubVerifier):
            def __init__(self):
                super().__init__(agg_verify=False, reject_ids=("p1", "p4"))

        batch = batch_of(
            proof("p4"), proof("p2"), proof("p3"), proof("p1")
        )
        report = verify_batch_windowed(batch, RejectAll(), 2)
        self.assertEqual(
            [f.proof_id for f in report.result.failures], ["p1", "p4"]
        )
        self.assertEqual(report.result.failed, 2)
        self.assertEqual(report.result.passed, 2)


if __name__ == "__main__":
    unittest.main()
