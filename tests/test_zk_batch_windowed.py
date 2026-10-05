"""verify_batch_windowed / WindowedBatchVerificationReport 测试。

覆盖：窗口切分（多组、末窗可短、首次出现序、不重排不混组）、容量上限
校验（bool 不算、越界/非整数、且不调用验证器）、五种窗内情形复用详细
流水线语义、聚合异常按窗定位、IncompatibleAggregationError 传播、
result 按证明计数、to_dict 固定键序与无 proof/public_inputs、与既有入口
一致的输入异常。

直接运行：python tests/test_zk_batch_windowed.py
"""

import sys
import unittest

sys.path.insert(0, ".")

from zk_batch import (  # noqa: E402
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidAggregationLimitError,
    InvalidProofError,
    MAX_BATCH_ITEMS,
    UnsupportedProofSystemError,
    VerifierContractError,
    WindowedBatchVerificationReport,
    WindowVerificationReport,
    ZKVerifier,
    verify_batch,
    verify_batch_detailed,
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


class RecordingVerifier(ZKVerifier):
    """记录每次 aggregate 收到的窗口成员，其余默认全通过。"""

    protocol = "groth16"

    def __init__(self):
        self.aggregate_calls = []
        self.verify_aggregate_calls = []
        self.verify_calls = []

    def aggregate(self, proofs):
        self.aggregate_calls.append([p.proof_id for p in proofs])
        return ["AGG", len(self.aggregate_calls)]

    def verify_aggregate(self, proofs, aggregated):
        self.verify_aggregate_calls.append([p.proof_id for p in proofs])
        return True

    def verify(self, p):
        self.verify_calls.append(p.proof_id)
        return True


class StubVerifier(ZKVerifier):
    """可编排行为的假验证器（与详细流水线测试同款）。"""

    protocol = "groth16"

    def __init__(
        self,
        reject_ids=(),
        error_ids=(),
        agg_verify=True,
        agg_error=None,
        agg_exc=None,
        aggregate_exc=None,
        incompatible_window_ids=(),
    ):
        self._reject = set(reject_ids)
        self._errors = set(error_ids)
        self._agg_verify = agg_verify
        self._agg_error = agg_error
        self._agg_exc = agg_exc
        self._aggregate_exc = aggregate_exc
        self._incompatible = set(incompatible_window_ids)
        self.calls = {"aggregate": [], "verify_aggregate": 0, "verify": []}

    def aggregate(self, proofs):
        ids = tuple(p.proof_id for p in proofs)
        self.calls["aggregate"].append(list(ids))
        if self._aggregate_exc is not None:
            raise self._aggregate_exc
        if set(ids) & self._incompatible:
            raise IncompatibleAggregationError("cannot aggregate this window")
        return ["AGG"]

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


class NonCallableVerifier(ZKVerifier):
    protocol = "groth16"
    aggregate = None  # type: ignore[assignment]

    def verify_aggregate(self, proofs, aggregated):
        return True

    def verify(self, p):
        return True


BATCH = {
    "batch_id": "B-1",
    "proofs": [proof(f"p{i}") for i in range(1, 8)],
}


# ================================================================ 模型与结构

class TestReportShape(unittest.TestCase):
    def test_report_types_and_top_level_keys(self):
        report = verify_batch_windowed(BATCH, RecordingVerifier(), 3)
        self.assertIsInstance(report, WindowedBatchVerificationReport)
        self.assertEqual(
            list(report.to_dict().keys()), ["result", "windows"]
        )

    def test_window_key_order(self):
        report = verify_batch_windowed(BATCH, RecordingVerifier(), 3)
        w = report.windows[0]
        self.assertIsInstance(w, WindowVerificationReport)
        self.assertEqual(
            list(w.to_dict().keys()),
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
        self.assertEqual(
            list(w.proofs[0].to_dict().keys()),
            ["proof_id", "status", "message"],
        )

    def test_dict_contains_no_material_fields(self):
        report = verify_batch_windowed(BATCH, StubVerifier(), 3)
        text = str(report.to_dict())
        self.assertNotIn("public_inputs", text)
        self.assertNotIn("blob", text)


# ================================================================ 窗口切分

class TestWindowing(unittest.TestCase):
    def test_single_group_windows_in_order_with_short_tail(self):
        verifier = RecordingVerifier()
        report = verify_batch_windowed(BATCH, verifier, 3)
        # 7 个证明 -> [p1 p2 p3] [p4 p5 p6] [p7]
        self.assertEqual(
            verifier.aggregate_calls,
            [["p1", "p2", "p3"], ["p4", "p5", "p6"], ["p7"]],
        )
        self.assertEqual(
            verifier.verify_aggregate_calls,
            [["p1", "p2", "p3"], ["p4", "p5", "p6"], ["p7"]],
        )
        self.assertEqual(
            [w.proof_ids for w in report.windows],
            [["p1", "p2", "p3"], ["p4", "p5", "p6"], ["p7"]],
        )
        self.assertEqual([w.window_index for w in report.windows], [1, 2, 3])
        self.assertTrue(
            all(w.group_id == "groth16:c1:k1" for w in report.windows)
        )
        self.assertEqual(report.result.aggregate_count, 3)
        self.assertEqual(report.result.passed, 7)
        self.assertEqual(report.result.failed, 0)
        self.assertEqual(report.result.failures, [])

    def test_group_first_appearance_order_and_per_group_index(self):
        # A B A B A A C：组首次出现序 A -> B -> C；窗序不跨组混切。
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("a1", key="A"),
                proof("b1", key="B"),
                proof("a2", key="A"),
                proof("b2", key="B"),
                proof("a3", key="A"),
                proof("a4", key="A"),
                proof("c1", key="C"),
            ],
        }
        verifier = RecordingVerifier()
        report = verify_batch_windowed(batch, verifier, 2)
        self.assertEqual(
            [(w.group_id, w.window_index, tuple(w.proof_ids))
             for w in report.windows],
            [
                ("groth16:c1:A", 1, ("a1", "a2")),
                ("groth16:c1:A", 2, ("a3", "a4")),
                ("groth16:c1:B", 1, ("b1", "b2")),
                ("groth16:c1:C", 1, ("c1",)),
            ],
        )
        # aggregate 只收窗内、按窗口报告序
        self.assertEqual(
            verifier.aggregate_calls,
            [["a1", "a2"], ["a3", "a4"], ["b1", "b2"], ["c1"]],
        )
        self.assertEqual(report.result.aggregate_count, 4)

    def test_full_group_size_one_window_matches_detailed(self):
        verifier_w = RecordingVerifier()
        verifier_d = RecordingVerifier()
        w = verify_batch_windowed(BATCH, verifier_w, 100)
        d = verify_batch_detailed(BATCH, verifier_d)
        self.assertEqual(len(w.windows), 1)
        self.assertEqual(w.windows[0].proof_ids, [f"p{i}" for i in range(1, 8)])
        self.assertEqual(w.result.to_dict(), d.result.to_dict())

    def test_window_size_one(self):
        verifier = RecordingVerifier()
        report = verify_batch_windowed(BATCH, verifier, 1)
        self.assertEqual(len(report.windows), 7)
        self.assertEqual(
            verifier.aggregate_calls,
            [[f"p{i}"] for i in range(1, 8)],
        )
        self.assertEqual([w.window_index for w in report.windows],
                         [1, 2, 3, 4, 5, 6, 7])

    def test_proofs_keep_input_order_in_result_and_details(self):
        batch = {
            "batch_id": "B",
            "proofs": [proof("z9"), proof("a1"), proof("m5")],
        }
        report = verify_batch_windowed(batch, StubVerifier(), 2)
        # 窗内明细保持批次原序，不做排序
        self.assertEqual(
            [d.proof_id for d in report.windows[0].proofs], ["z9", "a1"]
        )
        self.assertEqual(report.windows[1].proof_ids, ["m5"])


# ================================================================ 容量校验

class RecordingOnlyOnCall(RecordingVerifier):
    def __init__(self):
        super().__init__()
        self.touched = False

    def aggregate(self, proofs):  # pragma: no cover - 不应被调用
        self.touched = True
        return super().aggregate(proofs)


class TestMaxGroupSizeValidation(unittest.TestCase):
    def _assert_invalid(self, value):
        verifier = RecordingOnlyOnCall()
        with self.assertRaises(InvalidAggregationLimitError) as cm:
            verify_batch_windowed(BATCH, verifier, value)
        self.assertEqual(cm.exception.max_group_size, value)
        self.assertEqual(cm.exception.limit, MAX_BATCH_ITEMS)
        self.assertFalse(verifier.touched)
        self.assertEqual(verifier.aggregate_calls, [])

    def test_bool_rejected_even_though_int_subclass(self):
        self._assert_invalid(True)
        self._assert_invalid(False)

    def test_out_of_range_and_non_int_rejected(self):
        for value in (0, -1, MAX_BATCH_ITEMS + 1, 10**9, 1.5, "2", None,
                      [2], (2,)):
            self._assert_invalid(value)

    def test_boundaries_accepted(self):
        # 1 与 MAX_BATCH_ITEMS 合法
        r1 = verify_batch_windowed(BATCH, RecordingVerifier(), 1)
        self.assertEqual(r1.result.aggregate_count, 7)
        r2 = verify_batch_windowed(BATCH, RecordingVerifier(), MAX_BATCH_ITEMS)
        self.assertEqual(r2.result.aggregate_count, 1)

    def test_invalid_limit_with_bad_verifiers_does_not_call_methods(self):
        # 容量非法时不调用任何验证器方法：即使 verifiers 是不可解析的对象，
        # 也不会触发其内部行为（解析本身不调用 aggregate/verify_*）。
        class ExplodingResolver:
            def __iter__(self):  # pragma: no cover - 不应被调用
                raise AssertionError("verifier resolution must not run")

        with self.assertRaises(InvalidAggregationLimitError):
            verify_batch_windowed(BATCH, ExplodingResolver(), 0)

    def test_batch_errors_take_priority_over_limit(self):
        with self.assertRaises(EmptyBatchError):
            verify_batch_windowed({"batch_id": "B", "proofs": []},
                                  RecordingVerifier(), 0)
        with self.assertRaises(InvalidProofError):
            verify_batch_windowed(
                {"batch_id": "B", "proofs": [{"proof_id": "x"}]},
                RecordingVerifier(), 0,
            )
        with self.assertRaises(DuplicateProofIdError):
            verify_batch_windowed(
                {"batch_id": "B",
                 "proofs": [proof("dup"), proof("dup")]},
                RecordingVerifier(), 0,
            )


# ================================================================ 窗内流水线复用

class TestWindowPipelineSemantics(unittest.TestCase):
    def test_window_aggregate_pass_passes_without_single_verify(self):
        verifier = StubVerifier()
        report = verify_batch_windowed(BATCH, verifier, 2)
        self.assertEqual(report.result.failed, 0)
        self.assertEqual(verifier.calls["verify"], [])
        self.assertTrue(all(
            w.aggregate_call_status == "succeeded"
            and w.aggregate_verify_status == "passed"
            and not w.fell_back
            for w in report.windows
        ))
        for w in report.windows:
            self.assertTrue(all(d.status == "passed" for d in w.proofs))

    def test_window_aggregate_error_locates_only_that_window(self):
        # 仅含 p3 p4 的窗口 aggregate 抛非 Incompatible 异常：该窗失败，
        # 其他窗口照常通过且不回退。
        class SelectiveBoom(StubVerifier):
            def aggregate(self, proofs):
                self.calls["aggregate"].append(
                    [p.proof_id for p in proofs]
                )
                if {p.proof_id for p in proofs} == {"p3", "p4"}:
                    raise RuntimeError("aggregator down")
                return ["AGG"]

        verifier = SelectiveBoom()
        report = verify_batch_windowed(BATCH, verifier, 2)
        by_window = {tuple(w.proof_ids): w for w in report.windows}
        bad = by_window[("p3", "p4")]
        self.assertEqual(bad.aggregate_call_status, "error")
        self.assertEqual(bad.aggregate_verify_status, "not_run")
        self.assertFalse(bad.fell_back)
        self.assertTrue(all(d.status == "not_run" for d in bad.proofs))
        self.assertTrue(all(
            d.message == "RuntimeError: aggregator down"
            for d in bad.proofs
        ))
        # 其他窗口不受影响
        good = by_window[("p1", "p2")]
        self.assertEqual(good.aggregate_call_status, "succeeded")
        self.assertEqual(good.aggregate_verify_status, "passed")
        self.assertFalse(good.fell_back)
        # result：仅 p3、p4 失败，按 proof 计数；failures 按 proof_id 排序
        r = report.result
        self.assertEqual(len(report.windows), 4)
        self.assertEqual(r.aggregate_count, 4)
        self.assertEqual(r.passed, 5)
        self.assertEqual(r.failed, 2)
        self.assertEqual([f.proof_id for f in r.failures], ["p3", "p4"])
        self.assertTrue(all(
            f.stage == "aggregate" and f.code == "verify_error"
            for f in r.failures
        ))
        # 聚合失败的窗口不回退、不调用 verify
        self.assertNotIn("p3", verifier.calls["verify"])
        self.assertNotIn("p4", verifier.calls["verify"])

    def test_window_fallback_rejected_and_error_within_window_only(self):
        # 聚合验证被拒 -> 窗内按序回退；p3 抛异常、p5 被拒。两窗均含显式
        # 失败证明，不会落到“单证全过但聚合失败”的聚合归责分支。
        batch = {
            "batch_id": "B-6",
            "proofs": [proof(f"p{i}") for i in range(1, 7)],
        }
        verifier = StubVerifier(
            agg_verify=False, reject_ids={"p5"}, error_ids={"p3"}
        )
        report = verify_batch_windowed(batch, verifier, 3)
        # 窗口 [p1p2p3][p4p5p6] 全部回退
        statuses = {
            d.proof_id: d
            for w in report.windows for d in w.proofs
        }
        self.assertEqual(statuses["p1"].status, "passed")
        self.assertEqual(statuses["p3"].status, "error")
        self.assertIn("ValueError", statuses["p3"].message)
        self.assertEqual(statuses["p5"].status, "rejected")
        self.assertTrue(all(w.fell_back for w in report.windows))
        self.assertTrue(all(
            w.aggregate_verify_status == "rejected" for w in report.windows
        ))
        # 回退严格按窗：verify 调用顺序为每窗内的批次原序
        self.assertEqual(
            verifier.calls["verify"],
            ["p1", "p2", "p3", "p4", "p5", "p6"],
        )
        failed_ids = sorted(
            f.proof_id for f in report.result.failures
        )
        self.assertEqual(failed_ids, ["p3", "p5"])
        codes = {f.proof_id: f.code for f in report.result.failures}
        self.assertEqual(codes["p3"], "verify_error")
        self.assertEqual(codes["p5"], "rejected")
        stages = {f.proof_id: f.stage for f in report.result.failures}
        self.assertTrue(all(s == "single_verify" for s in stages.values()))

    def test_all_singles_pass_but_window_rejected_attributes_to_aggregate_verify(
        self,
    ):
        verifier = StubVerifier(agg_verify=False)
        report = verify_batch_windowed(
            {"batch_id": "B", "proofs": [proof("p1"), proof("p2")]},
            verifier, 2,
        )
        w = report.windows[0]
        self.assertTrue(w.fell_back)
        self.assertEqual(w.aggregate_verify_status, "rejected")
        self.assertEqual(report.result.failed, 2)
        self.assertTrue(all(
            f.stage == "aggregate_verify" and f.code == "rejected"
            for f in report.result.failures
        ))
        # 逐证状态仍为 passed
        self.assertTrue(all(d.status == "passed" for d in w.proofs))

    def test_incompatible_aggregation_propagates(self):
        verifier = StubVerifier(incompatible_window_ids={"p3"})
        with self.assertRaises(IncompatibleAggregationError):
            verify_batch_windowed(BATCH, verifier, 2)
        # 异常窗之后的窗口不再执行（按窗串行，异常直接传播）
        self.assertNotIn("p7", verifier.calls["verify"])

    def test_failure_independence_across_windows(self):
        # 第一窗聚合异常，第二窗聚合拒绝后单证全过（归责聚合验证），
        # 第三窗正常：三窗互不影响。
        class MixedVerifier(ZKVerifier):
            protocol = "groth16"

            def aggregate(self, proofs):
                ids = [p.proof_id for p in proofs]
                if ids == ["p1", "p2"]:
                    raise RuntimeError("boom")
                return ["AGG"]

            def verify_aggregate(self, proofs, aggregated):
                return [p.proof_id for p in proofs] != ["p3", "p4"]

            def verify(self, p):
                return True

        report = verify_batch_windowed(BATCH, MixedVerifier(), 2)
        by_window = {tuple(w.proof_ids): w for w in report.windows}
        w1 = by_window[("p1", "p2")]
        w2 = by_window[("p3", "p4")]
        w3 = by_window[("p5", "p6")]
        w4 = by_window[("p7",)]
        self.assertEqual(w1.aggregate_call_status, "error")
        self.assertFalse(w1.fell_back)
        self.assertTrue(w2.fell_back)
        self.assertEqual(w2.aggregate_verify_status, "rejected")
        self.assertEqual(w3.aggregate_verify_status, "passed")
        self.assertFalse(w3.fell_back)
        self.assertEqual(w4.aggregate_verify_status, "passed")
        # p1 p2 -> aggregate/verify_error；p3 p4 -> aggregate_verify/rejected
        by_id = {f.proof_id: f for f in report.result.failures}
        self.assertEqual(set(by_id), {"p1", "p2", "p3", "p4"})
        self.assertEqual(by_id["p1"].stage, "aggregate")
        self.assertEqual(by_id["p3"].stage, "aggregate_verify")
        self.assertEqual(report.result.passed, 3)
        self.assertEqual(report.result.failed, 4)


# ================================================================ 其他输入异常

class TestInputErrors(unittest.TestCase):
    def test_empty_batch(self):
        with self.assertRaises(EmptyBatchError):
            verify_batch_windowed(
                {"batch_id": "B", "proofs": []}, StubVerifier(), 2
            )

    def test_field_error(self):
        with self.assertRaises(InvalidProofError):
            verify_batch_windowed(
                {"batch_id": "B", "proofs": [{"proof_id": "x"}]},
                StubVerifier(), 2,
            )

    def test_duplicate_proof_id(self):
        with self.assertRaises(DuplicateProofIdError):
            verify_batch_windowed(
                {"batch_id": "B",
                 "proofs": [proof("dup"), proof("dup")]},
                StubVerifier(), 2,
            )

    def test_unknown_protocol(self):
        batch = {"batch_id": "B", "proofs": [proof("p1", proto="plonk")]}
        with self.assertRaises(UnsupportedProofSystemError) as cm:
            verify_batch_windowed(batch, StubVerifier(), 2)
        self.assertEqual(cm.exception.protocol, "plonk")

    def test_contract_violation_propagates(self):
        batch = {"batch_id": "B", "proofs": [proof("p1")]}
        with self.assertRaises(VerifierContractError):
            verify_batch_windowed(batch, NonCallableVerifier(), 2)


# ================================================================ 与既有入口一致

class TestParityWithExistingEntries(unittest.TestCase):
    def test_batch_id_passthrough_any_type(self):
        for bid in (None, 42, ("x",), {"k": 1}):
            batch = {"batch_id": bid, "proofs": [proof("p1")]}
            report = verify_batch_windowed(batch, StubVerifier(), 2)
            self.assertIs(report.result.batch_id, bid)

    def test_counts_are_proof_granularity(self):
        # 2 组、各 3 证、窗大小 3 -> 每组 1 窗，共 2 个聚合；每窗含一个
        # 显式被拒证明，失败严格按证明计数而不是按窗/组计。
        batch = {
            "batch_id": "B",
            "proofs": [
                proof("a1", key="A"), proof("b1", key="B"),
                proof("a2", key="A"), proof("b2", key="B"),
                proof("a3", key="A"), proof("b3", key="B"),
            ],
        }
        verifier = StubVerifier(agg_verify=False, reject_ids={"a2", "b3"})
        report = verify_batch_windowed(batch, verifier, 3)
        self.assertEqual(report.result.aggregate_count, 2)
        self.assertEqual(report.result.passed, 4)
        self.assertEqual(report.result.failed, 2)
        self.assertEqual(
            [f.proof_id for f in report.result.failures], ["a2", "b3"]
        )
        # verify_batch 同输入（不分窗）同样只有 a2、b3 失败
        plain = verify_batch(batch, StubVerifier(
            agg_verify=False, reject_ids={"a2", "b3"}
        ))
        self.assertEqual(
            sorted(f.proof_id for f in plain.failures), ["a2", "b3"]
        )

    def test_windowed_does_not_mutate_queue_or_other_entries(self):
        # 窗口化入口是引擎级新增；既有详细入口行为不变。
        d = verify_batch_detailed(BATCH, StubVerifier())
        w = verify_batch_windowed(BATCH, StubVerifier(), 3)
        self.assertEqual(
            [g.group_id for g in d.groups],
            sorted({win.group_id for win in w.windows})
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
