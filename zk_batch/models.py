"""数据模型：原始证明、失败项、批次结果与队列作业。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


# 分组键：protocol / circuit_id / aggregation_key
GroupKey = Tuple[str, str, str]

# 失败阶段
STAGE_NORMALIZE = "normalize"
STAGE_AGGREGATE = "aggregate"
STAGE_AGGREGATE_VERIFY = "aggregate_verify"
STAGE_SINGLE_VERIFY = "single_verify"
STAGES = (
    STAGE_NORMALIZE,
    STAGE_AGGREGATE,
    STAGE_AGGREGATE_VERIFY,
    STAGE_SINGLE_VERIFY,
)

# 失败码
CODE_REJECTED = "rejected"      # 验证方法返回 False
CODE_VERIFY_ERROR = "verify_error"  # 验证方法抛出异常

# 详细报告状态
# 聚合调用：aggregate 成功 / 抛非 IncompatibleAggregationError 异常
DETAIL_AGG_SUCCEEDED = "succeeded"
DETAIL_AGG_ERROR = "error"
# 聚合验证：verify_aggregate 返回 True / 返回 False（rejected）/ 抛异常
DETAIL_PASSED = "passed"
DETAIL_REJECTED = "rejected"
DETAIL_ERROR = "error"
# 逐证状态：未进入回退
DETAIL_NOT_RUN = "not_run"


@dataclass(frozen=True)
class Proof:
    """归一化后的单证。

    ``public_inputs`` 与 ``proof`` 原样保留调用方给出的对象，不做转换。
    """

    proof_id: str
    protocol: str
    circuit_id: str
    aggregation_key: str
    public_inputs: List[Any]
    proof: Any
    # 该证明在批次 proofs 中的原始下标，决定组内顺序
    index: int = 0

    @property
    def group_key(self) -> GroupKey:
        return (self.protocol, self.circuit_id, self.aggregation_key)

    @property
    def group_id(self) -> str:
        protocol, circuit_id, aggregation_key = self.group_key
        return f"{protocol}:{circuit_id}:{aggregation_key}"


@dataclass(frozen=True)
class Failure:
    """单个证明的失败定位记录。"""

    proof_id: str
    group_id: str
    stage: str
    code: str
    message: str

    def to_dict(self) -> dict:
        return {
            "proof_id": self.proof_id,
            "group_id": self.group_id,
            "stage": self.stage,
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True)
class BatchVerificationResult:
    """整批验证结果。``batch_id`` 原样回填。"""

    batch_id: Any
    aggregate_count: int
    passed: int
    failed: int
    failures: List[Failure] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "aggregate_count": self.aggregate_count,
            "passed": self.passed,
            "failed": self.failed,
            "failures": [f.to_dict() for f in self.failures],
        }

    def failure_groups(self) -> List[Dict[str, Any]]:
        """按聚合组汇总失败定位，返回分组摘要列表；无失败时返回空列表。

        仅提供已有 ``failures`` 的分组视图，不改动失败记录本身。摘要按
        ``group_id`` 升序，组内明细按 ``proof_id`` 升序；同一 proof_id 的
        多条记录（如 rejected 与 verify_error 并存）保持相对次序。
        每个摘要固定包含：

        * ``group_id`` ——分组标识；
        * ``failed_proof_ids`` ——该组失败证明 id（按 proof_id 升序、去重）；
        * ``failed_count`` ——该组失败证明数（按证明去重计数）；
        * ``failures`` ——该组失败明细列表，每项含 Failure 的
          ``proof_id``/``stage``/``code``/``message``（``group_id`` 已在
          摘要上，不在明细中重复）。
        """
        grouped: Dict[str, List[Failure]] = {}
        for failure in self.failures:
            grouped.setdefault(failure.group_id, []).append(failure)

        summaries: List[Dict[str, Any]] = []
        for group_id in sorted(grouped):
            # self.failures 已按 proof_id 排序；显式再排一次，保证直接构造
            # 的结果重复调用时顺序同样稳定。
            members = sorted(
                grouped[group_id],
                key=lambda f: (f.proof_id, f.stage, f.code, f.message),
            )
            proof_ids = sorted({f.proof_id for f in members})
            summaries.append(
                {
                    "group_id": group_id,
                    "failed_proof_ids": proof_ids,
                    "failed_count": len(proof_ids),
                    "failures": [
                        {
                            "proof_id": f.proof_id,
                            "stage": f.stage,
                            "code": f.code,
                            "message": f.message,
                        }
                        for f in members
                    ],
                }
            )
        return summaries


@dataclass
class VerificationJob:
    """队列中的一个验证作业。"""

    job_id: str
    batch: Any
    status: str
    result: Any = None  # type: BatchVerificationResult | None
    error: Any = None   # 运行中抛出的异常（作业仍标记 completed？见 queue 说明）


# ================================================================ 详细报告

@dataclass(frozen=True)
class ProofVerificationDetail:
    """组内单证明的验证明细。

    ``status`` 取值：

    * ``passed`` —— 通过（聚合验证通过时整组逐证同样记 passed）；
    * ``rejected`` —— 单证验证返回 False，``message`` 固定为
      "proof rejected by verifier"；
    * ``error`` —— 单证验证抛异常，``message`` 为「异常类型名: str」；
    * ``not_run`` —— 未执行到该证明（如聚合调用即失败）。

    ``proof`` 与 ``public_inputs`` 不出现在明细中，也不拼入任何消息。
    """

    proof_id: str
    status: str
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "proof_id": self.proof_id,
            "status": self.status,
            "message": self.message,
        }


@dataclass(frozen=True)
class GroupVerificationReport:
    """单个聚合组的验证报告；组内顺序一律为批次原序。

    * ``proof_ids`` —— 组内 proof_id，按批次原序；
    * ``aggregate_status`` —— 聚合调用状态：``succeeded`` / ``error``；
      抛 IncompatibleAggregationError 时整批抛出、不产生报告；
    * ``aggregate_message`` —— 聚合异常消息（成功为空串）；
    * ``aggregate_verify_status`` —— 聚合验证状态：``passed`` /
      ``rejected`` / ``error``；聚合未执行时为 ``not_run``；
    * ``fell_back`` —— 是否回退到逐证验证；
    * ``proofs`` —— 逐证状态与消息，按批次原序。
    """

    group_id: str
    proof_ids: List[str]
    aggregate_status: str
    aggregate_message: str
    aggregate_verify_status: str
    aggregate_verify_message: str
    fell_back: bool
    proofs: List[ProofVerificationDetail] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "proof_ids": list(self.proof_ids),
            "aggregate_status": self.aggregate_status,
            "aggregate_message": self.aggregate_message,
            "aggregate_verify_status": self.aggregate_verify_status,
            "aggregate_verify_message": self.aggregate_verify_message,
            "fell_back": self.fell_back,
            "proofs": [detail.to_dict() for detail in self.proofs],
        }


@dataclass(frozen=True)
class BatchVerificationReport:
    """整批详细报告。

    ``result`` 与同输入下 :func:`verify_batch` 返回的
    :class:`BatchVerificationResult` 同值；``groups`` 按分组首次出现顺序
    排列，组内保持批次原序。
    """

    result: BatchVerificationResult
    groups: List[GroupVerificationReport] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "result": self.result.to_dict(),
            "groups": [group.to_dict() for group in self.groups],
        }
