"""数据模型：原始证明、失败项、批次结果与队列作业。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Tuple


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


@dataclass
class VerificationJob:
    """队列中的一个验证作业。"""

    job_id: str
    batch: Any
    status: str
    result: Any = None  # type: BatchVerificationResult | None
    error: Any = None   # 运行中抛出的异常（作业仍标记 completed？见 queue 说明）
