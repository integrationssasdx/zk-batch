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
