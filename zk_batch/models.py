"""数据模型：原始证明、失败项、批次结果与队列作业。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


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


# 详细报告：聚合调用 / 聚合验证 / 回退单证验证状态
AGG_CALL_SUCCEEDED = "succeeded"   # aggregate 正常返回
AGG_CALL_ERROR = "error"           # aggregate 抛出 IncompatibleAggregationError 之外的异常
AGG_VERIFY_PASSED = "passed"       # verify_aggregate 返回 True
AGG_VERIFY_REJECTED = "rejected"   # verify_aggregate 返回 False
AGG_VERIFY_ERROR = "error"         # verify_aggregate 抛异常
AGG_VERIFY_NOT_RUN = "not_run"     # aggregate 调用失败，未进入聚合验证
SINGLE_NOT_RUN = "not_run"         # 未进入回退（聚合调用即失败）
SINGLE_PASSED = "passed"
SINGLE_REJECTED = "rejected"
SINGLE_ERROR = "error"


@dataclass(frozen=True)
class ProofVerificationDetail:
    """回退阶段单条证明的状态与消息。

    未回退（聚合调用即失败）时 ``status`` 为 ``not_run``；``message`` 只
    承载状态文本或 ``异常类型名: str(exc)``，绝不包含 proof 与
    public_inputs 内容。
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
    """单个聚合组的详细报告；组内明细按批次原序。"""

    group_id: str
    proof_ids: List[str]
    aggregate_call_status: str
    aggregate_verify_status: str
    fell_back: bool
    proofs: List[ProofVerificationDetail]

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "proof_ids": list(self.proof_ids),
            "aggregate_call_status": self.aggregate_call_status,
            "aggregate_verify_status": self.aggregate_verify_status,
            "fell_back": self.fell_back,
            "proofs": [p.to_dict() for p in self.proofs],
        }


@dataclass(frozen=True)
class BatchVerificationReport:
    """整批详细报告。

    ``result`` 与 :func:`verify_batch` 返回的 :class:`BatchVerificationResult`
    同值（相同输入下字段完全一致）；``groups`` 按分组首次出现顺序排列，
    组内证明保持批次原序。
    """

    result: BatchVerificationResult
    groups: List[GroupVerificationReport]

    def to_dict(self) -> dict:
        return {
            "result": self.result.to_dict(),
            "groups": [g.to_dict() for g in self.groups],
        }


@dataclass
class VerificationJob:
    """队列中的一个验证作业。"""

    job_id: str
    batch: Any
    status: str
    result: Any = None  # type: BatchVerificationResult | None
    error: Any = None   # 运行中抛出的异常（作业仍标记 completed？见 queue 说明）


# ============================================ 排队逐项验证（任务流程）模型

# 单项结论
ITEM_PASSED = "passed"
ITEM_FAILED = "failed"

# 单项失败阶段（逐项验证只暴露单证验证这一个公开阶段）
ITEM_STAGE_VERIFY = "verify"

# 单项错误码：稳定可比较；验证方法返回 False / 抛异常两类，
# 与既有引擎的 rejected / verify_error 口径一致。
ITEM_CODE_REJECTED = "rejected"
ITEM_CODE_VERIFY_ERROR = "verify_error"


@dataclass(frozen=True)
class ItemResult:
    """单个验证项的结果；与提交顺序一致（index 为 0 基输入序号）。

    定位信息固定为 ``index``/``item_id``/``stage``/``code``/``message``：
    通过项 ``stage`` 与 ``code`` 为空串。任何字段都不包含证明材料、
    内部调用栈或未公开验证器信息。
    """

    index: int
    item_id: str
    passed: bool
    stage: str = ""
    code: str = ""
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "item_id": self.item_id,
            "passed": self.passed,
            "stage": self.stage,
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True)
class ItemBatchResult:
    """一个任务的整批逐项结果（终态可取）。

    ``results`` 严格按输入顺序排列；``failed_item_ids`` 为失败项
    标识，同样按输入顺序（稳定、可复现）。
    """

    task_id: str
    total: int
    passed_count: int
    failed_count: int
    results: List[ItemResult] = field(default_factory=list)
    failed_item_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "total": self.total,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "results": [r.to_dict() for r in self.results],
            "failed_item_ids": list(self.failed_item_ids),
        }


@dataclass(frozen=True)
class BatchSummary:
    """提交成功后随回执返回的当前批次摘要（提交期即可确定，不含结果）。"""

    total: int
    item_ids: List[str]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "item_ids": list(self.item_ids),
        }


@dataclass(frozen=True)
class TaskReceipt:
    """提交成功的回执：稳定任务标识、排队状态与批次摘要。"""

    task_id: str
    status: str
    summary: BatchSummary

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "summary": self.summary.to_dict(),
        }


@dataclass(frozen=True)
class TaskProgress:
    """任务的实时进度。

    ``completed`` 为已经得出结论（通过或失败）的验证项数；
    ``total`` 为整批项数。任务结束前查询不给出最终结果。
    """

    task_id: str
    status: str
    total: int
    completed: int

    @property
    def remaining(self) -> int:
        return self.total - self.completed

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "total": self.total,
            "completed": self.completed,
            "remaining": self.remaining,
        }


@dataclass(frozen=True)
class TaskSnapshot:
    """任务查询快照（统一查询入口的返回值）。

    * 任务未结束（queued/processing）或因基础设施故障停在 failed 时：
      ``result`` 为 ``None``（不提前给出最终聚合结果），``items`` 给出
      已完成项的逐条结论（输入顺序），``completed`` 为真实完成进度；
    * 任务 completed：``result`` 为稳定的 :class:`ItemBatchResult`，
      ``items`` 与其 ``results`` 同值同序。

    快照为只读视图：查询不调用验证器、不改变任务状态；终态后重复查询
    返回同一结果对象。
    """

    task_id: str
    status: str
    total: int
    completed: int
    items: List["ItemResult"] = field(default_factory=list)
    result: Optional["ItemBatchResult"] = None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "total": self.total,
            "completed": self.completed,
            "remaining": self.total - self.completed,
            "result": self.result.to_dict() if self.result is not None else None,
            "items": [r.to_dict() for r in self.items],
        }


