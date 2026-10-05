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


@dataclass(frozen=True)
class WindowVerificationReport:
    """窗口化验证中单个聚合窗口的详细报告。

    字段语义与 :class:`GroupVerificationReport` 相同，另在 ``group_id``
    之后携带从 1 起、组内连续编号的 ``window_index``。``proof_ids`` 为
    该窗内证明按批次原序的标识；``proofs`` 为同序的
    :class:`ProofVerificationDetail`。不含 ``proof`` 与 ``public_inputs``。
    """

    group_id: str
    window_index: int
    proof_ids: List[str]
    aggregate_call_status: str
    aggregate_verify_status: str
    fell_back: bool
    proofs: List[ProofVerificationDetail]

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "window_index": self.window_index,
            "proof_ids": list(self.proof_ids),
            "aggregate_call_status": self.aggregate_call_status,
            "aggregate_verify_status": self.aggregate_verify_status,
            "fell_back": self.fell_back,
            "proofs": [p.to_dict() for p in self.proofs],
        }


@dataclass(frozen=True)
class WindowedBatchVerificationReport:
    """``verify_batch_windowed`` 的窗口化验证报告。

    ``result`` 为 :class:`BatchVerificationResult`，``batch_id`` 原样回填，
    ``aggregate_count`` 等于窗口总数；``passed``/``failed``/``failures``
    均按证明计数，沿用 :class:`Failure`。``windows`` 按分组首次出现序、
    组内按窗序排列，窗内证明保持批次原序。
    """

    result: BatchVerificationResult
    windows: List[WindowVerificationReport]

    def to_dict(self) -> dict:
        return {
            "result": self.result.to_dict(),
            "windows": [w.to_dict() for w in self.windows],
        }


@dataclass
class VerificationJob:
    """队列中的一个验证作业。"""

    job_id: str
    batch: Any
    status: str
    result: Any = None  # type: BatchVerificationResult | None
    error: Any = None   # 运行中抛出的异常（作业仍标记 completed？见 queue 说明）


# ============================================================ 任务流（逐项）

# 单批验证项数量的公开上限
MAX_BATCH_ITEMS = 1000

# 任务优先级的公开闭区间
MIN_TASK_PRIORITY = 0
MAX_TASK_PRIORITY = 100

# 任务状态
TASK_QUEUED = "queued"
TASK_PROCESSING = "processing"
TASK_COMPLETED = "completed"
TASK_FAILED = "failed"
TASK_CANCELLED = "cancelled"
TASK_TERMINAL_STATUSES = (TASK_COMPLETED, TASK_FAILED, TASK_CANCELLED)

# 逐项验证的失败阶段（固定字面量，与单证验证流水线一一对应）
ITEM_STAGE_PROOF_READ = "proof_read"
ITEM_STAGE_VERIFY = "verify"

# 逐项结果状态
ITEM_PASSED = "passed"
ITEM_FAILED = "failed"

# 可稳定比较的错误码
ITEM_CODE_REJECTED = "rejected"          # 验证器判定不通过
ITEM_CODE_INVALID_PROOF = "invalid_proof"      # 无法读取证明材料
ITEM_CODE_VERIFIER_UNAVAILABLE = "verifier_unavailable"  # 无可用验证器
ITEM_CODE_VERIFIER_FAULT = "verifier_fault"    # 验证器执行失败
ITEM_CODE_SAVE_FAULT = "result_save_fault"     # 无法保存最终结果


@dataclass(frozen=True)
class ItemResult:
    """单个验证项的结果，顺序与输入一致。

    仅承载定位信息：``index``（输入序号，0 起）、``item_id``、``status``
    （``passed``/``failed``）、``stage``、``code`` 与供人工定位的
    ``message``。绝不包含完整证明材料、内部调用栈或未公开验证器信息。
    """

    index: int
    item_id: str
    status: str
    stage: str = ""
    code: str = ""
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "item_id": self.item_id,
            "status": self.status,
            "stage": self.stage,
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True)
class BatchSummary:
    """提交时返回的当前批次摘要（不含证明材料）。"""

    total: int
    item_ids: List[str]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "item_ids": list(self.item_ids),
        }


@dataclass(frozen=True)
class TaskSubmission:
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
class TaskCancellationReceipt:
    """取消成功的回执。

    ``status`` 固定 ``cancelled``；``completed`` 固定 0（取消的任务尚
    未验证任何项）；``cancelled_item_ids`` 为整批验证项标识，按输入
    顺序全量保留。只承载定位信息，不含证明材料、``public_inputs``、
    调用栈或验证器信息。
    """

    task_id: str
    status: str
    total: int
    completed: int
    cancelled_item_ids: List[str]

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "total": self.total,
            "completed": self.completed,
            "cancelled_item_ids": list(self.cancelled_item_ids),
        }


@dataclass(frozen=True)
class TaskRetrySubmission:
    """复核任务的提交回执。

    ``source_task_id`` 为源任务标识；``task_id``/``status``/``summary``
    与 :class:`TaskSubmission` 同口径（``status`` 固定 ``queued``，
    ``summary`` 只覆盖入选复核的项）；``retried_indexes`` 为入选项在
    根任务输入中的零起下标，按根任务输入顺序排列。只承载定位信息，
    不含证明材料或验证器信息。
    """

    source_task_id: str
    task_id: str
    status: str
    summary: BatchSummary
    retried_indexes: List[int]

    def to_dict(self) -> dict:
        return {
            "source_task_id": self.source_task_id,
            "task_id": self.task_id,
            "status": self.status,
            "summary": self.summary.to_dict(),
            "retried_indexes": list(self.retried_indexes),
        }


@dataclass(frozen=True)
class BatchTaskResult:
    """整批逐项验证的聚合结果。

    ``results`` 与输入顺序一致；``failed_item_ids`` 为失败项标识列表，
    同样按输入顺序（不做额外排序），满足 ``total == passed + failed``。
    """

    task_id: str
    total: int
    passed: int
    failed: int
    results: List[ItemResult]
    failed_item_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "results": [r.to_dict() for r in self.results],
            "failed_item_ids": list(self.failed_item_ids),
        }


@dataclass(frozen=True)
class TaskProgress:
    """任务处理进度快照；未结束时 ``result`` 为 ``None``。"""

    task_id: str
    status: str
    total: int
    completed: int
    result: Any = None  # type: BatchTaskResult | None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "total": self.total,
            "completed": self.completed,
            "result": None if self.result is None else self.result.to_dict(),
        }


@dataclass(frozen=True)
class TaskScheduleEntry:
    """调度快照中的单个 queued 任务条目。

    只承载调度定位信息：``task_id``、``priority``（0-100）与该任务在
    当前执行顺序中的 ``queue_position``（1 起），以及该调度快照的条目
    总数 ``total``。不含证明材料、``public_inputs``、调用栈或验证器信息。
    """

    task_id: str
    priority: int
    queue_position: int
    total: int

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "priority": self.priority,
            "queue_position": self.queue_position,
            "total": self.total,
        }


@dataclass(frozen=True)
class TaskScheduleReport:
    """只读调度快照：queued 任务按执行顺序（优先级降序、入队先后）排列。

    ``entries`` 只含 queued 任务，终态任务不参与；``queued_count`` 等于
    ``len(entries)``，每条目的 ``total`` 与之相同。重复查询不调用验证器、
    不消费任务、不改变任何状态或结果。
    """

    queued_count: int
    entries: List[TaskScheduleEntry]

    def to_dict(self) -> dict:
        return {
            "queued_count": self.queued_count,
            "entries": [entry.to_dict() for entry in self.entries],
        }


# ============================================================ 复核对账

# 对账前后可能出现的结果之外的状态：入选项在该次验证中尚无结果
ITEM_UNRESOLVED = "unresolved"

# 复核项的唯一结论
OUTCOME_RECOVERED = "recovered"              # 复核后通过
OUTCOME_STILL_FAILED = "still_failed"        # 复核后仍失败
OUTCOME_STILL_UNRESOLVED = "still_unresolved"  # 复核后仍无结果


@dataclass(frozen=True)
class RetryOutcomeItem:
    """单个复核项的对账明细。

    ``index`` 为根任务输入的零起下标；``before_*`` 取自源任务、
    ``after_*`` 取自复核任务（``status`` 只取 ``passed``/``failed``/
    ``unresolved``）；``outcome`` 为该项唯一结论（``recovered`` /
    ``still_failed`` / ``still_unresolved``）。只承载定位信息，不含
    证明材料、调用栈或未公开验证器信息。
    """

    index: int
    item_id: str
    before_status: str
    before_stage: str
    before_code: str
    before_message: str
    after_status: str
    after_stage: str
    after_code: str
    after_message: str
    outcome: str

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "item_id": self.item_id,
            "before_status": self.before_status,
            "before_stage": self.before_stage,
            "before_code": self.before_code,
            "before_message": self.before_message,
            "after_status": self.after_status,
            "after_stage": self.after_stage,
            "after_code": self.after_code,
            "after_message": self.after_message,
            "outcome": self.outcome,
        }


@dataclass(frozen=True)
class RetryOutcomeReport:
    """``retry_failed`` 复核结果的只读对账报告。

    ``retried_indexes`` 与 ``items`` 均按根任务输入顺序排列；
    ``recovered_item_ids``/``still_failed_item_ids``/``unresolved_item_ids``
    按结论归类、各自保持根任务输入顺序；三个 ``*_count`` 为对应归类的
    数量。只承载定位信息，不含证明材料、调用栈或未公开验证器信息。
    """

    source_task_id: str
    retry_task_id: str
    source_status: str
    retry_status: str
    retried_indexes: List[int]
    items: List[RetryOutcomeItem]
    recovered_item_ids: List[str]
    still_failed_item_ids: List[str]
    unresolved_item_ids: List[str]
    recovered_count: int
    still_failed_count: int
    unresolved_count: int

    def to_dict(self) -> dict:
        return {
            "source_task_id": self.source_task_id,
            "retry_task_id": self.retry_task_id,
            "source_status": self.source_status,
            "retry_status": self.retry_status,
            "retried_indexes": list(self.retried_indexes),
            "items": [item.to_dict() for item in self.items],
            "recovered_item_ids": list(self.recovered_item_ids),
            "still_failed_item_ids": list(self.still_failed_item_ids),
            "unresolved_item_ids": list(self.unresolved_item_ids),
            "recovered_count": self.recovered_count,
            "still_failed_count": self.still_failed_count,
            "unresolved_count": self.unresolved_count,
        }


# ================================================== 队列复核对账（reverify）

# 对账前后状态（固定字面量）：入选项在源作业中固定为 failed；复核后
# 只取 passed / failed。
REVERIFY_BEFORE_STATUS = "failed"
REVERIFY_AFTER_PASSED = "passed"
REVERIFY_AFTER_FAILED = "failed"


@dataclass(frozen=True)
class ReverifyOutcomeItem:
    """单个复核证明的对账明细。

    ``before_*`` 取自源作业保存的失败定位（``before_status`` 固定
    ``failed``）；``after_*`` 取自复核作业的定位——恢复项
    ``after_status`` 为 ``passed`` 且定位为空，仍失败项取复核
    :class:`Failure` 的 ``stage``/``code``/``message``。``outcome``
    只取 ``recovered`` / ``still_failed``。只承载定位信息，不含证明
    材料、``public_inputs``、调用栈或未公开验证器信息。
    """

    proof_id: str
    outcome: str
    before_status: str
    before_stage: str
    before_code: str
    before_message: str
    after_status: str
    after_stage: str
    after_code: str
    after_message: str

    def to_dict(self) -> dict:
        return {
            "proof_id": self.proof_id,
            "outcome": self.outcome,
            "before_status": self.before_status,
            "before_stage": self.before_stage,
            "before_code": self.before_code,
            "before_message": self.before_message,
            "after_status": self.after_status,
            "after_stage": self.after_stage,
            "after_code": self.after_code,
            "after_message": self.after_message,
        }


@dataclass(frozen=True)
class ReverifyOutcomeReport:
    """``reverify_failures`` 复核结果的只读对账报告。

    ``selected_proof_ids`` 与 ``items`` 均按源批次原序排列；
    ``recovered_proof_ids``/``still_failed_proof_ids`` 按结论归类、
    各自保持源批次原序。只承载定位信息，不含证明材料、
    ``public_inputs``、调用栈或未公开验证器信息。
    """

    source_job_id: str
    retry_job_id: str
    selected_proof_ids: List[str]
    items: List[ReverifyOutcomeItem]
    recovered_proof_ids: List[str]
    still_failed_proof_ids: List[str]

    def to_dict(self) -> dict:
        return {
            "source_job_id": self.source_job_id,
            "retry_job_id": self.retry_job_id,
            "selected_proof_ids": list(self.selected_proof_ids),
            "items": [item.to_dict() for item in self.items],
            "recovered_proof_ids": list(self.recovered_proof_ids),
            "still_failed_proof_ids": list(self.still_failed_proof_ids),
        }


# ========================================== 队列按组复核（reverify_groups）

@dataclass(frozen=True)
class GroupReverifySubmission:
    """按组复核提交成功的回执。

    ``source_job_id`` 为源作业标识；``job_id`` 为新 queued 作业标识，
    ``status`` 固定 ``queued``；``selected_group_ids`` 按请求输入顺序
    保留，``selected_proof_ids`` 按源批次原序排列（跨组按源批次中分组
    首次出现序，组内保持源批次原序）。只承载定位信息，不含证明材料、
    ``public_inputs``、调用栈或验证器信息。
    """

    source_job_id: str
    job_id: str
    status: str
    selected_group_ids: List[str]
    selected_proof_ids: List[str]

    def to_dict(self) -> dict:
        return {
            "source_job_id": self.source_job_id,
            "job_id": self.job_id,
            "status": self.status,
            "selected_group_ids": list(self.selected_group_ids),
            "selected_proof_ids": list(self.selected_proof_ids),
        }


@dataclass(frozen=True)
class GroupReverifyProofItem:
    """按组对账中单个证明复核前后的状态与消息。

    ``before_*`` 取自源作业该组的逐证详细状态，``after_*`` 取自复核作业
    同组同证的状态；消息只承载状态文本或经脱敏的异常信息。只承载定位
    信息，不含证明材料、``public_inputs``、调用栈或未公开验证器信息。
    """

    proof_id: str
    before_status: str
    before_message: str
    after_status: str
    after_message: str

    def to_dict(self) -> dict:
        return {
            "proof_id": self.proof_id,
            "before_status": self.before_status,
            "before_message": self.before_message,
            "after_status": self.after_status,
            "after_message": self.after_message,
        }


@dataclass(frozen=True)
class GroupReverifyGroupItem:
    """按组对账中单个聚合组的复核前后定位与唯一结论。

    组级 ``before_*``/``after_*`` 分别取自源作业与复核作业的
    :class:`GroupVerificationReport` 同名字段（``aggregate_call_status``、
    ``aggregate_verify_status``、``fell_back``）；``proofs`` 按组内批次
    原序给出每证前后状态。``outcome`` 只取 ``recovered`` /
    ``still_failed``：源组失败且复核整组无失败证明为 ``recovered``，
    否则为 ``still_failed``。
    """

    group_id: str
    proof_ids: List[str]
    outcome: str
    before_aggregate_call_status: str
    before_aggregate_verify_status: str
    before_fell_back: bool
    after_aggregate_call_status: str
    after_aggregate_verify_status: str
    after_fell_back: bool
    proofs: List[GroupReverifyProofItem]

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "proof_ids": list(self.proof_ids),
            "outcome": self.outcome,
            "before_aggregate_call_status":
                self.before_aggregate_call_status,
            "before_aggregate_verify_status":
                self.before_aggregate_verify_status,
            "before_fell_back": self.before_fell_back,
            "after_aggregate_call_status":
                self.after_aggregate_call_status,
            "after_aggregate_verify_status":
                self.after_aggregate_verify_status,
            "after_fell_back": self.after_fell_back,
            "proofs": [item.to_dict() for item in self.proofs],
        }


@dataclass(frozen=True)
class GroupReverifyOutcomeReport:
    """``reverify_groups`` 复核结果的只读对账报告。

    ``source_status``/``retry_status`` 为双方作业状态；``groups`` 按请求
    输入的分组顺序排列，组内证明保持源批次原序；
    ``recovered_group_ids``/``still_failed_group_ids`` 按结论归类、各自
    保持输入顺序，两个 ``*_count`` 为对应归类的数量。只承载定位信息，
    不含证明材料、``public_inputs``、调用栈或未公开验证器信息。
    """

    source_job_id: str
    retry_job_id: str
    source_status: str
    retry_status: str
    groups: List[GroupReverifyGroupItem]
    recovered_group_ids: List[str]
    still_failed_group_ids: List[str]
    recovered_count: int
    still_failed_count: int

    def to_dict(self) -> dict:
        return {
            "source_job_id": self.source_job_id,
            "retry_job_id": self.retry_job_id,
            "source_status": self.source_status,
            "retry_status": self.retry_status,
            "groups": [item.to_dict() for item in self.groups],
            "recovered_group_ids": list(self.recovered_group_ids),
            "still_failed_group_ids": list(self.still_failed_group_ids),
            "recovered_count": self.recovered_count,
            "still_failed_count": self.still_failed_count,
        }
