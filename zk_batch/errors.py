"""异常层次。

验证流程六类错误（空批次、字段错误、重复 ID、不能聚合、未知系统、
验证器契约违约）依次检测、依次抛出；队列状态机的错误相互独立。
"""

from __future__ import annotations


class ZKBatchError(Exception):
    """zk_batch 所有异常的根。"""


# ---------------------------------------------------------------- 验证阶段

class EmptyBatchError(ZKBatchError):
    """批次为空（``batch_id`` 缺失或 proofs 为空）。"""


class InvalidProofError(ZKBatchError):
    """单证字段缺失或类型错误。"""


class DuplicateProofIdError(ZKBatchError):
    """同一批次内 ``proof_id`` 重复。"""

    def __init__(self, proof_id: str):
        super().__init__(f"duplicate proof_id: {proof_id!r}")
        self.proof_id = proof_id


class IncompatibleAggregationError(ZKBatchError):
    """同一分组内证明不能一起聚合。"""


class UnsupportedProofSystemError(ZKBatchError):
    """``protocol`` 未在任何已注册验证器中出现。"""

    def __init__(self, protocol: str):
        super().__init__(f"unsupported proof system: {protocol!r}")
        self.protocol = protocol


class VerifierContractError(ZKBatchError):
    """验证器缺方法，或其返回值不是布尔。"""


# ---------------------------------------------------------------- 队列阶段

class QueueError(ZKBatchError):
    """队列相关异常的根。"""


class RunningJobError(QueueError):
    """对正在运行的作业调用 cancel。"""


class CompletedJobError(QueueError):
    """对已完成的作业调用 cancel。"""


class CancelledJobError(QueueError):
    """对已取消的作业调用 cancel。"""


class UnknownJobError(QueueError):
    """作业 id 在队列中不存在。"""


class ResultUnavailableError(QueueError):
    """作业不在 completed 状态时调用 result。"""


class NoFailedProofError(QueueError):
    """completed 作业的结果中没有失败证明，无法发起复核。"""


# --------------------------------------------------- 排队逐项验证（任务流程）

class DuplicateItemIdError(ZKBatchError):
    """同一任务批次内稳定标识 ``item_id`` 重复。

    提交期校验抛出：该次提交不生成任务、不进入验证队列。
    """

    def __init__(self, item_id: str):
        super().__init__(f"duplicate item_id: {item_id!r}")
        self.item_id = item_id


class InvalidItemIdError(ZKBatchError):
    """验证项缺少稳定标识，或 ``item_id`` 为空/非字符串。"""


class BatchSizeLimitError(ZKBatchError):
    """单次提交的验证项数量超过公开上限 :data:`MAX_BATCH_ITEMS`。"""

    def __init__(self, size: int, limit: int):
        super().__init__(f"batch size {size} exceeds limit {limit}")
        self.size = size
        self.limit = limit


class InvalidProofFormatError(ZKBatchError):
    """证明材料格式不满足公开约束（缺字段或字段类型不符）。"""


class TaskNotFoundError(ZKBatchError):
    """任务标识在任务队列中不存在。"""


class TaskStateConflictError(ZKBatchError):
    """任务状态机冲突。

    覆盖：终态任务被再次置为 queued/processing（终态后再次入队）、
    状态被要求沿非法迁移变化（任务状态互相冲突）。重复消费由
    :meth:`run_next` 与终态保护共同保证，非法迁移统一抛本异常。
    """


class VerificationInfrastructureError(ZKBatchError):
    """无法读取证明材料、验证器执行失败或无法保存最终结果。

    区别于业务性验证不通过（``passed=False``）：这是基础设施层面的
    故障。任务保留此前已完成项及其定位信息，并可在故障消除后继续
    处理剩余项（已完成项不重复验证）。
    """

    def __init__(self, message: str, *, stage: str = "", code: str = "infrastructure_error"):
        super().__init__(message)
        #: 基础设施故障发生的阶段（读取材料/执行验证器/保存结果）
        self.stage = stage
        #: 可稳定比较的错误码
        self.code = code
