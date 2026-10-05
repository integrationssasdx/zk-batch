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


class ReverifyLineageMismatchError(QueueError):
    """复核作业不是源作业的直接 ``reverify_failures`` 作业。

    对账时要求复核作业确由该源作业直接复核生成；与
    :class:`UnknownJobError`（作业不存在）和
    :class:`ResultUnavailableError`（作业未 completed）互不替代。
    """


class InvalidGroupSelectionError(QueueError):
    """``reverify_groups`` 的分组选择非法。

    ``group_ids`` 必须是非空列表或元组，元素为非空字符串且不重复。
    检出非法时不创建作业、不改变源作业。
    """


class UnknownFailedGroupError(QueueError):
    """选中的合法分组标识不属于源作业的失败定位。

    每个请求的分组标识都必须能在源作业结果的失败分组中找到；检出时不
    创建作业、不改变源作业。
    """


class GroupReverifyLineageMismatchError(QueueError):
    """复核作业不是源作业的直接 ``reverify_groups`` 作业。

    按组对账时要求复核作业确由该源作业直接按组复核生成；与
    :class:`UnknownJobError`（作业不存在）、
    :class:`ResultUnavailableError`（作业未 completed）以及
    :class:`ReverifyLineageMismatchError`（逐证复核谱系）互不替代。
    """


# ------------------------------------------------- 排队批量验证（任务流）

class TaskValidationError(ZKBatchError):
    """任务流提交前完整输入校验错误的根。

    校验在入队前一次完成：请求未通过校验时不生成任务标识、不进入验证队列。
    """


class DuplicateItemIdError(TaskValidationError):
    """同一批内 ``item_id`` 重复（提交前检出，请求不进入队列）。"""

    def __init__(self, item_id: str):
        super().__init__(f"duplicate item_id: {item_id!r}")
        self.item_id = item_id


class InvalidItemIdError(TaskValidationError):
    """``item_id`` 缺失或不是非空字符串。"""


class BatchSizeLimitError(TaskValidationError):
    """单批验证项数量超过公开上限 :data:`MAX_BATCH_ITEMS`。"""

    def __init__(self, count: int, limit: int):
        super().__init__(f"batch size {count} exceeds limit {limit}")
        self.count = count
        self.limit = limit


class InvalidProofFormatError(TaskValidationError):
    """证明材料不满足公开格式约束（缺 ``proof`` 字段或其不是非空映射）。"""


class InvalidPriorityError(TaskValidationError):
    """``priority`` 不是 0 到 100 闭区间内的整数。

    ``bool`` 不是整数（``True``/``False`` 同样非法）；提交校验中优先级
    最后检查，非法时不生成任务标识、不进入队列，复核请求也不改变源任务。
    """

    def __init__(self, priority: object):
        super().__init__(
            f"priority must be an int in [0, 100], got {priority!r}"
        )
        self.priority = priority


class TaskError(ZKBatchError):
    """任务队列相关异常的根。"""


class TaskNotFoundError(TaskError):
    """任务标识在队列中不存在（含从未提交与失败任务记录被移除）。"""


class TaskStateConflictError(TaskError):
    """重复消费、终态后再次入队或任务状态互相冲突。"""


class NoRetryableItemsError(TaskError):
    """复核源任务没有任何可重试的验证项。

    源任务既没有失败的验证项，也没有因基础设施错误而未得出结果的
    后续项时抛出；与 :class:`TaskNotFoundError`（任务不存在）和
    :class:`TaskStateConflictError`（任务未终结）互不替代。
    """


class TaskLineageMismatchError(TaskError):
    """复核任务不是源任务的直接 ``retry_failed`` 任务。

    对账时要求复核任务确由该源任务直接复核生成；与
    :class:`TaskNotFoundError`（任务不存在）和
    :class:`TaskStateConflictError`（任务未终结）互不替代。
    """


class VerificationInfrastructureError(TaskError):
    """基础设施失败：无法读取证明材料、验证器执行失败或无法保存最终结果。

    与"证明本身不通过"不同：单证被拒或返回失败是正常的
    :attr:`ItemResult.status`，不是基础设施错误。出现该错误时任务进入
    ``failed`` 终态，错误定位固定保留 :attr:`index`（输入序号）、
    :attr:`item_id`、:attr:`stage` 与 :attr:`code`，且此前已完成项的结果
    一并保留。
    """

    def __init__(self, index=None, item_id=None, stage=None, code=None,
                 message="verification infrastructure failure"):
        super().__init__(message)
        #: 出错项在输入中的序号（0 起）；整体保存失败等无明确项下标时为 None
        self.index = index
        #: 出错项标识；无明确项时为 None
        self.item_id = item_id
        #: 失败阶段
        self.stage = stage
        #: 可稳定比较的错误码
        self.code = code
