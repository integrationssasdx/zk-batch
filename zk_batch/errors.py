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
