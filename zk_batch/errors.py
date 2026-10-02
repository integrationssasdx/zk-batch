"""zk_batch 的异常体系。

批量校验阶段（verify_batch 调用即失败，不产生结果）：

- EmptyBatchError              空批次
- InvalidProofError            批次/单证字段缺失或类型错误
- DuplicateProofIdError        proof_id 重复
- IncompatibleAggregationError 数据或验证器表明整组不可聚合
- UnsupportedProofSystemError  protocol 未注册验证器
- VerifierContractError        验证器缺方法或返回非布尔

队列阶段：

- UnknownJobError        作业不存在
- RunningJobError        对 running 作业调用 cancel
- CompletedJobError      对 completed 作业调用 cancel
- CancelledJobError      对 cancelled 作业调用 cancel
- ResultUnavailableError 非 completed 状态取 result
"""

from __future__ import annotations


class ZKBatchError(Exception):
    """zk_batch 批量验证相关错误的基类。"""


class EmptyBatchError(ZKBatchError):
    """批次缺少 proofs 或 proofs 为空。"""

    def __init__(self, message: str = "batch contains no proofs") -> None:
        super().__init__(message)


class InvalidProofError(ZKBatchError):
    """批次或单证字段缺失、类型不正确。

    属性 proof_id/field 在可定位时填充，用于指出出错的单证与字段。
    """

    def __init__(
        self,
        message: str,
        *,
        proof_id: str | None = None,
        field: str | None = None,
    ) -> None:
        self.proof_id = proof_id
        self.field = field
        super().__init__(message)


class DuplicateProofIdError(ZKBatchError):
    """同一批次中出现重复 proof_id。"""

    def __init__(self, proof_id: str) -> None:
        self.proof_id = proof_id
        super().__init__(f"duplicate proof_id: {proof_id!r}")


class IncompatibleAggregationError(ZKBatchError):
    """分组内单证无法聚合。

    验证器在 aggregate/verify_aggregate/verify 中主动抛出本异常时，
    verify_batch 会原样向上传播，表示整批应被拒绝而非降级为单证验证。
    """

    def __init__(
        self,
        message: str = "proofs in the group cannot be aggregated",
        *,
        group_id: str | None = None,
    ) -> None:
        self.group_id = group_id
        super().__init__(message)


class UnsupportedProofSystemError(ZKBatchError):
    """proof.protocol 没有注册对应验证器。"""

    def __init__(self, protocol: str) -> None:
        self.protocol = protocol
        super().__init__(f"unsupported proof system: {protocol!r}")


class VerifierContractError(ZKBatchError):
    """验证器不满足契约：缺少 aggregate/verify_aggregate/verify，或返回非布尔。"""

    def __init__(
        self,
        message: str,
        *,
        protocol: str | None = None,
        method: str | None = None,
    ) -> None:
        self.protocol = protocol
        self.method = method
        super().__init__(message)


class QueueError(Exception):
    """验证队列相关错误的基类。"""


class UnknownJobError(QueueError):
    """job_id 在队列中不存在。"""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"unknown job: {job_id!r}")


class RunningJobError(QueueError):
    """作业正在运行，不能取消。"""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"job {job_id!r} is running")


class CompletedJobError(QueueError):
    """作业已完成，不能取消。"""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"job {job_id!r} is completed")


class CancelledJobError(QueueError):
    """作业已取消，不能再次取消。"""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"job {job_id!r} is already cancelled")


class ResultUnavailableError(QueueError):
    """作业未处于 completed 状态，结果不可用。"""

    def __init__(self, job_id: str, state: str) -> None:
        self.job_id = job_id
        self.state = state
        super().__init__(f"result unavailable for job {job_id!r} in state {state!r}")
