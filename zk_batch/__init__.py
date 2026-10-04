"""zk_batch: ZK 证明批聚合验证服务。

公开入口：

* :func:`verify_batch` —— 按分组键聚合验证，失败回退单证验证。
* :func:`verify_batch_detailed` —— 同流水线的详细报告入口，
  返回 :class:`BatchVerificationReport`。
* :class:`VerificationQueue` —— 分组聚合的串行作业队列。
* :class:`VerificationTaskQueue` —— 可排队、可定位失败原因的逐项验证队列。
* :class:`ZKVerifier` —— 用户实现具体证明系统时继承的契约基类。
"""

from .errors import (
    BatchSizeLimitError,
    CancelledJobError,
    CompletedJobError,
    DuplicateItemIdError,
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidItemIdError,
    InvalidProofError,
    InvalidProofFormatError,
    NoFailedProofError,
    ResultUnavailableError,
    RunningJobError,
    TaskNotFoundError,
    TaskStateConflictError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerificationInfrastructureError,
    VerifierContractError,
)
from .models import (
    BatchSummary,
    BatchVerificationReport,
    BatchVerificationResult,
    BatchTaskResult,
    Failure,
    GroupVerificationReport,
    ItemResult,
    MAX_BATCH_ITEMS,
    Proof,
    ProofVerificationDetail,
    TaskProgress,
    TaskSubmission,
    VerificationJob,
)
from .queue import VerificationQueue
from .tasks import VerificationTaskQueue
from .verifier import ZKVerifier
from .engine import verify_batch, verify_batch_detailed

__all__ = [
    "verify_batch",
    "verify_batch_detailed",
    "VerificationQueue",
    "VerificationTaskQueue",
    "ZKVerifier",
    "Proof",
    "Failure",
    "BatchVerificationResult",
    "BatchVerificationReport",
    "GroupVerificationReport",
    "ProofVerificationDetail",
    "VerificationJob",
    "ItemResult",
    "BatchTaskResult",
    "BatchSummary",
    "TaskSubmission",
    "TaskProgress",
    "MAX_BATCH_ITEMS",
    "EmptyBatchError",
    "InvalidProofError",
    "DuplicateProofIdError",
    "IncompatibleAggregationError",
    "UnsupportedProofSystemError",
    "VerifierContractError",
    "RunningJobError",
    "CompletedJobError",
    "CancelledJobError",
    "UnknownJobError",
    "ResultUnavailableError",
    "NoFailedProofError",
    "DuplicateItemIdError",
    "InvalidItemIdError",
    "BatchSizeLimitError",
    "InvalidProofFormatError",
    "TaskNotFoundError",
    "TaskStateConflictError",
    "VerificationInfrastructureError",
]
