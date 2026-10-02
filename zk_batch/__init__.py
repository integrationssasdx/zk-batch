"""zk_batch: ZK 证明批聚合验证服务。

公开入口：

* :func:`verify_batch` —— 按分组键聚合验证，失败回退单证验证。
* :class:`VerificationQueue` —— 串行作业队列。
* :class:`ZKVerifier` —— 用户实现具体证明系统时继承的契约基类。
"""

from .errors import (
    CancelledJobError,
    CompletedJobError,
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidProofError,
    ResultUnavailableError,
    RunningJobError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerifierContractError,
)
from .models import (
    BatchVerificationResult,
    Failure,
    Proof,
    VerificationJob,
)
from .queue import VerificationQueue
from .verifier import ZKVerifier
from .engine import verify_batch

__all__ = [
    "verify_batch",
    "VerificationQueue",
    "ZKVerifier",
    "Proof",
    "Failure",
    "BatchVerificationResult",
    "VerificationJob",
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
]
