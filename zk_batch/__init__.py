"""zk_batch：零知识证明批聚合验证工具。

公开入口：

- verify_batch                       批次聚合验证
- VerificationQueue                  串行验证队列（enqueue/run_next/cancel/status/result）
- ZKVerifier / DefaultVerifier       验证器注册中心与内置参考实现
- Batch / Proof / Failure / BatchVerificationResult
- 全部异常类型
"""

from __future__ import annotations

from .errors import (
    CancelledJobError,
    CompletedJobError,
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidProofError,
    QueueError,
    ResultUnavailableError,
    RunningJobError,
    UnknownJobError,
    UnsupportedProofSystemError,
    VerifierContractError,
    ZKBatchError,
)
from .models import (
    Batch,
    BatchVerificationResult,
    Failure,
    Proof,
    STAGE_AGGREGATE,
    STAGE_AGGREGATE_VERIFY,
    STAGE_NORMALIZE,
    STAGE_SINGLE_VERIFY,
)
from .queue import (
    STATE_CANCELLED,
    STATE_COMPLETED,
    STATE_QUEUED,
    STATE_RUNNING,
    VerificationQueue,
)
from .verifier import verify_batch
from .verifiers import (
    DefaultVerifier,
    Verifier,
    ZKVerifier,
    default_registry,
)

__all__ = [
    # 入口
    "verify_batch",
    "VerificationQueue",
    "ZKVerifier",
    "DefaultVerifier",
    "default_registry",
    "Verifier",
    # 模型
    "Batch",
    "Proof",
    "Failure",
    "BatchVerificationResult",
    # 阶段常量
    "STAGE_NORMALIZE",
    "STAGE_AGGREGATE",
    "STAGE_AGGREGATE_VERIFY",
    "STAGE_SINGLE_VERIFY",
    # 队列状态
    "STATE_QUEUED",
    "STATE_RUNNING",
    "STATE_COMPLETED",
    "STATE_CANCELLED",
    # 异常
    "ZKBatchError",
    "EmptyBatchError",
    "InvalidProofError",
    "DuplicateProofIdError",
    "IncompatibleAggregationError",
    "UnsupportedProofSystemError",
    "VerifierContractError",
    "QueueError",
    "UnknownJobError",
    "RunningJobError",
    "CompletedJobError",
    "CancelledJobError",
    "ResultUnavailableError",
]
