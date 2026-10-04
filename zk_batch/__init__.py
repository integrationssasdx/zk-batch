"""zk_batch: ZK 证明批聚合验证服务。

公开入口：

* :func:`verify_batch` —— 按分组键聚合验证，失败回退单证验证。
* :func:`verify_batch_detailed` —— 同流水线的详细报告入口，
  返回 :class:`BatchVerificationReport`。
* :class:`VerificationQueue` —— 分组聚合验证的串行作业队列。
* :class:`ItemVerificationQueue` —— 排队逐项验证任务队列（可排队、
  可定位失败原因、基础设施故障可续跑）。
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
    Failure,
    GroupVerificationReport,
    ItemBatchResult,
    ItemResult,
    Proof,
    ProofVerificationDetail,
    TaskProgress,
    TaskReceipt,
    TaskSnapshot,
    VerificationJob,
)
from .queue import VerificationQueue
from .tasks import (
    INFRA_CODE_CONTRACT,
    INFRA_CODE_EXECUTION,
    INFRA_CODE_MATERIAL,
    INFRA_CODE_SAVE,
    INFRA_CODE_UNSUPPORTED,
    INFRA_STAGE_READ,
    INFRA_STAGE_SAVE,
    INFRA_STAGE_VERIFY,
    MAX_BATCH_ITEMS,
    ItemVerificationQueue,
)
from .verifier import ZKVerifier
from .engine import verify_batch, verify_batch_detailed

__all__ = [
    # 既有入口
    "verify_batch",
    "verify_batch_detailed",
    "VerificationQueue",
    "ZKVerifier",
    "Proof",
    "Failure",
    "BatchVerificationResult",
    "BatchVerificationReport",
    "GroupVerificationReport",
    "ProofVerificationDetail",
    "VerificationJob",
    # 排队逐项验证（任务流程）
    "ItemVerificationQueue",
    "MAX_BATCH_ITEMS",
    "ItemResult",
    "ItemBatchResult",
    "BatchSummary",
    "TaskReceipt",
    "TaskProgress",
    "TaskSnapshot",
    # 基础设施故障阶段与错误码
    "INFRA_STAGE_READ",
    "INFRA_STAGE_VERIFY",
    "INFRA_STAGE_SAVE",
    "INFRA_CODE_MATERIAL",
    "INFRA_CODE_UNSUPPORTED",
    "INFRA_CODE_CONTRACT",
    "INFRA_CODE_EXECUTION",
    "INFRA_CODE_SAVE",
    # 既有异常
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
    # 任务流程异常
    "DuplicateItemIdError",
    "InvalidItemIdError",
    "BatchSizeLimitError",
    "InvalidProofFormatError",
    "TaskNotFoundError",
    "TaskStateConflictError",
    "VerificationInfrastructureError",
]
