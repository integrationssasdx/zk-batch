"""串行验证队列。

状态机（仅四个状态）：

    queued -> running -> completed
                   \\--> cancelled（仅 queued 可取消）

* :meth:`enqueue` 入队，返回 ``job_id``；
* :meth:`run_next` 同步执行最早的 queued 作业，空队列返回 ``None``；
  同一作业同时只会被执行一次（串行执行，无并发）；
* :meth:`cancel` 仅 queued 可取消并返回 ``True``；运行/完成/已取消/不存在
  分别抛 RunningJobError / CompletedJobError / CancelledJobError /
  UnknownJobError；
* :meth:`result` 仅 completed 返回 BatchVerificationResult，其余状态抛
  ResultUnavailableError（不存在抛 UnknownJobError）。
* :meth:`reverify_failures` 以已完成作业的失败清单为输入，只挑出原批次中
  失败的证明（保持相对顺序、batch_id 不变）创建新的 queued 作业。源作业
  不存在抛 UnknownJobError；非 completed 抛 ResultUnavailableError；
  completed 但无失败证明抛 NoFailedProofError。

不持久化、不联网、不使用线程。
"""

from __future__ import annotations

import itertools
from collections import OrderedDict
from typing import Any, Dict, Optional

from .engine import verify_batch
from .errors import (
    CancelledJobError,
    CompletedJobError,
    NoFailedProofError,
    ResultUnavailableError,
    RunningJobError,
    UnknownJobError,
)
from .models import BatchVerificationResult, VerificationJob

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"


class VerificationQueue:
    """内存中的串行验证作业队列。"""

    def __init__(self, verifiers: Any = None):
        """:param verifiers: 传给 :func:`verify_batch` 的默认验证器集合。"""
        self._default_verifiers = verifiers
        self._jobs: "OrderedDict[str, VerificationJob]" = OrderedDict()
        # 每个作业的验证器覆盖；VerificationJob 不新增字段以免污染公开模型
        self._verifiers: Dict[str, Any] = {}
        self._counter = itertools.count(1)

    # ------------------------------------------------------------ 入队/执行

    def enqueue(self, batch: Any, verifiers: Any = None) -> str:
        """把批次加入队列，返回作业 id。

        ``verifiers`` 省略时使用构造队列时给定的默认验证器。
        """
        job_id = f"job-{next(self._counter)}"
        self._jobs[job_id] = VerificationJob(
            job_id=job_id,
            batch=batch,
            status=STATUS_QUEUED,
            result=None,
        )
        # 验证器随作业保存（不暴露在 VerificationJob 的对外字段语义里，
        # 仅执行期使用）
        self._job_verifiers(job_id, verifiers)
        return job_id

    def run_next(self) -> Optional[BatchVerificationResult]:
        """执行最早的 queued 作业；队列空返回 ``None``。

        作业执行成功后状态置为 completed 并返回结果。若验证本身抛异常
        （如 EmptyBatchError），异常向调用方传播，该作业从队列移除
        （四态状态机不为失败的输入保留悬挂作业）。
        """
        job = self._next_queued()
        if job is None:
            return None

        job.status = STATUS_RUNNING
        verifiers = self._verifiers.pop(job.job_id)
        try:
            result = verify_batch(job.batch, verifiers)
        except Exception:
            # 输入/验证错误不属于四个持久状态，移除以释放调用方重试。
            self._jobs.pop(job.job_id, None)
            raise
        job.status = STATUS_COMPLETED
        job.result = result
        return result

    def cancel(self, job_id: str) -> bool:
        """取消 queued 作业并返回 ``True``；其余状态按异常约定抛出。"""
        job = self._require_job(job_id)
        if job.status == STATUS_QUEUED:
            job.status = STATUS_CANCELLED
            self._verifiers.pop(job_id, None)
            return True
        if job.status == STATUS_RUNNING:
            raise RunningJobError(f"job {job_id!r} is running")
        if job.status == STATUS_COMPLETED:
            raise CompletedJobError(f"job {job_id!r} is completed")
        if job.status == STATUS_CANCELLED:
            raise CancelledJobError(f"job {job_id!r} is already cancelled")
        # 理论不可达：状态机封闭
        raise UnknownJobError(f"job {job_id!r} in unexpected state {job.status!r}")

    # ---------------------------------------------------------------- 查询

    def status(self, job_id: str) -> str:
        """返回 queued/running/completed/cancelled；不存在抛 UnknownJobError。"""
        return self._require_job(job_id).status

    def result(self, job_id: str) -> BatchVerificationResult:
        """仅 completed 作业可取结果。"""
        job = self._require_job(job_id)
        if job.status != STATUS_COMPLETED:
            raise ResultUnavailableError(
                f"job {job_id!r} has no result (status={job.status!r})"
            )
        return job.result  # type: ignore[return-value]

    # ---------------------------------------------------------------- 复核

    def reverify_failures(self, job_id: str, verifiers: Any = None) -> str:
        """只重验已完成作业结果中的失败证明，返回新作业 id。

        依据源作业保存的批次内容与失败清单，按 ``proof_id`` 挑出失败证明，
        并按源批次中的相对顺序组成子批次（``batch_id`` 不变、
        ``public_inputs``/``proof`` 原样保留），交给一个独立的 queued 作业；
        不重提整批，源作业状态与结果保持不变。

        ``verifiers`` 只作用于新作业，省略时使用队列默认验证器。新作业沿用
        run_next/cancel/status/result；执行时仍走 verify_batch 的完整校验与
        分组流水线。

        * 源作业不存在 ——UnknownJobError；
        * 存在但非 completed ——ResultUnavailableError；
        * completed 但 failures 为空 ——NoFailedProofError。
        """
        job = self._require_job(job_id)
        if job.status != STATUS_COMPLETED:
            raise ResultUnavailableError(
                f"job {job_id!r} has no result to reverify "
                f"(status={job.status!r})"
            )
        source_result: BatchVerificationResult = job.result
        failed_ids = {failure.proof_id for failure in source_result.failures}
        if not failed_ids:
            raise NoFailedProofError(
                f"job {job_id!r} completed with no failed proofs"
            )
        # 从已保存的源批次选证：按原批次相对顺序，证明对象原样保留。
        selected = [
            raw
            for raw in job.batch["proofs"]
            if raw["proof_id"] in failed_ids
        ]
        sub_batch = {
            "batch_id": source_result.batch_id,
            "proofs": selected,
        }
        return self.enqueue(sub_batch, verifiers)

    # ---------------------------------------------------------------- 内部

    def _next_queued(self) -> Optional[VerificationJob]:
        for job in self._jobs.values():
            if job.status == STATUS_QUEUED:
                return job
        return None

    def _require_job(self, job_id: str) -> VerificationJob:
        job = self._jobs.get(job_id)
        if job is None:
            raise UnknownJobError(f"unknown job id: {job_id!r}")
        return job

    # 每个作业的验证器覆盖；VerificationJob 不新增字段以免污染公开模型
    def _job_verifiers(self, job_id: str, verifiers: Any) -> None:
        chosen = verifiers if verifiers is not None else self._default_verifiers
        self._verifiers[job_id] = chosen
