"""串行验证队列。

作业生命周期（单线程、同步、内存态，不做并发调度与持久化）::

    queued ──run_next──▶ running ──成功──▶ completed
       │                   │
       └────cancel─────────┴────（running/completed/cancelled 均不可取消）
                           │
                         cancel 对 queued：queued ──▶ cancelled

- enqueue(batch) 入队，返回 job_id（FIFO，自增编号 job-1、job-2…）。
- run_next() 执行队首作业：queued→running→completed，返回该作业的
  BatchVerificationResult；队列空返回 None。verify_batch 抛出的异常会
  原样传播给 run_next 的调用方，该作业保留 running 状态（fail-closed，
  结果不可用、不可取消）。
- cancel(job_id) 仅对 queued 作业生效，返回 True 并迁出队列。
- status(job_id) 返回 queued/running/completed/cancelled 之一。
- result(job_id) 仅 completed 作业返回 BatchVerificationResult。

队列严格串行：同一时刻至多一个 running 作业，同一作业不会被执行两次。
"""

from __future__ import annotations

from collections import deque
from itertools import count
from typing import Any

from .errors import (
    CancelledJobError,
    CompletedJobError,
    ResultUnavailableError,
    RunningJobError,
    UnknownJobError,
)
from .models import BatchVerificationResult
from .verifier import verify_batch
from .verifiers import ZKVerifier

#: 作业状态值
STATE_QUEUED = "queued"
STATE_RUNNING = "running"
STATE_COMPLETED = "completed"
STATE_CANCELLED = "cancelled"


class _Job:
    __slots__ = ("job_id", "state", "batch", "verifiers", "result")

    def __init__(self, job_id: str, batch: Any, verifiers: ZKVerifier | None) -> None:
        self.job_id = job_id
        self.state = STATE_QUEUED
        self.batch = batch
        self.verifiers = verifiers
        self.result: BatchVerificationResult | None = None


class VerificationQueue:
    """FIFO 的批量验证作业队列。

    verifiers 为队列级默认 ZKVerifier 注册中心；enqueue 时也可为单个
    批次单独传入注册中心覆盖默认值。
    """

    def __init__(self, verifiers: ZKVerifier | None = None) -> None:
        self._default_verifiers = verifiers
        self._jobs: dict[str, _Job] = {}
        self._pending: deque[str] = deque()
        self._running_id: str | None = None
        self._ids = count(1)

    def enqueue(self, batch: Any, verifiers: ZKVerifier | None = None) -> str:
        """把一个批次入队，返回 job_id。

        批次内容在 run_next 执行时才校验与验证；这里只登记。
        """
        job_id = f"job-{next(self._ids)}"
        self._jobs[job_id] = _Job(
            job_id, batch, verifiers if verifiers is not None else self._default_verifiers
        )
        self._pending.append(job_id)
        return job_id

    def run_next(self) -> BatchVerificationResult | None:
        """执行队首的 queued 作业并返回其验证结果；队列空返回 None。

        串行保证：已有 running 作业时（例如验证器逻辑重入本队列）抛出
        RuntimeError，而不是并发执行第二个作业。
        """
        if self._running_id is not None:
            raise RuntimeError(
                f"another job is already running: {self._running_id!r}"
            )
        if not self._pending:
            return None

        job_id = self._pending.popleft()
        job = self._jobs[job_id]
        job.state = STATE_RUNNING
        self._running_id = job_id
        try:
            result = verify_batch(job.batch, job.verifiers)
        except BaseException:
            # fail-closed：异常原样传播，作业停留在 running，
            # 既不可取结果也不可取消。
            raise
        else:
            job.result = result
            job.state = STATE_COMPLETED
            return result
        finally:
            self._running_id = None

    def cancel(self, job_id: str) -> bool:
        """取消作业。仅 queued 可取消，返回 True；其余状态按序抛错。"""
        job = self._require_job(job_id)
        if job.state == STATE_QUEUED:
            self._pending.remove(job_id)
            job.state = STATE_CANCELLED
            return True
        if job.state == STATE_RUNNING:
            raise RunningJobError(job_id)
        if job.state == STATE_COMPLETED:
            raise CompletedJobError(job_id)
        if job.state == STATE_CANCELLED:
            raise CancelledJobError(job_id)
        # 状态集合封闭，不可达。
        raise RuntimeError(f"job {job_id!r} in unknown state {job.state!r}")

    def status(self, job_id: str) -> str:
        """返回作业当前状态；不存在抛 UnknownJobError。"""
        return self._require_job(job_id).state

    def result(self, job_id: str) -> BatchVerificationResult:
        """取 completed 作业的结果；非 completed 抛 ResultUnavailableError。"""
        job = self._require_job(job_id)
        if job.state != STATE_COMPLETED or job.result is None:
            raise ResultUnavailableError(job_id, job.state)
        return job.result

    def _require_job(self, job_id: str) -> _Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise UnknownJobError(job_id)
        return job
