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
* :meth:`report` 仅 completed 返回 BatchVerificationReport；它与
  :meth:`result` 来自同一次详细验证流水线执行
  （``report(job_id).result is result(job_id)``），查询报告不会再次调用
  验证器，也不改变作业状态。其余状态/不存在的异常约定与 :meth:`result`
  相同。
* :meth:`reverify_failures` 以已完成作业的失败清单为输入，只挑出原批次中
  失败的证明（保持相对顺序、batch_id 不变）创建新的 queued 作业，并记录
  父子关系（复核作业 -> 源作业）；重复复核得到不同 job_id，父子关系互不
  覆盖。源作业不存在抛 UnknownJobError；非 completed 抛
  ResultUnavailableError；completed 但无失败证明抛 NoFailedProofError。
* :meth:`reverify_outcome` 只读对账复核结果：比对源作业与其直接复核作业
  的结果，按源批次原序给出每个入选证明复核前后的定位与唯一结论
  （recovered / still_failed）。不调用验证器、不创建作业、不改变任何
  状态或结果，重复查询结果一致。

不持久化、不联网、不使用线程。
"""

from __future__ import annotations

import itertools
from collections import OrderedDict
from typing import Any, Dict, Optional

from .engine import verify_batch_detailed
from .errors import (
    CancelledJobError,
    CompletedJobError,
    NoFailedProofError,
    ResultUnavailableError,
    ReverifyLineageMismatchError,
    RunningJobError,
    UnknownJobError,
)
from .models import (
    OUTCOME_RECOVERED,
    OUTCOME_STILL_FAILED,
    REVERIFY_AFTER_FAILED,
    REVERIFY_AFTER_PASSED,
    REVERIFY_BEFORE_STATUS,
    BatchVerificationReport,
    BatchVerificationResult,
    ReverifyOutcomeItem,
    ReverifyOutcomeReport,
    VerificationJob,
)

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"


class VerificationQueue:
    """内存中的串行验证作业队列。"""

    def __init__(self, verifiers: Any = None):
        """:param verifiers: 作业执行时传给详细验证流水线的默认验证器集合。"""
        self._default_verifiers = verifiers
        self._jobs: "OrderedDict[str, VerificationJob]" = OrderedDict()
        # 每个作业的验证器覆盖；VerificationJob 不新增字段以免污染公开模型
        self._verifiers: Dict[str, Any] = {}
        # completed 作业的详细报告；与 job.result 来自同一次流水线执行
        self._reports: Dict[str, BatchVerificationReport] = {}
        # 复核父子关系：复核作业 id -> 源作业 id；重复复核各自记录、互不覆盖
        self._reverify_lineage: Dict[str, str] = {}
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

        作业执行时一次走完整的详细验证流水线
        （:func:`verify_batch_detailed`），普通结果与详细报告取自同一次
        执行：成功后状态置为 completed，:meth:`result` 返回报告内的
        result，:meth:`report` 返回整份报告。若验证本身抛异常
        （如 EmptyBatchError），异常向调用方传播，该作业从队列移除
        （四态状态机不为失败的输入保留悬挂作业）。
        """
        job = self._next_queued()
        if job is None:
            return None

        job.status = STATUS_RUNNING
        verifiers = self._verifiers.pop(job.job_id)
        try:
            report = verify_batch_detailed(job.batch, verifiers)
        except Exception:
            # 输入/验证错误不属于四个持久状态，移除以释放调用方重试。
            self._jobs.pop(job.job_id, None)
            self._reports.pop(job.job_id, None)
            self._reverify_lineage.pop(job.job_id, None)
            raise
        job.status = STATUS_COMPLETED
        job.result = report.result
        self._reports[job.job_id] = report
        return report.result

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

    def report(self, job_id: str) -> BatchVerificationReport:
        """仅 completed 作业可取详细报告。

        返回执行该作业时保存的 :class:`BatchVerificationReport`，与
        :meth:`result` 来自同一次详细验证流水线
        （``report(job_id).result is result(job_id)``）；本方法只读已保存
        的报告，不会再次调用验证器，也不改变作业状态。queued/running/
        cancelled 抛 ResultUnavailableError，不存在抛 UnknownJobError。
        """
        job = self._require_job(job_id)
        if job.status != STATUS_COMPLETED:
            raise ResultUnavailableError(
                f"job {job_id!r} has no report (status={job.status!r})"
            )
        return self._reports[job_id]

    # ---------------------------------------------------------------- 复核

    def reverify_failures(self, job_id: str, verifiers: Any = None) -> str:
        """只重验已完成作业结果中的失败证明，返回新作业 id。

        依据源作业保存的批次内容与失败清单，按 ``proof_id`` 挑出失败证明，
        并按源批次中的相对顺序组成子批次（``batch_id`` 不变、
        ``public_inputs``/``proof`` 原样保留），交给一个独立的 queued 作业；
        不重提整批，源作业状态与结果保持不变。

        ``verifiers`` 只作用于新作业，省略时使用队列默认验证器。新作业沿用
        run_next/cancel/status/result/report；执行时同样一次走完整的详细
        验证流水线并保存报告。创建时记录父子关系（新作业 -> 源作业），供
        :meth:`reverify_outcome` 校验血缘；重复复核得到不同 job_id，父子
        关系各自记录、互不覆盖。

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
        new_job_id = self.enqueue(sub_batch, verifiers)
        self._reverify_lineage[new_job_id] = job_id
        return new_job_id

    def reverify_outcome(
        self, source_job_id: str, retry_job_id: str
    ) -> ReverifyOutcomeReport:
        """只读对账 ``reverify_failures`` 的复核结果，返回
        :class:`ReverifyOutcomeReport`。

        对账范围严格沿用 ``reverify_failures`` 的选取（即复核作业实际
        验证的证明，按源批次原序）。每项给出复核前后定位与唯一结论：

        * ``before_status`` 固定 ``failed``，``before_*`` 取源作业保存的
          失败定位（同一 proof_id 多条记录时取首条）；
        * ``after_*`` 取复核作业的定位——恢复项 ``after_status`` 为
          ``passed`` 且 ``stage``/``code``/``message`` 为空，仍失败项取
          复核 :class:`Failure` 的同名字段；
        * ``outcome`` 只取 ``recovered`` / ``still_failed``。

        本方法只读已保存的结果，不调用验证器、不创建作业、不改变任何
        状态或结果，重复查询返回一致内容；报告只含定位信息，不含证明
        材料、``public_inputs``、调用栈或未公开验证器信息。

        * 任一作业不存在 ——UnknownJobError；
        * 任一作业未 completed ——ResultUnavailableError；
        * 均 completed 但 ``retry_job_id`` 不是 ``source_job_id`` 的直接
          复核作业 ——ReverifyLineageMismatchError。

        三类异常互不替代，按上述顺序依次检查。
        """
        source = self._require_job(source_job_id)
        retry = self._require_job(retry_job_id)
        for job in (source, retry):
            if job.status != STATUS_COMPLETED:
                raise ResultUnavailableError(
                    f"job {job.job_id!r} has no result to reconcile "
                    f"(status={job.status!r})"
                )
        if self._reverify_lineage.get(retry_job_id) != source_job_id:
            raise ReverifyLineageMismatchError(
                f"job {retry_job_id!r} is not a direct reverify_failures "
                f"job of {source_job_id!r}"
            )
        return _build_reverify_outcome(source, retry)

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


# ============================================================ 复核对账

def _first_failures_by_proof(result: BatchVerificationResult) -> Dict[str, Any]:
    """每个 proof_id 的首条失败定位（保持结果中的相对次序）。"""
    by_proof: Dict[str, Any] = {}
    for failure in result.failures:
        by_proof.setdefault(failure.proof_id, failure)
    return by_proof


def _build_reverify_outcome(
    source: VerificationJob, retry: VerificationJob
) -> ReverifyOutcomeReport:
    """比对源作业与其直接复核作业的结果，生成对账报告（纯只读）。

    明细范围即复核作业实际验证的证明（其批次就是按源批次原序选出的
    失败证明）；前后定位分别取自源作业与复核作业已保存的结果。
    """
    # 复核作业的批次即 reverify_failures 按源批次原序选出的失败证明。
    selected_ids = [raw["proof_id"] for raw in retry.batch["proofs"]]
    source_failures = _first_failures_by_proof(source.result)
    retry_failures = _first_failures_by_proof(retry.result)

    items = []
    recovered_ids = []
    still_failed_ids = []
    for proof_id in selected_ids:
        # 入选证明在源作业中必有失败定位（选取即来自源失败清单）。
        before = source_failures[proof_id]
        after = retry_failures.get(proof_id)
        if after is None:
            outcome = OUTCOME_RECOVERED
            after_status = REVERIFY_AFTER_PASSED
            after_stage = after_code = after_message = ""
            recovered_ids.append(proof_id)
        else:
            outcome = OUTCOME_STILL_FAILED
            after_status = REVERIFY_AFTER_FAILED
            after_stage = after.stage
            after_code = after.code
            after_message = after.message
            still_failed_ids.append(proof_id)
        items.append(
            ReverifyOutcomeItem(
                proof_id=proof_id,
                outcome=outcome,
                before_status=REVERIFY_BEFORE_STATUS,
                before_stage=before.stage,
                before_code=before.code,
                before_message=before.message,
                after_status=after_status,
                after_stage=after_stage,
                after_code=after_code,
                after_message=after_message,
            )
        )

    return ReverifyOutcomeReport(
        source_job_id=source.job_id,
        retry_job_id=retry.job_id,
        selected_proof_ids=selected_ids,
        items=items,
        recovered_proof_ids=recovered_ids,
        still_failed_proof_ids=still_failed_ids,
    )
