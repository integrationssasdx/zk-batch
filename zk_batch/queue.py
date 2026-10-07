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
* :meth:`reverify_groups` 以已完成作业失败定位中的聚合组为输入，按源批次
  序取出选中组内的全部证明（保留 batch_id、材料、分组键与组内序）创建新
  的 queued 作业，并记录独立的按组复核父子关系；重复复核得到不同
  job_id，与 :meth:`reverify_failures` 的谱系互不覆盖。异常顺序：源作业
  不存在 UnknownJobError、未 completed ResultUnavailableError、选择非法
  或重复 InvalidGroupSelectionError、合法分组不属源失败定位
  UnknownFailedGroupError；失败时不建作业、不改源作业。
* :meth:`group_reverify_outcome` 只读对账按组复核结果：按请求输入的分组
  顺序，逐组给出双方组级状态、每证前后状态与唯一结论（源组失败且复核整
  组无失败 recovered，否则 still_failed）。不调用验证器、不创建作业、不
  改变任何状态或结果，重复查询一致；谱系必须是直接的 ``reverify_groups``
  父子关系，与逐证复核谱系互不替代。
* :meth:`retry_failed_proofs` 按失败定位结果筛选证明并定向重试：以原批次
  标识、匹配的失败错误码与可选 proofIds 为输入，先确认批次和证明归属，
  再把当前仍失败且错误码匹配、可重试的证明重新加入本队列（独立的
  queued 子任务）；响应逐个给出 proofId 被接受或被跳过（唯一跳过原因）。
  重复请求行为确定：已接受且待验证/验证中的记 ``already_queued``，已有
  最新成功结果的记 ``already_completed``，有失败定位但不可重试的记
  ``not_retryable``。新作业的验证状态与失败定位仍由既有
  :meth:`result`/:meth:`report` 查询给出。

不持久化、不联网、不使用线程。
"""

from __future__ import annotations

import itertools
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

from .engine import verify_batch_detailed
from .errors import (
    BatchNotFoundError,
    CancelledJobError,
    CompletedJobError,
    InvalidGroupSelectionError,
    InvalidRetrySelectionError,
    NoFailedProofError,
    ProofNotInBatchError,
    ResultUnavailableError,
    GroupReverifyLineageMismatchError,
    ReverifyLineageMismatchError,
    RunningJobError,
    UnknownFailedGroupError,
    UnknownJobError,
)
from .models import (
    CODE_REJECTED,
    CODE_VERIFY_ERROR,
    OUTCOME_RECOVERED,
    OUTCOME_STILL_FAILED,
    RETRY_SKIP_ALREADY_COMPLETED,
    RETRY_SKIP_ALREADY_QUEUED,
    RETRY_SKIP_NOT_RETRYABLE,
    REVERIFY_AFTER_FAILED,
    REVERIFY_AFTER_PASSED,
    REVERIFY_BEFORE_STATUS,
    BatchVerificationReport,
    BatchVerificationResult,
    GroupReverifyGroupItem,
    GroupReverifyOutcomeReport,
    GroupReverifyProofItem,
    GroupReverifySubmission,
    ProofRetryResponse,
    ProofRetrySkipItem,
    ReverifyOutcomeItem,
    ReverifyOutcomeReport,
    VerificationJob,
)

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"

# 定向重试可识别的失败错误码（沿用 Failure.code 口径）
RETRYABLE_FAILURE_CODES = (CODE_REJECTED, CODE_VERIFY_ERROR)

# 定向重试中逐证明的内部状态
_PROOF_PASSED = "passed"  # 最新一次验证通过
_PROOF_FAILED = "failed"  # 最新一次验证失败（可按错误码筛选重试）
_PROOF_QUEUED = "queued"  # 已被重试请求接受，子任务待验证或验证中


class VerificationQueue:
    """内存中的串行验证作业队列。"""

    def __init__(self, verifiers: Any = None):
        """:param verifiers: 作业执行时传给详细验证流水线的默认验证器集合。"""
        self._default_verifiers = verifiers
        self._jobs: "OrderedDict[str, VerificationJob]" = OrderedDict()
        # 每个作业的验证器覆盖；VerificationJob 不新增字段以免污染公开模型
        self._verifiers: Dict[str, Any] = {}
        # 作业进入验证时的配置快照（验证器选择），定向重试子任务沿用
        self._config_snapshots: Dict[str, Any] = {}
        # completed 作业的详细报告；与 job.result 来自同一次流水线执行
        self._reports: Dict[str, BatchVerificationReport] = {}
        # 复核父子关系：复核作业 id -> 源作业 id；重复复核各自记录、互不覆盖
        self._reverify_lineage: Dict[str, str] = {}
        # 按组复核的父子关系：复核作业 id -> (源作业 id, 请求的分组顺序)；
        # 与逐证复核谱系分开记录、互不替代。
        self._group_reverify_lineage: Dict[str, Tuple[str, Tuple[str, ...]]] = {}
        # 定向重试：原批次（根）作业 id -> {proof_id: {"status", "code"}}，
        # 首次处理该批次的重试请求时按源作业结果惰性建立。
        self._proof_retry_states: Dict[str, Dict[str, Dict[str, str]]] = {}
        # 定向重试子任务作业 id -> 原批次作业 id（直接父子关系）。
        self._proof_retry_lineage: Dict[str, str] = {}
        # 定向重试子任务作业 id -> {proof_id: 接受前的失败错误码}，
        # 子任务完成时归并状态，取消/执行失败时按此回退。
        self._proof_retry_selection: Dict[str, Dict[str, str]] = {}
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
        # 配置快照：作业进入验证时的验证器选择，定向重试子任务沿用。
        self._config_snapshots[job_id] = self._verifiers[job_id]
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
        retry_parent = self._proof_retry_lineage.get(job.job_id)
        try:
            report = verify_batch_detailed(job.batch, verifiers)
        except Exception:
            # 输入/验证错误不属于四个持久状态，移除以释放调用方重试。
            if retry_parent is not None:
                # 定向重试子任务执行失败：入选证明回到接受前的失败状态。
                self._revert_proof_retry(retry_parent, job.job_id)
            self._remove_job(job.job_id)
            raise
        job.status = STATUS_COMPLETED
        job.result = report.result
        self._reports[job.job_id] = report
        if retry_parent is not None:
            # 定向重试子任务完成：把逐证最新状态归并回原批次。
            self._merge_proof_retry(retry_parent, job)
        return report.result

    def _remove_job(self, job_id: str) -> None:
        """作业执行抛错后的统一清理（四态状态机不保留悬挂作业）。"""
        self._jobs.pop(job_id, None)
        self._reports.pop(job_id, None)
        self._verifiers.pop(job_id, None)
        self._config_snapshots.pop(job_id, None)
        self._reverify_lineage.pop(job_id, None)
        self._group_reverify_lineage.pop(job_id, None)
        self._proof_retry_lineage.pop(job_id, None)
        self._proof_retry_selection.pop(job_id, None)

    def cancel(self, job_id: str) -> bool:
        """取消 queued 作业并返回 ``True``；其余状态按异常约定抛出。

        取消的若是定向重试子任务：入选证明回到接受前的失败状态（不视为
        已重试），原批次汇总随之恢复。
        """
        job = self._require_job(job_id)
        if job.status == STATUS_QUEUED:
            retry_parent = self._proof_retry_lineage.get(job_id)
            if retry_parent is not None:
                self._revert_proof_retry(retry_parent, job_id)
            job.status = STATUS_CANCELLED
            self._verifiers.pop(job_id, None)
            self._config_snapshots.pop(job_id, None)
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

    # ------------------------------------------------------------ 按组复核

    def reverify_groups(
        self,
        job_id: str,
        group_ids: Any,
        verifiers: Any = None,
    ) -> GroupReverifySubmission:
        """按聚合组重验已完成作业，返回 :class:`GroupReverifySubmission`。

        ``group_ids`` 必须是非空的列表或元组，元素为非空字符串且互不
        重复；每个标识都必须出现在源作业结果的失败分组中。选证按源批次
        序进行：先按源批次中分组首次出现的先后确定组序（请求顺序只影响
        回执清单，不改变批次内证明的相对顺序），再把每个选中组的全部
        证明按组内批次原序纳入子批次（``batch_id`` 不变、
        ``public_inputs``/``proof`` 原样保留、分组键不变）；新作业是独立
        的 queued 作业，执行时同样一次走完整的详细验证流水线。

        ``verifiers`` 只作用于新作业，省略时使用队列默认验证器。创建时
        记录按组复核的父子关系（新作业 -> 源作业），与
        :meth:`reverify_failures` 的谱系分开；重复复核得到不同 job_id，
        父子关系各自记录、互不覆盖。任何校验失败都不创建作业，源作业的
        状态、批次与结果保持不变。

        异常按以下顺序依次检查、互不替代：

        * 源作业不存在 ——UnknownJobError；
        * 存在但非 completed ——ResultUnavailableError；
        * ``group_ids`` 不是非空列表/元组、元素不是非空字符串或有重复
          ——InvalidGroupSelectionError；
        * 选择合法但存在不属于源作业失败定位的分组
          ——UnknownFailedGroupError。
        """
        job = self._require_job(job_id)
        if job.status != STATUS_COMPLETED:
            raise ResultUnavailableError(
                f"job {job_id!r} has no result to reverify "
                f"(status={job.status!r})"
            )
        _validate_group_selection(group_ids)

        # 源作业失败定位中实际出现的分组标识（与 failure_groups 口径一致）。
        failed_group_ids = {
            failure.group_id for failure in job.result.failures
        }
        unknown = [gid for gid in group_ids if gid not in failed_group_ids]
        if unknown:
            raise UnknownFailedGroupError(
                f"job {job_id!r} has no failed group {unknown[0]!r}"
            )

        # 按源批次序取组内全部证明：组序取源批次中的首次出现序，
        # 组内保持源批次原序；证明对象原样保留（材料、分组键、组内序）。
        selected_group_set = set(group_ids)
        selected = [
            raw
            for raw in job.batch["proofs"]
            if _raw_group_id(raw) in selected_group_set
        ]
        sub_batch = {
            "batch_id": job.result.batch_id,
            "proofs": selected,
        }
        new_job_id = self.enqueue(sub_batch, verifiers)
        # 谱系同时保存请求的分组输入顺序，供 group_reverify_outcome
        # 按输入序排列 groups（批次内证明顺序不等于请求顺序）。
        self._group_reverify_lineage[new_job_id] = (
            job_id,
            tuple(group_ids),
        )
        return GroupReverifySubmission(
            source_job_id=job_id,
            job_id=new_job_id,
            status=STATUS_QUEUED,
            selected_group_ids=list(group_ids),
            selected_proof_ids=[raw["proof_id"] for raw in selected],
        )

    def group_reverify_outcome(
        self, source_job_id: str, retry_job_id: str
    ) -> GroupReverifyOutcomeReport:
        """只读对账 ``reverify_groups`` 的复核结果，返回
        :class:`GroupReverifyOutcomeReport`。

        对账范围严格沿用 ``reverify_groups`` 的选取（即复核作业实际验证
        的分组与证明）；``groups`` 按请求输入的分组顺序排列，组内证明按
        源批次原序。每组给出组级前后状态
        （``aggregate_call_status``/``aggregate_verify_status``/
        ``fell_back``）与每证前后 ``status``/``message``。源组失败且复核
        后整组无失败证明记 ``recovered``，否则记 ``still_failed``。

        本方法只读已保存的报告，不调用验证器、不创建作业、不改变任何
        状态或结果，重复查询返回一致内容；报告只含定位信息，不含证明
        材料、``public_inputs``、调用栈或未公开验证器信息。

        * 任一作业不存在 ——UnknownJobError；
        * 任一作业未 completed ——ResultUnavailableError；
        * 均 completed 但 ``retry_job_id`` 不是 ``source_job_id`` 的直接
          ``reverify_groups`` 作业
          ——GroupReverifyLineageMismatchError。

        三类异常互不替代，按上述顺序依次检查；按组谱系与逐证复核谱系
        （:meth:`reverify_outcome`）互不替代。
        """
        source = self._require_job(source_job_id)
        retry = self._require_job(retry_job_id)
        for job in (source, retry):
            if job.status != STATUS_COMPLETED:
                raise ResultUnavailableError(
                    f"job {job.job_id!r} has no result to reconcile "
                    f"(status={job.status!r})"
                )
        lineage = self._group_reverify_lineage.get(retry_job_id)
        if lineage is None or lineage[0] != source_job_id:
            raise GroupReverifyLineageMismatchError(
                f"job {retry_job_id!r} is not a direct reverify_groups "
                f"job of {source_job_id!r}"
            )
        requested_group_ids = lineage[1]
        return _build_group_reverify_outcome(
            self._reports[source_job_id],
            self._reports[retry_job_id],
            requested_group_ids,
            source_job_id,
            retry_job_id,
            source.status,
            retry.status,
        )

    # -------------------------------------------------------- 定向重试

    def retry_failed_proofs(
        self,
        batch_id: Any,
        code: Any = None,
        proof_ids: Any = None,
        verifiers: Any = None,
        error_code: Any = None,
        failure_code: Any = None,
    ) -> ProofRetryResponse:
        """按失败定位结果筛选证明并定向重试，返回
        :class:`ProofRetryResponse`。

        输入为原批次标识 ``batch_id``、匹配的失败错误码 ``code``
        （``error_code``/``failure_code`` 为同义关键字别名）与可选的
        ``proof_ids``：不提供 ``proof_ids`` 时选择批次内所有匹配错误码
        且可重试的证明，提供时只在这些证明中筛选。系统先确认批次和证明
        归属，再把符合条件的证明按原批次原序组成子批次（``batch_id``、
        证明材料与分组键原样保留），作为独立的 queued 作业重新加入本
        队列；新作业的验证状态与失败定位仍由 :meth:`result`/
        :meth:`report` 给出。

        响应逐个给出 proofId 被接受（``accepted``）或被跳过
        （``skipped``，唯一跳过原因）。同一证明重复请求行为确定：已被
        接受且处于待验证或验证中的记 ``already_queued``，已有最新成功
        结果的记 ``already_completed``，已有失败定位但错误码不匹配
        （禁止重试）的记 ``not_retryable``。筛选后没有可接受证明时请求
        不报错：``accepted`` 为空、不创建作业（``job_id``/``status``
        为 ``None``），``skipped`` 逐条给出原因。所有集合输出按输入
        ``proof_ids`` 的顺序返回（未提供时按批次原序），重复项直接报
        错而不静默去重。

        ``verifiers`` 只作用于新作业，省略时沿用原批次进入验证时的
        配置快照。校验按以下顺序依次进行，任一失败都不创建作业、不改
        变原批次的状态或结果：

        * 批次不存在（没有以 ``batch_id`` 完成验证的原批次）
          —— :class:`BatchNotFoundError`；
        * ``proof_ids`` 为空、元素不是非空字符串或同一请求内重复
          —— :class:`InvalidRetrySelectionError`；
        * ``proof_ids`` 含不属于 ``batch_id`` 的证明
          —— :class:`ProofNotInBatchError`；
        * 失败错误码无法识别（不在公开失败码口径内）
          —— :class:`InvalidRetrySelectionError`。
        """
        if code is None:
            code = error_code if error_code is not None else failure_code
        root = self._find_batch_job(batch_id)
        if root is None or root.status != STATUS_COMPLETED:
            raise BatchNotFoundError(f"unknown batch: {batch_id!r}")

        state = self._proof_retry_states.get(root.job_id)
        if state is None:
            state = self._init_proof_retry_state(root)
            self._proof_retry_states[root.job_id] = state

        if proof_ids is None:
            candidates = [
                pid
                for pid, info in state.items()
                if info["status"] != _PROOF_PASSED and info["code"] == code
            ] if isinstance(code, str) else []
        else:
            if not isinstance(proof_ids, (list, tuple)) or len(proof_ids) == 0:
                raise InvalidRetrySelectionError(
                    "proof_ids must be a non-empty list or tuple of proof ids"
                )
            seen = set()
            for pid in proof_ids:
                if not isinstance(pid, str) or not pid:
                    raise InvalidRetrySelectionError(
                        f"proof id must be a non-empty str, got {pid!r}"
                    )
                if pid in seen:
                    raise InvalidRetrySelectionError(
                        f"duplicate proof id in retry request: {pid!r}"
                    )
                seen.add(pid)
            for pid in proof_ids:
                if pid not in state:
                    raise ProofNotInBatchError(
                        f"proof id {pid!r} does not belong to batch "
                        f"{batch_id!r}"
                    )
            candidates = list(proof_ids)

        if not isinstance(code, str) or code not in RETRYABLE_FAILURE_CODES:
            raise InvalidRetrySelectionError(
                f"unrecognized failure code: {code!r}"
            )

        accepted: List[str] = []
        skipped: List[ProofRetrySkipItem] = []
        for pid in candidates:
            info = state[pid]
            if info["status"] == _PROOF_QUEUED:
                skipped.append(
                    ProofRetrySkipItem(pid, RETRY_SKIP_ALREADY_QUEUED)
                )
            elif info["status"] == _PROOF_PASSED:
                skipped.append(
                    ProofRetrySkipItem(pid, RETRY_SKIP_ALREADY_COMPLETED)
                )
            elif info["code"] != code:
                skipped.append(
                    ProofRetrySkipItem(pid, RETRY_SKIP_NOT_RETRYABLE)
                )
            else:
                accepted.append(pid)

        new_job_id: Optional[str] = None
        status: Optional[str] = None
        if accepted:
            # 子批次按原批次原序选证：材料、分组键、batch_id 原样保留。
            accepted_set = set(accepted)
            selected = [
                raw
                for raw in root.batch["proofs"]
                if raw["proof_id"] in accepted_set
            ]
            sub_batch = {
                "batch_id": root.result.batch_id,
                "proofs": selected,
            }
            chosen_verifiers = (
                verifiers
                if verifiers is not None
                else self._config_snapshots.get(root.job_id)
            )
            new_job_id = self.enqueue(sub_batch, chosen_verifiers)
            self._proof_retry_lineage[new_job_id] = root.job_id
            self._proof_retry_selection[new_job_id] = {
                pid: state[pid]["code"] for pid in accepted
            }
            for pid in accepted:
                state[pid] = {"status": _PROOF_QUEUED, "code": state[pid]["code"]}
            status = STATUS_QUEUED

        return ProofRetryResponse(
            batch_id=root.result.batch_id,
            source_job_id=root.job_id,
            job_id=new_job_id,
            status=status,
            code=code,
            accepted=accepted,
            skipped=skipped,
        )

    # 同语义别名：按失败码筛选的定向重试在不同文档中的命名。
    retry_proofs = retry_failed_proofs
    retry_proofs_by_code = retry_failed_proofs
    retry_proofs_by_failure_code = retry_failed_proofs
    retry_failures_by_code = retry_failed_proofs
    retry_by_failure_code = retry_failed_proofs
    retry_selected_proofs = retry_failed_proofs
    selective_retry_proofs = retry_failed_proofs

    # ------------------------------------------------------------ 重试内部

    def _find_batch_job(self, batch_id: Any) -> Optional[VerificationJob]:
        """按批次标识找到原批次（普通入队）作业。

        复核与定向重试产生的子任务沿用原批次 ``batch_id``，不作为独立
        批次参与查找；取最早入队的同标识普通作业（同标识批次按单例语
        义处理）。
        """
        child_ids = (
            set(self._reverify_lineage)
            | set(self._group_reverify_lineage)
            | set(self._proof_retry_lineage)
        )
        for job in self._jobs.values():
            if job.job_id in child_ids:
                continue
            batch = job.batch
            if isinstance(batch, Mapping) and batch.get("batch_id") == batch_id:
                return job
        return None

    def _init_proof_retry_state(
        self, root: VerificationJob
    ) -> Dict[str, Dict[str, str]]:
        """按原批次已保存的结果建立逐证明当前状态（批次原序）。"""
        failed: Dict[str, str] = {}
        for failure in root.result.failures:
            failed.setdefault(failure.proof_id, failure.code)
        state: Dict[str, Dict[str, str]] = {}
        for raw in root.batch["proofs"]:
            proof_id = raw["proof_id"]
            code = failed.get(proof_id)
            if code is None:
                state[proof_id] = {"status": _PROOF_PASSED, "code": ""}
            else:
                state[proof_id] = {"status": _PROOF_FAILED, "code": code}
        return state

    def _merge_proof_retry(self, root_id: str, job: VerificationJob) -> None:
        """定向重试子任务完成：把入选证明的最新状态归并回原批次。"""
        state = self._proof_retry_states.get(root_id)
        selected = self._proof_retry_selection.pop(job.job_id, {})
        if state is None:
            return
        failed: Dict[str, str] = {}
        for failure in job.result.failures:
            failed.setdefault(failure.proof_id, failure.code)
        for proof_id in selected:
            code = failed.get(proof_id)
            if code is None:
                state[proof_id] = {"status": _PROOF_PASSED, "code": ""}
            else:
                state[proof_id] = {"status": _PROOF_FAILED, "code": code}

    def _revert_proof_retry(self, root_id: str, job_id: str) -> None:
        """定向重试子任务被取消或执行失败：入选证明回到接受前的失败
        状态（不视为已重试）。"""
        state = self._proof_retry_states.get(root_id)
        selected = self._proof_retry_selection.pop(job_id, {})
        if state is None:
            return
        for proof_id, old_code in selected.items():
            state[proof_id] = {"status": _PROOF_FAILED, "code": old_code}

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


# ========================================================== 按组复核辅助

def _raw_group_id(raw: Any) -> str:
    """源批次中原始证明映射的分组标识（与引擎分组键同口径）。

    能进入这里的源批次都已完整跑过详细流水线，三个分组字段必然存在且
    为非空字符串。
    """
    return f"{raw['protocol']}:{raw['circuit_id']}:{raw['aggregation_key']}"


def _validate_group_selection(group_ids: Any) -> None:
    """校验分组选择：非空列表/元组、元素为非空字符串且不重复。"""
    if not isinstance(group_ids, (list, tuple)) or len(group_ids) == 0:
        raise InvalidGroupSelectionError(
            "group_ids must be a non-empty list or tuple of non-empty str"
        )
    seen = set()
    for gid in group_ids:
        if not isinstance(gid, str) or not gid:
            raise InvalidGroupSelectionError(
                f"group id must be a non-empty str, got {gid!r}"
            )
        if gid in seen:
            raise InvalidGroupSelectionError(f"duplicate group id: {gid!r}")
        seen.add(gid)


def _build_group_reverify_outcome(
    source_report: BatchVerificationReport,
    retry_report: BatchVerificationReport,
    requested_group_ids: Tuple[str, ...],
    source_job_id: str,
    retry_job_id: str,
    source_status: str,
    retry_status: str,
) -> GroupReverifyOutcomeReport:
    """比对源作业与其直接按组复核作业的报告，生成组级对账报告（纯只读）。

    明细范围即复核作业实际验证的分组（其批次就是按源批次序选出的整组
    证明）；``groups`` 按请求输入的分组顺序排列，组内证明保持源批次原序。
    组级与逐证前后定位分别取自双方已保存的详细报告；唯一结论以复核结果
    的失败定位为准（整组无失败证明才记 recovered）。
    """
    source_groups = {g.group_id: g for g in source_report.groups}
    retry_groups = {g.group_id: g for g in retry_report.groups}
    # 复核结果中仍有失败定位的分组（权威失败口径，含单证全过但整组聚合
    # 验证失败的组级归责情形）。
    retry_failed_groups = {
        failure.group_id for failure in retry_report.result.failures
    }

    items: List[GroupReverifyGroupItem] = []
    recovered_ids: List[str] = []
    still_failed_ids: List[str] = []
    for group_id in requested_group_ids:
        before_group = source_groups[group_id]
        after_group = retry_groups[group_id]
        before_details = {d.proof_id: d for d in before_group.proofs}
        after_details = {d.proof_id: d for d in after_group.proofs}

        # 组内证明以复核批次为准：即按源批次原序选出的整组证明。
        proof_items = []
        for proof_id in after_group.proof_ids:
            before = before_details[proof_id]
            after = after_details[proof_id]
            proof_items.append(
                GroupReverifyProofItem(
                    proof_id=proof_id,
                    before_status=before.status,
                    before_message=before.message,
                    after_status=after.status,
                    after_message=after.message,
                )
            )

        if group_id in retry_failed_groups:
            outcome = OUTCOME_STILL_FAILED
            still_failed_ids.append(group_id)
        else:
            outcome = OUTCOME_RECOVERED
            recovered_ids.append(group_id)

        items.append(
            GroupReverifyGroupItem(
                group_id=group_id,
                proof_ids=list(after_group.proof_ids),
                outcome=outcome,
                before_aggregate_call_status=
                    before_group.aggregate_call_status,
                before_aggregate_verify_status=
                    before_group.aggregate_verify_status,
                before_fell_back=before_group.fell_back,
                after_aggregate_call_status=
                    after_group.aggregate_call_status,
                after_aggregate_verify_status=
                    after_group.aggregate_verify_status,
                after_fell_back=after_group.fell_back,
                proofs=proof_items,
            )
        )

    return GroupReverifyOutcomeReport(
        source_job_id=source_job_id,
        retry_job_id=retry_job_id,
        source_status=source_status,
        retry_status=retry_status,
        groups=items,
        recovered_group_ids=recovered_ids,
        still_failed_group_ids=still_failed_ids,
        recovered_count=len(recovered_ids),
        still_failed_count=len(still_failed_ids),
    )
