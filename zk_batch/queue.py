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
* :meth:`retry_by_error_code` 按失败定位结果筛选证明并定向重试：以
  ``batch_id``、匹配的失败错误码与可选 ``proof_ids`` 为输入，先确认批次
  与证明归属，再把符合条件的证明重新加入本队列（独立的 queued 作业），
  响应中逐个返回 proofId 被接受或被跳过（跳过原因为五种固定字面量之
  一）；之后仍由既有 ``result``/``report`` 给出新验证状态和失败定位。
  同一证明重复请求行为确定：已接受并处于待验证或验证中的记
  ``ALREADY_QUEUED``，已有最新成功结果的记 ``ALREADY_COMPLETED``，已有
  失败定位但不匹配本次错误码的记 ``NOT_RETRYABLE``。

不持久化、不联网、不使用线程。
"""

from __future__ import annotations

import itertools
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

from .engine import verify_batch_detailed
from .errors import (
    BatchNotFoundException,
    CancelledJobError,
    CompletedJobError,
    InvalidGroupSelectionError,
    InvalidRetrySelectionException,
    NoFailedProofError,
    ProofNotInBatchException,
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
    RETRY_SKIP_UNKNOWN_PROOF,
    REVERIFY_AFTER_FAILED,
    REVERIFY_AFTER_PASSED,
    REVERIFY_BEFORE_STATUS,
    BatchVerificationReport,
    BatchVerificationResult,
    GroupReverifyGroupItem,
    GroupReverifyOutcomeReport,
    GroupReverifyProofItem,
    GroupReverifySubmission,
    ProofRetrySkip,
    ProofRetrySubmission,
    ReverifyOutcomeItem,
    ReverifyOutcomeReport,
    VerificationJob,
)

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"

# 定向重试可匹配的失败错误码（即失败定位使用的全部 code）
_RETRY_ERROR_CODES = (CODE_REJECTED, CODE_VERIFY_ERROR)


class _BatchRecord:
    """批次台账（内部）：批次存在性、证明归属与最新逐证结果。

    ``proofs`` 登记首次出现的原始证明材料（保持批次原序，同一
    ``batch_id`` 的多次提交取并集）；``latest`` 记录每个证明最近一次
    completed 作业的结果——键缺失表示尚无结果，值为 ``None`` 表示
    通过，值为字符串表示失败定位的错误码。
    """

    def __init__(self) -> None:
        self.proofs: "OrderedDict[str, Any]" = OrderedDict()
        self.latest: Dict[str, Optional[str]] = {}


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
        # 按组复核的父子关系：复核作业 id -> (源作业 id, 请求的分组顺序)；
        # 与逐证复核谱系分开记录、互不替代。
        self._group_reverify_lineage: Dict[str, Tuple[str, Tuple[str, ...]]] = {}
        # 批次台账：batch_id -> _BatchRecord（定向重试的批次/归属/最新结果）
        self._batches: "OrderedDict[Any, _BatchRecord]" = OrderedDict()
        self._counter = itertools.count(1)

    # ------------------------------------------------------------ 入队/执行

    def enqueue(self, batch: Any, verifiers: Any = None) -> str:
        """把批次加入队列，返回作业 id。

        ``verifiers`` 省略时使用构造队列时给定的默认验证器。
        """
        self._register_batch(batch)
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
            self._group_reverify_lineage.pop(job.job_id, None)
            raise
        job.status = STATUS_COMPLETED
        job.result = report.result
        self._reports[job.job_id] = report
        self._record_completion(job)
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

    # ---------------------------------------------------- 按错误码定向重试

    def retry_by_error_code(
        self,
        batch_id: Any,
        error_code: Any,
        proof_ids: Any = None,
        verifiers: Any = None,
    ) -> ProofRetrySubmission:
        """按失败定位结果筛选证明并定向重试，返回
        :class:`ProofRetrySubmission`。

        以 ``batch_id``、匹配的失败错误码 ``error_code`` 与可选的
        ``proof_ids`` 为输入，先确认批次与证明归属，再把符合条件的证明
        重新加入本队列。不提供 ``proof_ids`` 时选择批次内所有匹配错误码
        且可重试的证明（按批次原序）；提供时只在请求的证明中筛选，
        ``accepted`` 与 ``skipped`` 均按输入 ``proof_ids`` 顺序返回，
        重复项直接报错而不静默去重。被接受的证明按接受顺序组成子批次
        （``batch_id`` 与证明材料原样保留）创建独立的 queued 作业，之后
        仍由既有 :meth:`result`/:meth:`report` 给出新验证状态和失败定位。

        响应逐个给出 proofId 被接受或被跳过，跳过原因只取
        ``UNKNOWN_PROOF``（标识在系统中未知）、``PROOF_NOT_IN_BATCH``
        （证明不属于该批次）、``NOT_RETRYABLE``（已有失败定位但不匹配
        本次错误码，或尚无失败定位）、``ALREADY_QUEUED``（已接受并处于
        待验证或验证中）、``ALREADY_COMPLETED``（已有最新成功结果）之
        一。同一证明重复请求行为确定：已接受并处于待验证或验证中的记
        ``ALREADY_QUEUED``，已有最新成功结果的记 ``ALREADY_COMPLETED``，
        已有失败定位但禁止重试的记 ``NOT_RETRYABLE``。筛选后没有可接受
        证明时请求不报错：``accepted`` 为空、``job_id`` 为 ``None``，
        ``skipped`` 逐条给出原因。

        本方法不改变已有聚合规则、队列顺序、成功与失败结果及单证语义；
        筛选只决定证明是否重新验证，不改写历史失败结果、错误码含义、
        批次归属或既有请求响应格式。``verifiers`` 只作用于新作业，省略
        时使用队列默认验证器。

        异常按以下顺序依次检查、互不替代；检出时不创建作业、不改变任何
        既有状态：

        * 批次不存在 ——BatchNotFoundException；
        * 空 ``proof_ids``、重复或非法的证明标识、无法识别的失败错误码
          ——InvalidRetrySelectionException；
        * 请求的证明已知归属其他批次 ——ProofNotInBatchException；
          完全未知的标识不是请求级错误，在响应中逐条记
          ``UNKNOWN_PROOF`` 跳过。
        """
        record = self._require_batch_record(batch_id)
        code = _validate_retry_error_code(error_code)
        requested = _validate_retry_proof_ids(proof_ids)

        if requested is not None:
            # 证明归属确认：已知归属其他批次的证明是请求级错误；完全未知
            # 的标识留给筛选阶段逐条记 UNKNOWN_PROOF 跳过。
            for proof_id in requested:
                if proof_id not in record.proofs and self._known_in_other_batch(
                    batch_id, proof_id
                ):
                    raise ProofNotInBatchException(batch_id, proof_id)

        queued_ids = self._queued_proof_ids(batch_id)
        accepted: List[str] = []
        skipped: List[ProofRetrySkip] = []

        if requested is None:
            # 全批次选择：批次内所有匹配错误码且可重试的证明，按批次原序；
            # 未显式请求的证明不进入 skipped。
            for proof_id in record.proofs:
                if (
                    proof_id not in queued_ids
                    and proof_id in record.latest
                    and record.latest[proof_id] == code
                ):
                    accepted.append(proof_id)
        else:
            for proof_id in requested:
                if proof_id not in record.proofs:
                    skipped.append(
                        ProofRetrySkip(proof_id, RETRY_SKIP_UNKNOWN_PROOF)
                    )
                elif proof_id in queued_ids:
                    skipped.append(
                        ProofRetrySkip(proof_id, RETRY_SKIP_ALREADY_QUEUED)
                    )
                elif proof_id not in record.latest:
                    skipped.append(
                        ProofRetrySkip(proof_id, RETRY_SKIP_NOT_RETRYABLE)
                    )
                elif record.latest[proof_id] is None:
                    skipped.append(
                        ProofRetrySkip(proof_id, RETRY_SKIP_ALREADY_COMPLETED)
                    )
                elif record.latest[proof_id] != code:
                    skipped.append(
                        ProofRetrySkip(proof_id, RETRY_SKIP_NOT_RETRYABLE)
                    )
                else:
                    accepted.append(proof_id)

        job_id = None
        if accepted:
            sub_batch = {
                "batch_id": batch_id,
                "proofs": [record.proofs[proof_id] for proof_id in accepted],
            }
            job_id = self.enqueue(sub_batch, verifiers)
        return ProofRetrySubmission(
            batch_id=batch_id,
            error_code=code,
            job_id=job_id,
            accepted=accepted,
            skipped=skipped,
        )

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

    # ------------------------------------------------------------ 批次台账

    def _require_batch_record(self, batch_id: Any) -> _BatchRecord:
        """定向重试的批次确认：批次不存在抛 BatchNotFoundException。"""
        try:
            record = self._batches.get(batch_id)
        except TypeError:
            # 不可哈希的 batch_id 不可能登记过，按批次不存在处理。
            record = None
        if record is None:
            raise BatchNotFoundException(batch_id)
        return record

    def _register_batch(self, batch: Any) -> None:
        """把入队批次登记到台账：批次存在性、证明归属与批次原序。

        同一 ``batch_id`` 的多次提交（原始批次与各类复核子批次）取并集：
        首次出现的证明材料与原序保持不变。入队不做批次校验（校验在执行
        时进行），无法辨认的批次内容不登记。
        """
        if not isinstance(batch, Mapping) or "batch_id" not in batch:
            return
        batch_id = batch["batch_id"]
        try:
            record = self._batches.get(batch_id)
        except TypeError:
            # 不可哈希的 batch_id 不入台账。
            return
        if record is None:
            record = _BatchRecord()
            self._batches[batch_id] = record
        proofs = batch.get("proofs")
        if not isinstance(proofs, (list, tuple)):
            return
        for raw in proofs:
            if not isinstance(raw, Mapping):
                continue
            proof_id = raw.get("proof_id")
            if (
                isinstance(proof_id, str)
                and proof_id
                and proof_id not in record.proofs
            ):
                record.proofs[proof_id] = raw

    def _record_completion(self, job: VerificationJob) -> None:
        """把 completed 作业的逐证结果写入批次台账（最新结果覆盖旧值）。

        只读本次执行的 ``job.result``，不调用验证器、不改变作业状态；
        历史失败结果与错误码含义保持不变。
        """
        batch = job.batch
        if not isinstance(batch, Mapping) or "batch_id" not in batch:
            return
        try:
            record = self._batches.get(batch["batch_id"])
        except TypeError:
            return
        if record is None or job.result is None:
            return
        failure_codes: Dict[str, str] = {}
        for failure in job.result.failures:
            failure_codes.setdefault(failure.proof_id, failure.code)
        proofs = batch.get("proofs")
        if not isinstance(proofs, (list, tuple)):
            return
        for raw in proofs:
            if not isinstance(raw, Mapping):
                continue
            proof_id = raw.get("proof_id")
            if not isinstance(proof_id, str) or not proof_id:
                continue
            # None 表示最新结果为通过；字符串为最新失败定位的错误码。
            record.latest[proof_id] = failure_codes.get(proof_id)

    def _queued_proof_ids(self, batch_id: Any) -> set:
        """该批次当前处于待验证（queued）或验证中（running）的证明标识。"""
        queued = set()
        for job in self._jobs.values():
            if job.status not in (STATUS_QUEUED, STATUS_RUNNING):
                continue
            batch = job.batch
            if not isinstance(batch, Mapping):
                continue
            if batch.get("batch_id") != batch_id:
                continue
            proofs = batch.get("proofs")
            if not isinstance(proofs, (list, tuple)):
                continue
            for raw in proofs:
                if isinstance(raw, Mapping):
                    proof_id = raw.get("proof_id")
                    if isinstance(proof_id, str) and proof_id:
                        queued.add(proof_id)
        return queued

    def _known_in_other_batch(self, batch_id: Any, proof_id: str) -> bool:
        """证明标识是否已登记在其他批次（归属确认）。"""
        for other_id, record in self._batches.items():
            if other_id != batch_id and proof_id in record.proofs:
                return True
        return False


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


# ========================================================== 定向重试辅助

def _validate_retry_error_code(error_code: Any) -> str:
    """校验失败错误码：只接受失败定位使用的公开 code。"""
    if not isinstance(error_code, str) or error_code not in _RETRY_ERROR_CODES:
        raise InvalidRetrySelectionException(
            f"unrecognized failure error code: {error_code!r}"
        )
    return error_code


def _validate_retry_proof_ids(proof_ids: Any) -> Optional[List[str]]:
    """校验定向重试的证明选择；``None`` 表示不筛选（全批次匹配）。

    提供时必须是非空列表/元组，元素为非空字符串且不重复——空
    ``proof_ids``、重复或非法元素都抛 InvalidRetrySelectionException，
    重复项直接报错而不静默去重。
    """
    if proof_ids is None:
        return None
    if not isinstance(proof_ids, (list, tuple)) or len(proof_ids) == 0:
        raise InvalidRetrySelectionException(
            "proof_ids must be a non-empty list or tuple of non-empty str"
        )
    seen = set()
    requested: List[str] = []
    for proof_id in proof_ids:
        if not isinstance(proof_id, str) or not proof_id:
            raise InvalidRetrySelectionException(
                f"proof id must be a non-empty str, got {proof_id!r}"
            )
        if proof_id in seen:
            raise InvalidRetrySelectionException(
                f"duplicate proof id: {proof_id!r}"
            )
        seen.add(proof_id)
        requested.append(proof_id)
    return requested


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
