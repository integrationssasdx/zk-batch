"""可排队、可定位失败原因的逐项批量验证。

与基于分组聚合的 :class:`~zk_batch.queue.VerificationQueue` 相互独立：
本模块的输入是一组按业务顺序排列的验证项（``item_id`` + 证明材料），
提交前一次完成完整输入校验，通过后进入独立的内存队列并按输入顺序逐项
验证，单证明失败不中止同批其他项。

状态机（仅四个状态）：

    queued -> processing -> completed
                          \\-> failed（基础设施错误；保留此前已完成项）

* :meth:`VerificationTaskQueue.submit` 完成提交前校验并入队，返回
  :class:`TaskSubmission`（稳定任务标识、``queued`` 状态与批次摘要）。
  校验失败（空批次/标识非法/超上限/材料格式不符/重复标识）直接抛出对应
  异常，不生成任务标识、不进入队列。
* :meth:`run_next` 同步消费最早的 queued 任务；:meth:`run_task` 消费指定
  任务。任务仅会被消费一次：处理中重复消费、终态后再次入队/消费均抛
  :class:`TaskStateConflictError`。
* :meth:`retry_failed` 从 completed/failed 源任务挑选待复核项，按根任务
  输入顺序创建独立的 queued 任务；新任务沿用同样的执行、进度、结果与错误
  查询，不修改源任务。
* 处理过程中 :meth:`progress` 返回真实完成进度（``completed`` 为已结束
  项数），未结束时 ``result`` 为 ``None``，不提前给出最终结果。
* 系统无法读取证明材料、验证器执行失败或无法保存最终结果时抛
  :class:`VerificationInfrastructureError`，任务进入 ``failed`` 终态，
  此前已完成项的结果与定位信息完整保留；终态结果查询幂等。

不持久化、不联网、不使用线程。失败定位只含输入序号、项标识、失败阶段、
稳定错误码与供人工定位的描述，绝不包含完整证明材料、内部调用栈或未公开
验证器信息。
"""

from __future__ import annotations

import itertools
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any, Callable, Dict, List, Optional

from .engine import (
    _REQUIRED_FIELDS,
    _exc_message,
    _require_bool,
    _require_method,
    _resolve_verifiers,
)
from .errors import (
    BatchSizeLimitError,
    DuplicateItemIdError,
    EmptyBatchError,
    InvalidItemIdError,
    InvalidProofFormatError,
    NoRetryableItemsError,
    TaskNotFoundError,
    TaskStateConflictError,
    VerificationInfrastructureError,
    VerifierContractError,
)
from .models import (
    ITEM_CODE_INVALID_PROOF,
    ITEM_CODE_REJECTED,
    ITEM_CODE_SAVE_FAULT,
    ITEM_CODE_VERIFIER_FAULT,
    ITEM_CODE_VERIFIER_UNAVAILABLE,
    ITEM_FAILED,
    ITEM_PASSED,
    ITEM_STAGE_PROOF_READ,
    ITEM_STAGE_VERIFY,
    MAX_BATCH_ITEMS,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PROCESSING,
    TASK_QUEUED,
    TASK_TERMINAL_STATUSES,
    BatchSummary,
    BatchTaskResult,
    ItemResult,
    Proof,
    TaskProgress,
    TaskRetrySubmission,
    TaskSubmission,
)

# 保存最终结果阶段（任务级，无具体项下标）
STAGE_RESULT_SAVE = "result_save"

# 供人工定位的固定描述（不随异常内部文本变化，避免泄露内部信息）
MSG_REJECTED = "proof rejected by verifier"
MSG_VERIFIER_UNAVAILABLE = "no verifier registered for protocol {protocol!r}"
MSG_VERIFIER_FAULT = "verifier execution failed: {detail}"
MSG_PROOF_READ = "cannot read proof material: {detail}"
MSG_SAVE_FAULT = "cannot persist final result: {detail}"


class _ProofReadError(Exception):
    """执行期读取/归一化证明材料失败（内部信号）。"""


class _TaskRecord:
    """队列内部的任务记录（不对外暴露）。"""

    def __init__(
        self,
        task_id: str,
        items: List[dict],
        verifiers: Any,
        root_indexes: Optional[List[int]] = None,
    ):
        self.task_id = task_id
        self.items = items
        self.verifiers = verifiers
        # 各项在根任务（最初提交）输入中的零起下标；普通提交时即自身下标，
        # 复核任务按入选项映射，保证 ItemResult.index 始终回溯到根任务。
        self.root_indexes = (
            list(range(len(items))) if root_indexes is None else list(root_indexes)
        )
        self.status: str = TASK_QUEUED
        self.total: int = len(items)
        self.results: List[ItemResult] = []
        self.result: Optional[BatchTaskResult] = None
        self.error: Optional[VerificationInfrastructureError] = None


class VerificationTaskQueue:
    """内存中的逐项验证任务队列（串行、同步、无后台线程）。"""

    def __init__(
        self,
        verifiers: Any = None,
        result_store: Optional[Callable[[BatchTaskResult], None]] = None,
    ):
        """:param verifiers: 任务执行时使用的默认验证器集合（单个/列表/
            ``{protocol: verifier}``，与 :func:`verify_batch` 同源解析）。
        :param result_store: 可选的最终结果保存钩子；任务正常完成时调用，
            钩子抛异常按"无法保存最终结果"处理为
            :class:`VerificationInfrastructureError`。无论钩子成败，结果
            都会保留在队列记录中供幂等查询。
        """
        self._default_verifiers = verifiers
        self._result_store = result_store
        self._tasks: "OrderedDict[str, _TaskRecord]" = OrderedDict()
        self._counter = itertools.count(1)
        # 正在被消费的任务标识；串行队列同一时刻至多一个。
        self._current: Optional[str] = None

    # --------------------------------------------------------------- 提交

    def submit(self, items: Any, verifiers: Any = None) -> TaskSubmission:
        """完整校验一批验证项并入队，返回 :class:`TaskSubmission`。

        ``items`` 为非空的验证项序列，每项是含 ``item_id``（非空字符串）
        与 ``proof``（非空映射的证明材料）的映射。校验全部通过后才生成
        稳定任务标识并入队；任一校验失败都不会留下队列记录。

        校验优先级（靠前的错误先抛出）：
        空批次 :class:`EmptyBatchError` -> 超上限
        :class:`BatchSizeLimitError` -> 按输入顺序逐项检查标识
        :class:`InvalidItemIdError` 与材料格式
        :class:`InvalidProofFormatError` -> 重复标识
        :class:`DuplicateItemIdError`。

        ``verifiers`` 省略时使用构造队列时给定的默认验证器。
        """
        normalized = _validate_items(items)
        item_ids = [item["item_id"] for item in normalized]

        chosen = verifiers if verifiers is not None else self._default_verifiers
        task_id = f"task-{next(self._counter)}"
        self._tasks[task_id] = _TaskRecord(task_id, normalized, chosen)
        return TaskSubmission(
            task_id=task_id,
            status=TASK_QUEUED,
            summary=BatchSummary(total=len(normalized), item_ids=item_ids),
        )

    # --------------------------------------------------------------- 消费

    def run_next(self) -> Optional[BatchTaskResult]:
        """消费最早的 queued 任务；没有可消费任务时返回 ``None``。

        处理中（如验证器回调内）再次调用本方法或 :meth:`run_task` 属于
        重复消费/状态冲突，抛 :class:`TaskStateConflictError`。
        基础设施错误以 :class:`VerificationInfrastructureError` 传播，
        任务保留为 ``failed`` 终态且已完成项结果可查。
        """
        self._ensure_no_active_consumer()
        task_id = None
        for record in self._tasks.values():
            if record.status == TASK_QUEUED:
                task_id = record.task_id
                break
        if task_id is None:
            return None
        return self._consume(task_id)

    def run_task(self, task_id: str) -> BatchTaskResult:
        """消费指定任务并返回最终结果。

        任务不存在抛 :class:`TaskNotFoundError`；任务不处于 queued
        （处理中或已终态）抛 :class:`TaskStateConflictError`——终态后
        再次入队/消费、重复消费统一走该异常。
        """
        self._ensure_no_active_consumer()
        record = self._require_task(task_id)
        if record.status != TASK_QUEUED:
            raise TaskStateConflictError(
                f"task {task_id!r} cannot be consumed in status "
                f"{record.status!r}"
            )
        return self._consume(task_id)

    # --------------------------------------------------------------- 查询

    def status(self, task_id: str) -> str:
        """返回 queued/processing/completed/failed；不存在抛
        :class:`TaskNotFoundError`。查询不触发验证、不改变状态。"""
        return self._require_task(task_id).status

    def progress(self, task_id: str) -> TaskProgress:
        """返回 :class:`TaskProgress` 快照。

        ``completed`` 为已经得出结论（通过或失败）的项数，是真实进度；
        任务未结束时 ``result`` 为 ``None``，不提前给出最终结果。终态后
        重复查询返回同一稳定结果对象。不存在抛
        :class:`TaskNotFoundError`。
        """
        record = self._require_task(task_id)
        return TaskProgress(
            task_id=record.task_id,
            status=record.status,
            total=record.total,
            completed=len(record.results),
            result=record.result,
        )

    def result(self, task_id: str) -> BatchTaskResult:
        """返回终态结果：completed 返回完整结果，failed 返回保留了此前已
        完成项的部分结果（``total`` 仍为整批总数）。

        queued/processing 尚未结束、不能提前给出最终结果，抛
        :class:`TaskStateConflictError`；不存在抛
        :class:`TaskNotFoundError`。重复查询幂等，不再次调用验证器。
        """
        record = self._require_task(task_id)
        if record.result is None:
            raise TaskStateConflictError(
                f"task {task_id!r} has no final result yet "
                f"(status={record.status!r})"
            )
        return record.result

    def task_error(
        self, task_id: str
    ) -> Optional[VerificationInfrastructureError]:
        """返回 failed 任务保留的基础设施错误；其余状态返回 ``None``。

        不存在抛 :class:`TaskNotFoundError`。
        """
        return self._require_task(task_id).error

    # --------------------------------------------------------------- 复核

    def retry_failed(
        self, task_id: str, verifiers: Any = None
    ) -> TaskRetrySubmission:
        """从 completed/failed 源任务挑选待复核项，创建独立的 queued 任务。

        入选项按**根任务（最初提交）输入顺序**组成新任务，源任务的状态、
        结果、错误定位与保存历史均不变；新任务只是普通任务，沿用
        run_next/run_task/progress/result/task_error，不起线程、不联网、
        不落盘。每次复核都生成不同的新任务标识，可对复核任务再次复核。

        入选规则：

        * completed：只选 ``results`` 中 ``status == failed`` 的项；
        * failed：从 :attr:`VerificationInfrastructureError.index` 对应的
          错误项起，纳入错误项及其后尚无结果的项，再与已有失败项合并按根
          任务输入顺序去重；若错误无明确项下标（如 result_save 失败）且
          结果已覆盖全部输入，则只选已有失败项。

        新任务每项只验证一次，:class:`ItemResult.index` 保留根任务下标——
        再次失败仍定位到最初提交的输入位置。``verifiers`` 只作用于新任务，
        省略时继承源任务的选择，解析与契约异常的口径与普通执行一致。

        异常（不得互相替代）：未知任务 :class:`TaskNotFoundError`；
        queued/processing 源任务 :class:`TaskStateConflictError`；
        没有可复核项 :class:`NoRetryableItemsError`。
        """
        record = self._require_task(task_id)
        if record.status not in TASK_TERMINAL_STATUSES:
            raise TaskStateConflictError(
                f"task {task_id!r} cannot be retried in status "
                f"{record.status!r}"
            )

        positions = self._retryable_positions(record)
        if not positions:
            raise NoRetryableItemsError(
                f"task {task_id!r} has no retryable items "
                f"(status={record.status!r})"
            )

        items = [record.items[pos] for pos in positions]
        root_indexes = [record.root_indexes[pos] for pos in positions]
        chosen = verifiers if verifiers is not None else record.verifiers
        new_task_id = f"task-{next(self._counter)}"
        self._tasks[new_task_id] = _TaskRecord(
            new_task_id, items, chosen, root_indexes=root_indexes
        )
        return TaskRetrySubmission(
            source_task_id=task_id,
            task_id=new_task_id,
            status=TASK_QUEUED,
            summary=BatchSummary(
                total=len(items),
                item_ids=[item["item_id"] for item in items],
            ),
            retried_indexes=root_indexes,
        )

    # --------------------------------------------------------------- 内部

    def _consume(self, task_id: str) -> BatchTaskResult:
        record = self._tasks[task_id]
        record.status = TASK_PROCESSING
        self._current = task_id
        try:
            self._verify_items(record)
        except VerificationInfrastructureError as exc:
            # 基础设施失败：冻结已完成项，任务进入 failed 终态并保留部分结果。
            record.error = exc
            record.status = TASK_FAILED
            record.result = self._build_result(record)
            raise
        else:
            result = self._build_result(record)
            record.result = result
            record.status = TASK_COMPLETED
            self._store_result(record, result)
            return result
        finally:
            self._current = None

    def _verify_items(self, record: _TaskRecord) -> None:
        try:
            protocol_map = _resolve_verifiers(record.verifiers)
        except VerifierContractError as exc:
            # 验证器集合本身配置错误：任务级验证器故障，尚无项完成。
            raise VerificationInfrastructureError(
                index=None,
                item_id=None,
                stage=ITEM_STAGE_VERIFY,
                code=ITEM_CODE_VERIFIER_FAULT,
                message=MSG_VERIFIER_FAULT.format(detail=_exc_message(exc)),
            )

        for index, item in enumerate(record.items):
            # 定位下标始终回溯到根任务（最初提交）输入，复核链上也不重置。
            root_index = record.root_indexes[index]
            item_id = item["item_id"]
            material = item["proof"]

            # ---- 读取证明材料 ------------------------------------------
            try:
                proof = _read_proof(material)
            except _ProofReadError as exc:
                raise VerificationInfrastructureError(
                    index=root_index,
                    item_id=item_id,
                    stage=ITEM_STAGE_PROOF_READ,
                    code=ITEM_CODE_INVALID_PROOF,
                    message=MSG_PROOF_READ.format(detail=str(exc)),
                )

            # ---- 解析验证器 --------------------------------------------
            verifier = protocol_map.get(proof.protocol)
            if verifier is None:
                raise VerificationInfrastructureError(
                    index=root_index,
                    item_id=item_id,
                    stage=ITEM_STAGE_VERIFY,
                    code=ITEM_CODE_VERIFIER_UNAVAILABLE,
                    message=MSG_VERIFIER_UNAVAILABLE.format(
                        protocol=proof.protocol
                    ),
                )

            # ---- 逐项验证（每项至多一次；单证失败不中止其他项） --------
            try:
                _require_method(verifier, "verify")
                ok = _require_bool(verifier.verify(proof), verifier, "verify")
            except VerifierContractError as exc:
                raise VerificationInfrastructureError(
                    index=root_index,
                    item_id=item_id,
                    stage=ITEM_STAGE_VERIFY,
                    code=ITEM_CODE_VERIFIER_FAULT,
                    message=MSG_VERIFIER_FAULT.format(detail=_exc_message(exc)),
                )
            except Exception as exc:  # noqa: BLE001 - 验证器执行失败
                raise VerificationInfrastructureError(
                    index=root_index,
                    item_id=item_id,
                    stage=ITEM_STAGE_VERIFY,
                    code=ITEM_CODE_VERIFIER_FAULT,
                    # 只保留异常类型名与文本，绝不带调用栈或证明材料。
                    message=MSG_VERIFIER_FAULT.format(detail=_exc_message(exc)),
                )

            if ok:
                record.results.append(
                    ItemResult(
                        index=root_index,
                        item_id=item_id,
                        status=ITEM_PASSED,
                    )
                )
            else:
                record.results.append(
                    ItemResult(
                        index=root_index,
                        item_id=item_id,
                        status=ITEM_FAILED,
                        stage=ITEM_STAGE_VERIFY,
                        code=ITEM_CODE_REJECTED,
                        message=MSG_REJECTED,
                    )
                )

    def _store_result(
        self, record: _TaskRecord, result: BatchTaskResult
    ) -> None:
        if self._result_store is None:
            return
        try:
            self._result_store(result)
        except Exception as exc:  # noqa: BLE001 - 无法保存最终结果
            record.error = VerificationInfrastructureError(
                index=None,
                item_id=None,
                stage=STAGE_RESULT_SAVE,
                code=ITEM_CODE_SAVE_FAULT,
                message=MSG_SAVE_FAULT.format(detail=_exc_message(exc)),
            )
            record.status = TASK_FAILED
            raise record.error

    @staticmethod
    def _retryable_positions(record: _TaskRecord) -> List[int]:
        """计算待复核项在该任务自身 ``items`` 中的位置，按根任务顺序去重。

        选择在**根任务下标空间**判定（复核任务的 ``root_indexes`` 把本地
        位置映射回根下标，普通提交时二者相同）：completed 只选已有失败项；
        failed 额外纳入错误项下标起、尚无结果的错误项及其后项。结果天然按
        根任务输入顺序排列，已通过项不会被选中。
        """
        failed_roots = {
            result.index
            for result in record.results
            if result.status == ITEM_FAILED
        }
        present_roots = {result.index for result in record.results}
        error_root: Optional[int] = None
        if record.status == TASK_FAILED and record.error is not None:
            error_root = record.error.index

        positions: List[int] = []
        for pos, root_index in enumerate(record.root_indexes):
            if root_index in failed_roots:
                positions.append(pos)
            elif (
                isinstance(error_root, int)
                and root_index >= error_root
                and root_index not in present_roots
            ):
                positions.append(pos)
        return positions

    @staticmethod
    def _build_result(record: _TaskRecord) -> BatchTaskResult:
        passed = sum(1 for r in record.results if r.status == ITEM_PASSED)
        failed = sum(1 for r in record.results if r.status == ITEM_FAILED)
        failed_ids = [
            r.item_id for r in record.results if r.status == ITEM_FAILED
        ]
        return BatchTaskResult(
            task_id=record.task_id,
            total=record.total,
            passed=passed,
            failed=failed,
            results=list(record.results),
            failed_item_ids=failed_ids,
        )

    def _ensure_no_active_consumer(self) -> None:
        if self._current is not None:
            raise TaskStateConflictError(
                f"task {self._current!r} is already being consumed"
            )

    def _require_task(self, task_id: str) -> _TaskRecord:
        record = self._tasks.get(task_id)
        if record is None:
            raise TaskNotFoundError(f"unknown task id: {task_id!r}")
        return record


# ============================================================ 提交前校验

def _validate_items(items: Any) -> List[dict]:
    """提交前完整输入校验；通过则返回原样保留的验证项列表。

    优先级：空批次 -> 超上限 -> 逐项（标识 -> 材料格式，按输入顺序）
    -> 重复标识。任何失败都抛出专用异常，绝不混用。
    """
    if not isinstance(items, (list, tuple)) or len(items) == 0:
        raise EmptyBatchError(
            "batch must be a non-empty list of verification items"
        )
    if len(items) > MAX_BATCH_ITEMS:
        raise BatchSizeLimitError(len(items), MAX_BATCH_ITEMS)

    normalized: List[dict] = []
    for index, item in enumerate(items):
        if not isinstance(item, Mapping):
            raise InvalidItemIdError(
                f"item at index {index} must be a mapping with 'item_id'"
            )
        item_id = item.get("item_id")
        if not isinstance(item_id, str) or not item_id:
            raise InvalidItemIdError(
                f"item at index {index}: 'item_id' must be a non-empty str"
            )
        # 公开格式约束：proof 必须存在且为非空映射；更深的字段结构在
        # 执行期读取时再校验（无法读取 -> 基础设施错误）。
        if "proof" not in item:
            raise InvalidProofFormatError(
                f"item {item_id!r} is missing 'proof' material"
            )
        material = item["proof"]
        if not isinstance(material, Mapping) or len(material) == 0:
            raise InvalidProofFormatError(
                f"item {item_id!r}: 'proof' must be a non-empty mapping"
            )
        normalized.append(dict(item))

    # 完整的逐项校验通过后再做唯一性检查（与基线"先归一化后查重"一致）。
    seen: Dict[str, int] = {}
    for index, item in enumerate(normalized):
        item_id = item["item_id"]
        if item_id in seen:
            raise DuplicateItemIdError(item_id)
        seen[item_id] = index
    return normalized


# ============================================================ 执行期读取

def _read_proof(material: Any) -> Proof:
    """把证明材料归一化为 :class:`Proof`；无法读取时抛
    :class:`_ProofReadError`。字段口径与聚合引擎的单证归一化一致。"""
    if not isinstance(material, Mapping):
        raise _ProofReadError("proof material is not a mapping")
    missing = [name for name in _REQUIRED_FIELDS if name not in material]
    if missing:
        raise _ProofReadError(
            f"missing fields: {', '.join(missing)}"
        )
    proof_id = material["proof_id"]
    if not isinstance(proof_id, str) or not proof_id:
        raise _ProofReadError("'proof_id' must be a non-empty str")
    for name in ("protocol", "circuit_id", "aggregation_key"):
        value = material[name]
        if not isinstance(value, str) or not value:
            raise _ProofReadError(f"{name!r} must be a non-empty str")
    if not isinstance(material["public_inputs"], list):
        raise _ProofReadError("'public_inputs' must be a list")
    return Proof(
        proof_id=proof_id,
        protocol=material["protocol"],
        circuit_id=material["circuit_id"],
        aggregation_key=material["aggregation_key"],
        public_inputs=material["public_inputs"],
        proof=material["proof"],
    )
