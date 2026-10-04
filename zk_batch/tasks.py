"""排队逐项验证（任务流程）。

与 :class:`~zk_batch.queue.VerificationQueue`（分组聚合、四态作业）相互
独立：本模块面向"一组按业务顺序排列、各带稳定标识的验证项"，提供可排队、
可定位失败原因、可在基础设施故障后续跑的逐项验证。

公开入口 :class:`ItemVerificationQueue`：

* :meth:`submit` 先做**完整输入校验**（不通过则不生成任务、不进入队列），
  通过后按输入顺序生成一个 queued 任务，返回 :class:`TaskReceipt`
  （稳定任务标识、排队状态、批次摘要）。校验错误固定为：
  EmptyBatchError / InvalidItemIdError / BatchSizeLimitError /
  InvalidProofFormatError / DuplicateItemIdError，互不混用。
* :meth:`run_next` 同步执行最早的 queued 任务，**逐项**调用验证器的
  单证验证（不做聚合，不改变既有聚合成功口径）：
  单证返回 ``False`` 记为该项失败（code=rejected），继续验证同批后续项；
  返回 ``True`` 记为通过。
* 无法读取证明材料、验证器执行失败（抛异常/契约违约/无对应验证器）或
  无法保存结果时抛 :class:`VerificationInfrastructureError`，任务置为
  failed 并保留此前已完成项；:meth:`retry` 续跑时已完成项不重复验证。
* :meth:`get_task` 是统一查询入口，返回 :class:`TaskSnapshot`：
  未结束给真实状态与完成进度（不提前给最终结果），completed 给稳定的
  :class:`ItemBatchResult`；查询幂等、不调用验证器、不改状态。
* 状态冲突（终态后再次入队、重复消费、非法迁移）统一抛
  :class:`TaskStateConflictError`；未知任务一律
  :class:`TaskNotFoundError`。

状态机：

    queued -> processing -> completed
       |          |
       v          v
   cancelled   failed --(retry)--> queued（排到队尾，仅 failed 可重试）

不持久化、不联网、不使用线程；保存/读取环节以可覆盖的受保护方法给出
接缝，默认实现为内存内即时完成。
"""

from __future__ import annotations

import itertools
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from .engine import _require_bool, _require_method
from .errors import (
    BatchSizeLimitError,
    DuplicateItemIdError,
    EmptyBatchError,
    InvalidItemIdError,
    InvalidProofFormatError,
    TaskNotFoundError,
    TaskStateConflictError,
    VerificationInfrastructureError,
    VerifierContractError,
)
from .models import (
    ITEM_CODE_REJECTED,
    ITEM_STAGE_VERIFY,
    BatchSummary,
    ItemBatchResult,
    ItemResult,
    Proof,
    TaskProgress,
    TaskReceipt,
    TaskSnapshot,
)
from .verifier import ZKVerifier

#: 单任务公开上限：一次提交最多包含的验证项数
MAX_BATCH_ITEMS = 1000

# 任务状态
TASK_QUEUED = "queued"
TASK_PROCESSING = "processing"
TASK_COMPLETED = "completed"
TASK_CANCELLED = "cancelled"
TASK_FAILED = "failed"

# 基础设施故障阶段
INFRA_STAGE_READ = "read_material"
INFRA_STAGE_VERIFY = "verify"
INFRA_STAGE_SAVE = "save_result"

# 基础设施故障错误码（稳定可比较）
INFRA_CODE_MATERIAL = "material_unreadable"
INFRA_CODE_UNSUPPORTED = "unsupported_protocol"
INFRA_CODE_CONTRACT = "verifier_contract_violation"
INFRA_CODE_EXECUTION = "verifier_execution_failed"
INFRA_CODE_SAVE = "result_not_saved"

_REJECTED_MESSAGE = "proof rejected by verifier"
_REQUIRED_MATERIAL_FIELDS = (
    "protocol",
    "circuit_id",
    "aggregation_key",
    "public_inputs",
    "proof",
)


class _Task:
    """任务的内部记录（不对外暴露）。"""

    def __init__(self, task_id: str, items: List[dict], verifiers: Any):
        self.task_id = task_id
        # 已通过提交期校验的原始验证项（含 item_id 与 proof_material）
        self.items = items
        self.verifiers = verifiers
        self.status = TASK_QUEUED
        # 按输入序号存放已完成项；None 表示该项尚未得出结论
        self.slots: List[Optional[ItemResult]] = [None] * len(items)
        self.completed = 0
        self.result: Optional[ItemBatchResult] = None
        self.error: Optional[VerificationInfrastructureError] = None


class ItemVerificationQueue:
    """内存中的串行逐项验证任务队列。"""

    def __init__(self, verifiers: Any = None, *, max_items: int = MAX_BATCH_ITEMS):
        """:param verifiers: 任务执行时使用的默认验证器集合（单个/列表/字典）。

        :param max_items: 单任务项数公开上限，默认
            :data:`MAX_BATCH_ITEMS`。
        """
        self._default_verifiers = verifiers
        self._max_items = max_items
        self._tasks: "OrderedDict[str, _Task]" = OrderedDict()
        self._counter = itertools.count(1)

    # ============================================================== 提交

    def submit(self, items: Any, verifiers: Any = None) -> TaskReceipt:
        """提交一组验证项；完成完整输入校验后入队，返回 :class:`TaskReceipt`。

        ``items`` 为非空列表，每项形如::

            {"item_id": "稳定标识",
             "proof_material": {"protocol": ..., "circuit_id": ...,
                                "aggregation_key": ...,
                                "public_inputs": [...], "proof": ...}}

        校验顺序（靠前的错误优先抛出，任何校验失败都不生成任务）：
        空批次 → 标识缺失/为空 → 数量超限 → 材料格式 → 标识重复。

        ``verifiers`` 省略时使用构造队列时给定的默认验证器。
        """
        self._validate(items)
        task_id = f"task-{next(self._counter)}"
        task = _Task(
            task_id=task_id,
            items=[dict(item) for item in items],
            verifiers=verifiers if verifiers is not None else self._default_verifiers,
        )
        self._tasks[task_id] = task
        return TaskReceipt(
            task_id=task_id,
            status=TASK_QUEUED,
            summary=BatchSummary(
                total=len(items),
                item_ids=[item["item_id"] for item in items],
            ),
        )

    # -------------------------------------------------- 提交期完整校验

    def _validate(self, items: Any) -> None:
        # 1) 批次非空：必须是非空列表/元组
        if not isinstance(items, (list, tuple)) or len(items) == 0:
            raise EmptyBatchError(
                "items must be a non-empty list of verification items"
            )

        # 2) 逐项标识校验（按输入顺序，第一个错误优先）
        for index, item in enumerate(items):
            if not isinstance(item, Mapping) or "item_id" not in item:
                raise InvalidItemIdError(
                    f"item at index {index} is missing 'item_id'"
                )
            item_id = item["item_id"]
            if not isinstance(item_id, str) or not item_id:
                raise InvalidItemIdError(
                    f"item at index {index}: 'item_id' must be a non-empty str"
                )

        # 3) 数量上限
        if len(items) > self._max_items:
            raise BatchSizeLimitError(len(items), self._max_items)

        # 4) 逐项证明材料格式校验
        for index, item in enumerate(items):
            self._validate_material(index, item.get("proof_material"))

        # 5) 标识唯一
        seen: set = set()
        for item in items:
            item_id = item["item_id"]
            if item_id in seen:
                raise DuplicateItemIdError(item_id)
            seen.add(item_id)

    def _validate_material(self, index: int, material: Any) -> None:
        where = f"item at index {index}"
        if not isinstance(material, Mapping):
            raise InvalidProofFormatError(
                f"{where}: 'proof_material' must be a mapping"
            )
        missing = [name for name in _REQUIRED_MATERIAL_FIELDS if name not in material]
        if missing:
            raise InvalidProofFormatError(
                f"{where}: 'proof_material' missing fields: {', '.join(missing)}"
            )
        for name in ("protocol", "circuit_id", "aggregation_key"):
            value = material[name]
            if not isinstance(value, str) or not value:
                raise InvalidProofFormatError(
                    f"{where}: 'proof_material.{name}' must be a non-empty str"
                )
        if not isinstance(material["public_inputs"], list):
            raise InvalidProofFormatError(
                f"{where}: 'proof_material.public_inputs' must be a list"
            )
        # proof 内容对队列不透明，仅要求字段存在。

    # ============================================================== 执行

    def run_next(self) -> Optional[ItemBatchResult]:
        """执行最早的 queued 任务；没有可执行任务时返回 ``None``。

        逐项验证、单项失败不中止同批其他项；已得出结论的项（含故障续跑）
        不重复验证。任务全部完成后状态置为 completed 并返回
        :class:`ItemBatchResult`；基础设施故障抛
        :class:`VerificationInfrastructureError`，任务置为 failed 并保留
        此前已完成项。
        """
        task = self._next_queued_task()
        if task is None:
            return None
        self._transition(task, TASK_QUEUED, TASK_PROCESSING)
        return self._process(task)

    def retry(self, task_id: str, verifiers: Any = None) -> TaskReceipt:
        """把 failed 任务重新排队续跑，返回新的排队回执（task_id 不变）。

        续跑从第一个尚未得出结论的项继续，已完成项不重复验证；任务排到
        队尾。``verifiers`` 给出时替换该任务使用的验证器集合（如故障消除
        后补注册对应 protocol），省略时沿用任务原有验证器。未知任务抛
        :class:`TaskNotFoundError`；任务不在 failed 状态
        （queued/processing/completed/cancelled）抛
        :class:`TaskStateConflictError`。
        """
        task = self._require_task(task_id)
        if task.status != TASK_FAILED:
            raise TaskStateConflictError(
                f"task {task_id!r} cannot be re-enqueued "
                f"(status={task.status!r}); only failed tasks can be retried"
            )
        if verifiers is not None:
            task.verifiers = verifiers
        task.error = None
        task.status = TASK_QUEUED
        self._tasks.move_to_end(task_id)
        return TaskReceipt(
            task_id=task_id,
            status=TASK_QUEUED,
            summary=BatchSummary(
                total=len(task.items),
                item_ids=[item["item_id"] for item in task.items],
            ),
        )

    def cancel(self, task_id: str) -> bool:
        """取消 queued 任务并返回 ``True``。

        processing/completed/cancelled/failed 抛
        :class:`TaskStateConflictError`；未知任务抛
        :class:`TaskNotFoundError`。
        """
        task = self._require_task(task_id)
        if task.status == TASK_QUEUED:
            task.status = TASK_CANCELLED
            return True
        raise TaskStateConflictError(
            f"task {task_id!r} cannot be cancelled (status={task.status!r})"
        )

    def _process(self, task: _Task) -> ItemBatchResult:
        try:
            try:
                protocol_map = self._resolve_verifiers(task.verifiers)
            except VerificationInfrastructureError:
                raise
            except Exception as exc:  # noqa: BLE001 - 验证器集合不可用属执行故障
                raise self._infra(
                    task, None,
                    INFRA_STAGE_VERIFY, INFRA_CODE_EXECUTION,
                    f"cannot resolve verifiers: {type(exc).__name__}",
                ) from None
            for index, item in enumerate(task.items):
                if task.slots[index] is not None:
                    # 故障续跑：已完成项不重复验证
                    continue
                item_result = self._verify_one(task, index, item, protocol_map)
                task.slots[index] = item_result
                task.completed += 1
                # 每项结论落盘接缝：保存失败归基础设施故障；
                # 结论已在内存保留，任务转入 failed 后续跑不重复该项。
                try:
                    self._persist_item_progress(task, index, item_result)
                except VerificationInfrastructureError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise self._infra(
                        task, index,
                        INFRA_STAGE_SAVE, INFRA_CODE_SAVE,
                        f"cannot save item result: {type(exc).__name__}",
                    ) from None

            result = self._build_result(task)
            # 最终结果保存接缝
            try:
                self._persist_final_result(task, result)
            except VerificationInfrastructureError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise self._infra(
                    task, None,
                    INFRA_STAGE_SAVE, INFRA_CODE_SAVE,
                    f"cannot save final result: {type(exc).__name__}",
                ) from None
        except VerificationInfrastructureError as exc:
            task.status = TASK_FAILED
            task.error = exc
            raise

        task.result = result
        task.status = TASK_COMPLETED
        return result

    def _verify_one(
        self,
        task: _Task,
        index: int,
        item: dict,
        protocol_map: Dict[str, ZKVerifier],
    ) -> ItemResult:
        """验证单项；业务性拒绝返回 failed ItemResult，系统故障抛 infra。"""
        item_id = item["item_id"]

        # ---- 读取证明材料（接缝；默认归一化不会失败） --------------------
        try:
            proof = self._load_proof(task, index, item)
        except VerificationInfrastructureError:
            raise
        except Exception as exc:  # noqa: BLE001 - 读取故障归基础设施
            raise self._infra(
                task, index,
                INFRA_STAGE_READ, INFRA_CODE_MATERIAL,
                f"cannot read proof material: {type(exc).__name__}",
            ) from None

        # ---- 解析验证器 --------------------------------------------------
        verifier = protocol_map.get(proof.protocol)
        if verifier is None:
            raise self._infra(
                task, index,
                INFRA_STAGE_VERIFY, INFRA_CODE_UNSUPPORTED,
                f"no verifier registered for protocol {proof.protocol!r}",
            )

        # ---- 执行单证验证 ------------------------------------------------
        try:
            _require_method(verifier, "verify")
            raw = verifier.verify(proof)
        except VerifierContractError as exc:
            raise self._infra(
                task, index,
                INFRA_STAGE_VERIFY, INFRA_CODE_CONTRACT,
                f"verifier contract violated: {type(exc).__name__}",
            ) from None
        except VerificationInfrastructureError:
            raise
        except Exception as exc:  # noqa: BLE001 - 验证器执行失败
            # 只保留异常类型名，不带 str(exc)（可能含内部信息），不带栈
            raise self._infra(
                task, index,
                INFRA_STAGE_VERIFY, INFRA_CODE_EXECUTION,
                f"verifier execution failed: {type(exc).__name__}",
            ) from None
        try:
            ok = _require_bool(raw, verifier, "verify")
        except VerifierContractError as exc:
            raise self._infra(
                task, index,
                INFRA_STAGE_VERIFY, INFRA_CODE_CONTRACT,
                f"verifier contract violated: {type(exc).__name__}",
            ) from None

        if ok:
            return ItemResult(index=index, item_id=item_id, passed=True)
        return ItemResult(
            index=index,
            item_id=item_id,
            passed=False,
            stage=ITEM_STAGE_VERIFY,
            code=ITEM_CODE_REJECTED,
            message=_REJECTED_MESSAGE,
        )

    # -------------------------------------------------- 存储/读取接缝

    def _load_proof(self, task: _Task, index: int, item: dict) -> Proof:
        """把验证项材料归一化为 :class:`Proof`。

        默认实现只做对象构造（材料已在提交期通过完整校验）。子类可覆盖以
        接入真实的材料读取（如反序列化、取对象存储）；读取失败应抛异常，
        队列将其归为 ``read_material`` 阶段的
        :class:`VerificationInfrastructureError`。
        """
        material = item["proof_material"]
        return Proof(
            proof_id=item["item_id"],
            protocol=material["protocol"],
            circuit_id=material["circuit_id"],
            aggregation_key=material["aggregation_key"],
            public_inputs=material["public_inputs"],
            proof=material["proof"],
            index=index,
        )

    def _persist_item_progress(
        self, task: _Task, index: int, item_result: ItemResult
    ) -> None:
        """单项结论保存接缝；默认内存实现为空操作。

        覆盖以接入持久化时，抛异常将归为 ``save_result`` 阶段的
        :class:`VerificationInfrastructureError`（结论已在内存保留）。
        """

    def _persist_final_result(
        self, task: _Task, result: ItemBatchResult
    ) -> None:
        """最终结果保存接缝；默认内存实现为空操作。"""

    # -------------------------------------------------- 结果组装

    def _build_result(self, task: _Task) -> ItemBatchResult:
        results: List[ItemResult] = [slot for slot in task.slots if slot is not None]
        passed_count = sum(1 for r in results if r.passed)
        failed_item_ids = [r.item_id for r in results if not r.passed]
        return ItemBatchResult(
            task_id=task.task_id,
            total=len(task.items),
            passed_count=passed_count,
            failed_count=len(results) - passed_count,
            results=results,
            failed_item_ids=failed_item_ids,
        )

    def _infra(
        self,
        task: _Task,
        index: Optional[int],
        stage: str,
        code: str,
        message: str,
    ) -> VerificationInfrastructureError:
        exc = VerificationInfrastructureError(message, stage=stage, code=code)
        exc.task_id = task.task_id
        exc.index = index
        exc.completed = task.completed
        exc.total = len(task.items)
        return exc

    # ============================================================== 查询

    def get_task(self, task_id: str) -> TaskSnapshot:
        """统一查询入口，返回 :class:`TaskSnapshot`。

        * queued/processing：``result`` 为 ``None``，给真实状态与完成进度
          （completed 为已得出结论的项数），不提前给出最终结果；
        * failed/cancelled：``result`` 为 ``None``，保留已完成项及定位；
        * completed：``result`` 为稳定的 :class:`ItemBatchResult`，重复
          查询返回同一对象，不再次调用验证器。

        未知任务抛 :class:`TaskNotFoundError`。
        """
        task = self._require_task(task_id)
        items = [slot for slot in task.slots if slot is not None]
        return TaskSnapshot(
            task_id=task_id,
            status=task.status,
            total=len(task.items),
            completed=task.completed,
            items=items,
            result=task.result,
        )

    def progress(self, task_id: str) -> TaskProgress:
        """返回 :class:`TaskProgress`（真实完成进度）；未知任务抛 TaskNotFoundError。"""
        task = self._require_task(task_id)
        return TaskProgress(
            task_id=task_id,
            status=task.status,
            total=len(task.items),
            completed=task.completed,
        )

    def status(self, task_id: str) -> str:
        """返回任务状态字符串；未知任务抛 :class:`TaskNotFoundError`。"""
        return self._require_task(task_id).status

    def result(self, task_id: str) -> ItemBatchResult:
        """返回 completed 任务的 :class:`ItemBatchResult`（幂等，同一对象）。

        queued/processing/failed/cancelled 抛
        :class:`TaskStateConflictError`；未知任务抛
        :class:`TaskNotFoundError`。
        """
        task = self._require_task(task_id)
        if task.status != TASK_COMPLETED:
            raise TaskStateConflictError(
                f"task {task_id!r} has no final result "
                f"(status={task.status!r})"
            )
        return task.result  # type: ignore[return-value]

    # ============================================================== 内部

    def _next_queued_task(self) -> Optional[_Task]:
        for task in self._tasks.values():
            if task.status == TASK_QUEUED:
                return task
        return None

    def _transition(self, task: _Task, expected: str, target: str) -> None:
        """封闭状态机迁移：当前状态不符即冲突（防止重复消费/并发踩踏）。"""
        if task.status != expected:
            raise TaskStateConflictError(
                f"task {task.task_id!r} cannot transition "
                f"{expected!r} -> {target!r} (status={task.status!r})"
            )
        task.status = target

    def _require_task(self, task_id: str) -> _Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(f"unknown task id: {task_id!r}")
        return task

    def _resolve_verifiers(self, verifiers: Any) -> Dict[str, ZKVerifier]:
        """把单个/列表/{protocol: verifier} 归一为 protocol 映射。

        解析本身的契约问题在逐项执行时体现为验证器执行故障；未知
        protocol 在具体项上归为基础设施故障并保留此前已完成项。
        """
        if isinstance(verifiers, Mapping):
            return {str(name): v for name, v in verifiers.items()}
        if isinstance(verifiers, ZKVerifier):
            verifiers = [verifiers]
        protocol_map: Dict[str, ZKVerifier] = {}
        try:
            iterator = iter(verifiers)
        except TypeError:
            return {}
        for verifier in iterator:
            for name in verifier.supported_protocols():
                protocol_map[name] = verifier
        return protocol_map
