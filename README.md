# ZK Batch

ZK 证明批聚合验证服务：批量证明聚合、验证队列与失败定位。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：分组聚合验证（含详细报告）、串行作业队列，以及排队逐项验证
（任务流程：可排队、失败定位、基础设施故障续跑）。行为以本说明与源码
公开入口为准。

## 公开入口

- `verify_batch(batch, verifiers)` —— 按 `(protocol, circuit_id, aggregation_key)`
  分组聚合验证，聚合验证失败时按原序回退单证验证，返回 `BatchVerificationResult`。
- `verify_batch_detailed(batch, verifiers)` —— 与 `verify_batch` 同一条校验与
  分组流水线（异常类型与优先级完全一致），返回 `BatchVerificationReport`：
  - `result`：与 `verify_batch` 返回值同值的 `BatchVerificationResult`；
  - `groups`：按分组首次出现顺序排列的 `GroupVerificationReport`，每组给出
    `group_id`、组内 `proof_id` 原序、`aggregate_call_status`
    （`succeeded`/`error`）、`aggregate_verify_status`
    （`passed`/`rejected`/`error`/`not_run`）、`fell_back` 以及逐证的
    `ProofVerificationDetail`（`proof_id`、`status`、`message`，原序）。
  - 聚合调用抛 `IncompatibleAggregationError` 时异常仍向调用方传播，不返回
    报告；抛其他异常时该组聚合状态记 `error`，逐证记 `not_run`，消息为
    `异常类型名: str(exc)`（`str(exc)` 为空时只保留类型名）。
  - 消息中不包含 `proof` 与 `public_inputs` 内容；各模型均提供固定键序的
    `to_dict()`。
- `VerificationQueue` —— 内存中的串行作业队列，不起线程、不联网、不落盘。
  作业执行时一次走完整的详细验证流水线并保存报告：
  - `result(job_id)` 返回该次执行的 `BatchVerificationResult`；
  - `report(job_id)` 返回同一次执行的 `BatchVerificationReport`
    （`report(job_id).result is result(job_id)`），给出聚合调用、聚合验证、
    回退单证验证各阶段的组级状态；查询报告不再次调用验证器，也不改变作业
    状态。仅 completed 作业可取结果/报告，queued/running/cancelled 抛
    `ResultUnavailableError`，未知或因执行错误移除的作业抛 `UnknownJobError`。
- `ItemVerificationQueue` —— 排队逐项验证任务队列（与上面的分组聚合
  队列相互独立，不改变既有聚合口径、队列可见字段、异常类型与顺序语义）。
  面向"一组按业务顺序排列、各带稳定标识的验证项"，提供可排队、可定位
  失败原因、基础设施故障可续跑的验证。
  - 输入为非空验证项列表，每项形如
    `{"item_id": "稳定标识", "proof_material": {protocol, circuit_id,
    aggregation_key, public_inputs, proof}}`。
  - `submit(items, verifiers=None)` 先完成**完整输入校验**，全部通过后才
    生成 queued 任务并返回 `TaskReceipt`（稳定 `task_id`、排队状态、
    `BatchSummary`：`total` 与按输入顺序的 `item_ids`）；任何校验失败都
    不生成任务、不进入验证队列。校验顺序与异常固定为：
    空批次 `EmptyBatchError` → 标识缺失/为空 `InvalidItemIdError` →
    数量超 `MAX_BATCH_ITEMS` 上限 `BatchSizeLimitError` →
    材料缺字段/类型不符 `InvalidProofFormatError` →
    同批重复标识 `DuplicateItemIdError`（携带 `item_id`）。
  - `run_next()` 同步执行最早的 queued 任务，**按输入顺序逐项**调用验证器
    的单证验证（`ZKVerifier.verify`，不做聚合）；单项返回 `False` 记该项
    失败（stage=`verify`、code=`rejected`）但不中止同批其他项，抛异常/
    契约违约/无对应验证器则按基础设施故障处理。已完成项不重复验证。
  - `get_task(task_id)` 是统一查询入口，返回 `TaskSnapshot`：
    queued/processing 时 `result=None`，只给真实状态与完成进度
    （`total`/`completed`/`remaining` 与已完成项），不提前给出最终结果；
    completed 时 `result` 为稳定的 `ItemBatchResult`，查询幂等、不再次调用
    验证器、不改变状态。未知任务抛 `TaskNotFoundError`。
  - `ItemBatchResult` 含 `task_id`、`total`、`passed_count`、
    `failed_count`、按输入顺序的 `results`（每项 `ItemResult`：
    `index`(0 基输入序号)、`item_id`、`passed`、失败时的 `stage`/`code`/
    `message`）与按输入顺序的 `failed_item_ids`。
  - 无法读取证明材料、验证器执行失败或无法保存最终结果时抛
    `VerificationInfrastructureError`（携带 `stage` 与稳定 `code`：
    `read_material`/`verify`/`save_result`），任务置 failed 并保留此前
    已完成项及其定位信息；`retry(task_id)` 仅对 failed 任务重新排队续跑
    （task_id 不变、排到队尾、跳过已完成项）。
  - 状态机为 `queued -> processing -> completed`，queued 可 `cancel()` 为
    cancelled，processing 可转入 failed。重复消费、终态后再次入队、非法
    状态迁移统一抛 `TaskStateConflictError`；非 completed 调 `result()`
    同样抛 `TaskStateConflictError`。
  - 定位信息只保留输入序号、项标识、失败阶段与原始错误码；描述与异常中
    不暴露完整证明材料、内部调用栈或未公开验证器信息。材料读取与结果保存
    通过受保护方法 `_load_proof` / `_persist_item_progress` /
    `_persist_final_result` 给出接缝，默认为内存内即时完成。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
