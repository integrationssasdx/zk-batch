# ZK Batch

ZK 证明批聚合验证服务：批量证明聚合、验证队列与失败定位。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

初始基线：只有本说明，尚无实现。

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
- `VerificationTaskQueue` —— 可排队、可定位失败原因的**逐项**批量验证队列，
  与上面的分组聚合队列相互独立；同样不起线程、不联网、不落盘。输入是一组
  按业务顺序排列的验证项，每项含稳定标识 `item_id`（非空字符串）与证明
  材料 `proof`（非空映射，字段口径同 `verify_batch` 的单证：`proof_id`、
  `protocol`、`circuit_id`、`aggregation_key`、`public_inputs`、`proof`）。
  - `submit(items, verifiers=None)` 先完成**完整输入校验**，全部通过后才
    生成稳定任务标识并入队，返回 `TaskSubmission`（`task_id`、
    `status="queued"`、`summary`：`total` 与按输入顺序的 `item_ids`）。
    校验错误在入队前抛出，请求不进入队列、不生成任务标识，优先级固定为：
    空批次 `EmptyBatchError` → 超过公开上限 `MAX_BATCH_ITEMS` 的
    `BatchSizeLimitError` → 按输入顺序逐项检查的 `InvalidItemIdError`
    （标识缺失/非非空字符串）与 `InvalidProofFormatError`
    （缺 `proof` 或不是非空映射）→ 同批重复标识 `DuplicateItemIdError`，
    不混用其他异常。
  - `run_next()` / `run_task(task_id)` 按输入顺序逐项验证。单个证明失败不
    中止同批其他项，每项只验证一次。`progress(task_id)` 返回
    `TaskProgress`：`status` 与真实 `completed`/`total` 进度，任务未结束时
    `result` 为 `None`，不提前给出最终结果。
  - 终态结果 `result(task_id)` 返回 `BatchTaskResult`：`total`/`passed`/
    `failed`、与输入顺序一致的 `results`（每项含 `index` 输入序号、
    `item_id`、`status`（`passed`/`failed`）、`stage`、稳定 `code` 与供
    人工定位的 `message`），以及按输入顺序的 `failed_item_ids`。单证被拒
    的失败项为 `stage="verify"`、`code="rejected"`。
  - 状态机为 `queued → processing → completed | failed`。查询不存在的任务
    抛 `TaskNotFoundError`；任务尚未结束时取结果、重复消费任务（含处理中
    再次 `run_next`/`run_task`）、终态后再次入队/消费等状态冲突抛
    `TaskStateConflictError`。
  - 系统无法读取证明材料（`stage="proof_read"`、`code="invalid_proof"`）、
    验证器执行失败（`stage="verify"`、`code="verifier_fault"`；协议无验证器
    为 `code="verifier_unavailable"`）或无法保存最终结果
    （`stage="result_save"`、`code="result_save_fault"`，由可选的
    `result_store` 钩子触发）时抛 `VerificationInfrastructureError`，任务
    进入 `failed` 终态，并保留此前已完成项及其定位信息（部分结果的 `total`
    仍为整批总数）；错误对象固定携带 `index`/`item_id`/`stage`/`code`。
  - 终态与结果查询保持幂等：不再次调用验证器、不改变状态，重复查询返回同一
    结果对象；定位信息绝不包含完整证明材料、内部调用栈或未公开验证器信息。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
