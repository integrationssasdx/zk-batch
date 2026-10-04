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
  - `reverify_failures(job_id, verifiers=None)` 以已完成作业的失败清单为
    输入，只挑出原批次中失败的证明（按源批次原序、`batch_id` 与证明材料
    原样）创建独立的 queued 作业，并记录新作业对源作业的直接父子关系；
    不重提整批，源作业状态与结果不变。重复复核生成不同作业 id，父子关系
    互不覆盖；复核作业执行异常按 `run_next` 传播并移除作业。源作业未知抛
    `UnknownJobError`，非 completed 抛 `ResultUnavailableError`，completed
    但无失败证明抛 `NoFailedProofError`。
  - `reverify_outcome(source_job_id, retry_job_id)` 只读复核
    `reverify_failures` 的结果，返回 `ReverifyOutcomeReport`（固定键序
    `to_dict`）：`source_job_id`、`retry_job_id`、按源批次原序的
    `selected_proof_ids` 与 `items` 明细、按结论归类的
    `recovered_proof_ids`/`still_failed_proof_ids`（各自保持源批次原序）。
    每条明细（`ReverifyOutcomeItem`，固定键序 `to_dict`）含 `proof_id`、
    唯一 `outcome`（仅 `recovered`/`still_failed`）以及
    `before_*`/`after_*` 的 `status`/`stage`/`code`/`message`：
    `before_status` 固定 `failed`，`before_*` 取源作业的失败定位；恢复项
    `after_status="passed"` 且定位为空，失败项 `after_status="failed"` 且
    定位取复核作业的 `Failure`。对账只覆盖 `reverify_failures` 所选失败
    证明，不暴露 proof、`public_inputs`、调用栈或未公开验证器信息；查询
    只读已保存结果，不调用验证器、不创建作业、不改状态，重复查询一致。
    作业未知抛 `UnknownJobError`，作业未 completed 抛
    `ResultUnavailableError`，均 completed 但 `retry_job_id` 不是
    `source_job_id` 的直接复核作业抛 `ReverifyLineageMismatchError`，三者
    互不替代。
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
  - `retry_failed(task_id, verifiers=None)` 从 completed/failed 源任务选取
    待复核项，按根任务输入顺序创建独立的 queued 任务，返回
    `TaskRetrySubmission`（`source_task_id`、`task_id`、`status="queued"`、
    `summary`、`retried_indexes` 根任务输入零起下标）。completed 只选
    `status="failed"` 的项；failed 从保留的
    `VerificationInfrastructureError.index` 对应项起，纳入该错误项及其后
    尚无结果的项，再与已有失败项按根任务输入顺序去重（`result_save` 失败
    且结果已覆盖全部输入时只选失败项）。新任务沿用
    `run_next`/`run_task`/`status`/`progress`/`result`/`task_error`，每项只
    验证一次，`ItemResult.index` 与再次失败的定位都保留根任务下标；重复
    复核生成不同任务标识，源任务状态、结果与错误定位不变。`verifiers` 只
    作用于新任务，省略时继承源任务的验证器选择。无可复核项抛
    `NoRetryableItemsError`，未知任务抛 `TaskNotFoundError`，queued 或
    processing 源任务抛 `TaskStateConflictError`，三者互不替代。
  - `retry_outcome(source_task_id, retry_task_id)` 只读对账复核结果，返回
    `RetryOutcomeReport`（固定键序 `to_dict`）：`source_task_id`、
    `retry_task_id`、双方终态 `source_status`/`retry_status`、
    `retried_indexes`、按根任务输入顺序排列的 `items` 明细、按结论归类的
    `recovered_item_ids`/`still_failed_item_ids`/`unresolved_item_ids`
    （各自保持根任务输入顺序）及对应 `*_count`。对账范围严格沿用
    `retry_failed` 的选取；每条明细（`RetryOutcomeItem`，固定键序
    `to_dict`）含根任务零起 `index`、`item_id`、`before_*`/`after_*` 的
    `status`/`stage`/`code`/`message` 与唯一 `outcome`。`before_status`
    只取 `failed`（源任务失败项）或 `unresolved`（错误项及其后无结果项）；
    `after_status` 只取 `passed`/`failed`/`unresolved`，分别记结论
    `recovered`/`still_failed`/`still_unresolved`；定位沿用对应任务已有的
    `stage`/`code`/`message`（无结果的项沿用该任务保留的基础设施错误定位），
    未入选项不进入明细。对账不调用验证器、不创建任务、不改变状态或结果，
    重复查询一致，不含证明材料、`public_inputs`、调用栈或未公开验证器信息。
    任务不存在抛 `TaskNotFoundError`，任务未终结抛
    `TaskStateConflictError`，复核任务不是源任务的直接 `retry_failed`
    任务抛 `TaskLineageMismatchError`，三者互不替代。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
