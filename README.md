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
- `verify_batch_windowed(batch, verifiers, max_group_size)` —— 受聚合容量
  约束的窗口化验证，参数同批量入口、仅新增 `max_group_size`，返回
  `WindowedBatchVerificationReport`：
  - 先按 `(protocol, circuit_id, aggregation_key)` 分组（分组按首次出现
    序、组内保持批次原序，不重排、不混组），再把每组按源序切成不超过
    `max_group_size` 的连续窗口；末窗可短，组内 `window_index` 从 1 起
    连续编号。`proofs` 保持输入原序，`proof_id` 全批唯一。
  - 每个窗口独立复用详细流水线与单证语义：`aggregate` 与
    `verify_aggregate` 只收窗内证明；聚合验证失败仅在窗内按序回退单证
    验证。
  - `result` 为 `BatchVerificationResult`：`batch_id` 原样回填，
    `aggregate_count` 等于窗口总数，`passed`/`failed`/`failures` 均按
    证明计数并沿用 `Failure`。
  - `windows` 按分组首次出现序、组内按窗序排列；每项
    （`WindowVerificationReport`，固定键序 `to_dict`）含 `group_id`、
    `window_index`、窗内按原序的 `proof_ids`、`aggregate_call_status`、
    `aggregate_verify_status`、`fell_back` 与逐证
    `ProofVerificationDetail`；不含 `proof` 与 `public_inputs`。
  - `max_group_size` 必须是 1 到 `MAX_BATCH_ITEMS` 闭区间内的整数
    （`bool` 不算整数），否则抛 `InvalidAggregationLimitError`，且不解析、
    不调用任何验证器。空批次、字段错误、重复 `proof_id`、未知 protocol、
    契约违约沿用原异常；某窗口 `aggregate` 抛 `IncompatibleAggregationError`
    时异常向调用方传播，其他聚合异常按该窗口 `aggregate` 失败定位到窗内
    每证，单证拒绝与异常沿用既有 `code`。既有入口与队列的
    result/report、`reverify_failures`、`reverify_groups` 与对账均不变。
- `VerificationQueue` —— 内存中的串行作业队列，不起线程、不联网、不落盘。
  作业执行时一次走完整的详细验证流水线并保存报告：
  - `result(job_id)` 返回该次执行的 `BatchVerificationResult`；
  - `report(job_id)` 返回同一次执行的 `BatchVerificationReport`
    （`report(job_id).result is result(job_id)`），给出聚合调用、聚合验证、
    回退单证验证各阶段的组级状态；查询报告不再次调用验证器，也不改变作业
    状态。仅 completed 作业可取结果/报告，queued/running/cancelled 抛
    `ResultUnavailableError`，未知或因执行错误移除的作业抛 `UnknownJobError`。
  - `reverify_failures(job_id, verifiers=None)` 只挑出源作业结果中失败的
    证明（按源批次原序、`batch_id` 与证明材料原样保留）创建独立的 queued
    作业，并记录父子关系（复核作业 → 源作业）；重复复核得到不同 `job_id`，
    父子关系互不覆盖。源作业不存在抛 `UnknownJobError`，非 completed 抛
    `ResultUnavailableError`，无失败证明抛 `NoFailedProofError`。
  - `reverify_outcome(source_job_id, retry_job_id)` 只读对账复核结果，返回
    `ReverifyOutcomeReport`（固定键序 `to_dict`）：`source_job_id`、
    `retry_job_id`、`selected_proof_ids`、`items`、`recovered_proof_ids`、
    `still_failed_proof_ids`。对账范围严格沿用 `reverify_failures` 的选取，
    各列表与 `items` 均按源批次原序；每条明细（`ReverifyOutcomeItem`，固定
    键序 `to_dict`）含 `proof_id`、`outcome` 与 `before_*`/`after_*` 的
    `status`/`stage`/`code`/`message`。`before_status` 固定 `failed`，
    `before_*` 取源作业保存的失败定位；`after_*` 取复核定位——恢复项
    `after_status="passed"` 且定位为空，仍失败项取复核 `Failure` 的同名字段；
    `outcome` 只取 `recovered`/`still_failed`。对账只读已保存结果，不调用
    验证器、不创建作业、不改变状态，重复查询一致，不含证明材料、
    `public_inputs`、调用栈或未公开验证器信息。作业不存在抛
    `UnknownJobError`，作业未 completed 抛 `ResultUnavailableError`，均
    completed 但 `retry_job_id` 不是 `source_job_id` 的直接复核作业抛
    `ReverifyLineageMismatchError`，三者互不替代。
  - `reverify_groups(job_id, group_ids, verifiers=None)` 按聚合组复核：
    `group_ids` 必须是非空列表/元组，元素为非空字符串且不重复，每个标识
    都须属于源作业的失败定位。选证按源批次序（组序取源批次中分组首次出现
    序、组内保持原序）取出选中组的**全部**证明，`batch_id`、材料与分组键
    原样保留，交给独立的 queued 作业走完整详细流水线；返回
    `GroupReverifySubmission`（`source_job_id`、`job_id`、
    `status="queued"`、按输入序的 `selected_group_ids`、按源批次序的
    `selected_proof_ids`，固定键序 `to_dict`）。异常顺序：源作业不存在
    `UnknownJobError`、未 completed `ResultUnavailableError`、选择非法或
    重复 `InvalidGroupSelectionError`、合法分组不属源失败定位
    `UnknownFailedGroupError`；失败不建作业、不改源作业。重复复核生成不同
    `job_id`，按组谱系与 `reverify_failures` 的谱系分开记录、互不覆盖。
  - `group_reverify_outcome(source_job_id, retry_job_id)` 只读对账按组
    复核结果，返回 `GroupReverifyOutcomeReport`（固定键序 `to_dict`）：
    双方 `source_status`/`retry_status`、按请求输入序的 `groups`、
    `recovered_group_ids`/`still_failed_group_ids` 与对应数量。每组
    （`GroupReverifyGroupItem`）含 `group_id`、按组内批次原序的 `proof_ids`、
    复核前后的 `aggregate_call_status`/`aggregate_verify_status`/
    `fell_back` 与每证（`GroupReverifyProofItem`）前后 `status`/`message`；
    源组失败且复核整组无失败证明记 `recovered`（含单证全过但整组归责的组
    已恢复），否则 `still_failed`。对账只读已保存报告，不调用验证器、不创建
    作业、不改变结果，重复查询一致，不含证明材料、`public_inputs`、调用栈
    或未公开验证器信息。作业不存在抛 `UnknownJobError`，未 completed 抛
    `ResultUnavailableError`，非直接 `reverify_groups` 谱系抛
    `GroupReverifyLineageMismatchError`，三者及与
    `ReverifyLineageMismatchError` 均互不替代。
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
    不混用其他异常；最后检查 `priority`，非法抛 `InvalidPriorityError`。
    `priority` 只接受 0 到 100 闭区间内的整数（`bool` 不算整数），省略时
    为 0；只作用于新任务，非法时不生成任务、不进入队列。
  - `run_next()` / `run_task(task_id)` 逐项验证：`run_next()` 消费
    **最高优先级**的 queued 任务（`priority` 数值越大越先消费，同值按
    入队先后先入先出；取消或进入终态的任务不再参与排序），`run_task`
    仍只消费指定的 queued 任务、不改动其他任务的位置。单个证明失败不
    中止同批其他项，每项只验证一次。`progress(task_id)` 返回
    `TaskProgress`：`status` 与真实 `completed`/`total` 进度，任务未结束时
    `result` 为 `None`，不提前给出最终结果。
  - `schedule()` 返回只读的 `TaskScheduleReport`（固定键序 `to_dict`：
    `queued_count`、`entries`）：`entries` 只含 queued 任务并按执行顺序
    （优先级降序、同值按入队先后）排列，每条 `TaskScheduleEntry`（固定
    键序 `to_dict`）含 `task_id`、`priority`、`queue_position`（从 1
    开始）、`total`（等于条目数，报告 `queued_count` 同值）；空队列
    返回 `queued_count=0`、`entries=[]`。重复查询不调用验证器、不消费、
    不改变任何任务的状态或结果。
  - 终态结果 `result(task_id)` 返回 `BatchTaskResult`：`total`/`passed`/
    `failed`、与输入顺序一致的 `results`（每项含 `index` 输入序号、
    `item_id`、`status`（`passed`/`failed`）、`stage`、稳定 `code` 与供
    人工定位的 `message`），以及按输入顺序的 `failed_item_ids`。单证被拒
    的失败项为 `stage="verify"`、`code="rejected"`。
  - 状态机为 `queued → processing → completed | failed`，另有
    `queued → cancelled`。查询不存在的任务
    抛 `TaskNotFoundError`；任务尚未结束时取结果、重复消费任务（含处理中
    再次 `run_next`/`run_task`）、终态后再次入队/消费等状态冲突抛
    `TaskStateConflictError`。
  - `cancel(task_id)` 取消 queued 任务，返回 `TaskCancellationReceipt`
    （固定键序 `to_dict`：`task_id`、`status="cancelled"`、`total`、
    `completed=0`、`cancelled_item_ids` 按输入顺序全量保留）。取消后的
    任务不再被 `run_next`/`run_task` 消费、不调用验证器；查询只读：
    `status` 为 `cancelled`，`progress` 的 `completed` 为 0 且 `result`
    为 `None`，`result` 抛 `TaskStateConflictError`，`task_error` 为
    `None`。未知任务抛 `TaskNotFoundError`；processing/completed/failed
    或重复取消抛 `TaskStateConflictError`，任务数据不变。回执不含证明
    材料、`public_inputs`、调用栈或验证器信息。
  - 系统无法读取证明材料（`stage="proof_read"`、`code="invalid_proof"`）、
    验证器执行失败（`stage="verify"`、`code="verifier_fault"`；协议无验证器
    为 `code="verifier_unavailable"`）或无法保存最终结果
    （`stage="result_save"`、`code="result_save_fault"`，由可选的
    `result_store` 钩子触发）时抛 `VerificationInfrastructureError`，任务
    进入 `failed` 终态，并保留此前已完成项及其定位信息（部分结果的 `total`
    仍为整批总数）；错误对象固定携带 `index`/`item_id`/`stage`/`code`。
  - 终态与结果查询保持幂等：不再次调用验证器、不改变状态，重复查询返回同一
    结果对象；定位信息绝不包含完整证明材料、内部调用栈或未公开验证器信息。
  - `retry_failed(task_id, verifiers=None)` 从 completed/failed/cancelled
    源任务选取
    待复核项，按根任务输入顺序创建独立的 queued 任务，返回
    `TaskRetrySubmission`（`source_task_id`、`task_id`、`status="queued"`、
    `summary`、`retried_indexes` 根任务输入零起下标）。completed 只选
    `status="failed"` 的项；failed 从保留的
    `VerificationInfrastructureError.index` 对应项起，纳入该错误项及其后
    尚无结果的项，再与已有失败项按根任务输入顺序去重（`result_save` 失败
    且结果已覆盖全部输入时只选失败项）；cancelled 的全部项均未验证，
    整批按根任务输入顺序重排。新任务沿用
    `run_next`/`run_task`/`status`/`progress`/`result`/`task_error`，每项只
    验证一次，`ItemResult.index` 与再次失败的定位都保留根任务下标；重复
    复核生成不同任务标识，源任务状态、结果与错误定位不变。`verifiers` 只
    作用于新任务，省略时继承源任务的验证器选择。`priority` 同样只作用
    于新任务：省略时继承源任务的优先级，显式值必须是 0-100 闭区间整数
    （`bool` 不算），非法抛 `InvalidPriorityError`，不建任务也不改源
    任务（在任务存在、已终结且确有可复核项之后才检查）。无可复核项抛
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
    未入选项不进入明细。源任务或复核任务为 cancelled 时，该侧无结论项的
    `before_status`/`after_status` 为 `unresolved` 且 `stage`/`code`/
    `message` 为空，结论记 `still_unresolved`。对账不调用验证器、不创建任务、不改变状态或结果，
    重复查询一致，不含证明材料、`public_inputs`、调用栈或未公开验证器信息。
    任务不存在抛 `TaskNotFoundError`，任务未终结抛
    `TaskStateConflictError`，复核任务不是源任务的直接 `retry_failed`
    任务抛 `TaskLineageMismatchError`，三者互不替代。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
