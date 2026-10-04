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

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
