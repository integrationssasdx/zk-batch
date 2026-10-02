"""verify_batch：按分组键聚合验证整组，失败时降级为逐单证定位。

异常阶段（任一触发则整次调用抛出，不产生结果），按检查顺序：

1. EmptyBatchError              proofs 缺失或为空
2. InvalidProofError            批次/单证字段缺失或类型不对
3. DuplicateProofIdError        proof_id 重复
4. IncompatibleAggregationError 验证器表明整组不可聚合（任何验证器方法
   主动抛出都会原样传播）
5. UnsupportedProofSystemError  protocol 未在 ZKVerifier 注册
6. VerifierContractError        验证器缺方法，或 verify_aggregate/verify
   返回非 bool

运行期分组处理（异常不致命时进入 failures）：

- normalize 抛异常（IncompatibleAggregationError 除外）→
  failure(stage=normalize)，该单证不参与本组聚合。
- 协议注册为不可聚合（aggregatable=False）→ 直接逐单证 verify。
- aggregate 抛 IncompatibleAggregationError → 原样上抛；
  抛其他异常 → 组内单证 failure(stage=aggregate)，整组失败闭环。
- verify_aggregate 返回 True → 整组通过。
- verify_aggregate 返回 False → 按组内顺序逐单证 verify 定位：
  通过者 passed，否则 failure(stage=single_verify)。
- verify_aggregate 抛其他异常 → 同样逐单证 verify；单证复现失败的记
  single_verify，单证通过但聚合态仍异常的 fail-closed 记
  aggregate_verify（宁可误报不可漏验）。
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

from .errors import (
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidProofError,
    UnsupportedProofSystemError,
)
from .models import (
    Batch,
    BatchVerificationResult,
    Failure,
    Proof,
    STAGE_AGGREGATE,
    STAGE_AGGREGATE_VERIFY,
    STAGE_NORMALIZE,
    STAGE_SINGLE_VERIFY,
)
from .verifiers import (
    ZKVerifier,
    check_contract,
    default_registry,
    ensure_bool,
)

#: failure.code 取值
CODE_NORMALIZE_ERROR = "normalize_error"
CODE_AGGREGATE_ERROR = "aggregate_error"
CODE_AGGREGATE_VERIFY_ERROR = "aggregate_verify_error"
CODE_VERIFY_FAILED = "verify_failed"
CODE_VERIFY_ERROR = "verify_error"

_REQUIRED_STR_FIELDS = (
    "proof_id",
    "protocol",
    "circuit_id",
    "aggregation_key",
)
_REQUIRED_ANY_FIELDS = ("public_inputs", "proof")


def _message(exc: BaseException) -> str:
    text = str(exc)
    return text if text else type(exc).__name__


def _validate_envelope(batch: Any) -> tuple[str, list[Any]]:
    """校验批次外层结构，返回 (batch_id, 原始单证列表)。"""
    if isinstance(batch, Batch):
        if not batch.proofs:
            raise EmptyBatchError()
        return batch.batch_id, list(batch.proofs)

    if not isinstance(batch, Mapping):
        raise InvalidProofError("batch must be a mapping")

    if "proofs" not in batch:
        raise EmptyBatchError("batch is missing 'proofs'")
    raw_proofs = batch["proofs"]
    if not isinstance(raw_proofs, (list, tuple)) or isinstance(raw_proofs, (str, bytes)):
        raise InvalidProofError(
            "batch 'proofs' must be a list or tuple", field="proofs"
        )
    if len(raw_proofs) == 0:
        raise EmptyBatchError()

    if "batch_id" not in batch:
        raise InvalidProofError("batch is missing 'batch_id'", field="batch_id")
    batch_id = batch["batch_id"]
    if not isinstance(batch_id, str) or not batch_id:
        raise InvalidProofError(
            "'batch_id' must be a non-empty string", field="batch_id"
        )

    return batch_id, list(raw_proofs)


def _validate_proofs(raw_proofs: list[Any]) -> list[Proof]:
    """逐单证做字段/类型校验，再做 proof_id 去重。"""
    proofs: list[Proof] = []
    for index, raw in enumerate(raw_proofs):
        # 来自 Batch 对象的已是 Proof 实例（构造时即定型），原样接受。
        if isinstance(raw, Proof):
            proofs.append(raw)
            continue
        if not isinstance(raw, Mapping):
            raise InvalidProofError(
                f"proof at index {index} must be a mapping",
                proof_id=None,
            )

        known_id = raw.get("proof_id") if hasattr(raw, "get") else None
        pid = known_id if isinstance(known_id, str) else None

        for field_name in (*_REQUIRED_STR_FIELDS, *_REQUIRED_ANY_FIELDS):
            if field_name not in raw:
                raise InvalidProofError(
                    f"proof at index {index} is missing field {field_name!r}",
                    proof_id=pid,
                    field=field_name,
                )

        for field_name in _REQUIRED_STR_FIELDS:
            value = raw[field_name]
            if not isinstance(value, str) or not value:
                raise InvalidProofError(
                    f"proof field {field_name!r} must be a non-empty string",
                    proof_id=pid,
                    field=field_name,
                )

        # public_inputs 与 proof 类型不做约束，原样透传给验证器。
        proofs.append(
            Proof(
                proof_id=raw["proof_id"],
                protocol=raw["protocol"],
                circuit_id=raw["circuit_id"],
                aggregation_key=raw["aggregation_key"],
                public_inputs=raw["public_inputs"],
                proof=raw["proof"],
            )
        )

    seen: set[str] = set()
    for proof in proofs:
        if proof.proof_id in seen:
            raise DuplicateProofIdError(proof.proof_id)
        seen.add(proof.proof_id)

    return proofs


def _group_proofs(
    proofs: list[Proof],
) -> list[tuple[tuple[str, str, str], str, list[Proof]]]:
    """按 (protocol, circuit_id, aggregation_key) 分组，保持出现顺序。"""
    groups: "OrderedDict[tuple[str, str, str], list[Proof]]" = OrderedDict()
    for proof in proofs:
        key = (proof.protocol, proof.circuit_id, proof.aggregation_key)
        groups.setdefault(key, []).append(proof)
    return [
        (key, f"{key[0]}:{key[1]}:{key[2]}", members)
        for key, members in groups.items()
    ]


def _single_verify_all(
    verifier: Any,
    protocol: str,
    group_id: str,
    members: list[Proof],
    failures: list[Failure],
    passed: list[str],
) -> None:
    """逐单证验证，结果直接归入 passed / failures。"""
    for proof in members:
        try:
            verdict = verifier.verify(proof.public_inputs, proof.proof)
        except IncompatibleAggregationError:
            raise
        except Exception as exc:  # 验证器自身报错：定位到单证
            failures.append(
                Failure(
                    proof.proof_id,
                    group_id,
                    STAGE_SINGLE_VERIFY,
                    CODE_VERIFY_ERROR,
                    _message(exc),
                )
            )
            continue
        ensure_bool(verdict, protocol=protocol, method="verify")
        if verdict:
            passed.append(proof.proof_id)
        else:
            failures.append(
                Failure(
                    proof.proof_id,
                    group_id,
                    STAGE_SINGLE_VERIFY,
                    CODE_VERIFY_FAILED,
                    "verifier returned False",
                )
            )


def verify_batch(
    batch: Any,
    verifiers: ZKVerifier | None = None,
) -> BatchVerificationResult:
    """聚合验证一个批次，返回 BatchVerificationResult。

    batch 可为 ``{"batch_id": ..., "proofs": [...]}`` 形式的映射，
    也可是本包的 :class:`~zk_batch.models.Batch`。verifiers 为按
    protocol 注册的 :class:`~zk_batch.verifiers.ZKVerifier`；省略时
    使用内置 DefaultVerifier（protocol 名 ``groth16``）。
    """
    registry = verifiers if verifiers is not None else default_registry()

    batch_id, raw_proofs = _validate_envelope(batch)
    proofs = _validate_proofs(raw_proofs)
    groups = _group_proofs(proofs)

    # 未知系统与契约检查先于任何验证执行，按分组首次出现顺序报告。
    seen_protocols: set[str] = set()
    for (protocol, _circuit, _agg_key), _gid, _members in groups:
        if protocol in seen_protocols:
            continue
        seen_protocols.add(protocol)
        if not registry.is_supported(protocol):
            raise UnsupportedProofSystemError(protocol)
        check_contract(protocol, registry.get(protocol))

    failures: list[Failure] = []
    passed: list[str] = []
    aggregate_count = 0

    for (protocol, _circuit, _agg_key), group_id, members in groups:
        verifier = registry.get(protocol)

        # 1) normalize 阶段：失败单证定位并从聚合成员中剔除。
        normalized: list[Proof] = []
        for proof in members:
            try:
                normalized_proof = verifier.normalize(proof)
            except IncompatibleAggregationError:
                raise
            except Exception as exc:
                failures.append(
                    Failure(
                        proof.proof_id,
                        group_id,
                        STAGE_NORMALIZE,
                        CODE_NORMALIZE_ERROR,
                        _message(exc),
                    )
                )
                continue
            normalized.append(normalized_proof)

        if not normalized:
            # 整组都在 normalize 阶段失败，无聚合可做。
            continue

        # 2) 协议显式不支持聚合：直接逐单证验证。
        if not registry.supports_aggregation(protocol):
            _single_verify_all(
                verifier, protocol, group_id, normalized, failures, passed
            )
            continue

        # 3) 聚合阶段。
        aggregate_count += 1
        try:
            aggregate = verifier.aggregate(normalized)
        except IncompatibleAggregationError:
            raise
        except Exception as exc:
            # 聚合体构造失败：整组 fail-closed，不做单证降级
            # （规格只要求“聚合验证失败”才降级）。
            for proof in normalized:
                failures.append(
                    Failure(
                        proof.proof_id,
                        group_id,
                        STAGE_AGGREGATE,
                        CODE_AGGREGATE_ERROR,
                        _message(exc),
                    )
                )
            continue

        public_inputs = [proof.public_inputs for proof in normalized]
        try:
            verdict = verifier.verify_aggregate(
                aggregate, normalized, public_inputs
            )
        except IncompatibleAggregationError:
            raise
        except Exception as exc:
            # 聚合验证器自身异常：逐单证复现定位；单证无法解释的
            # 成员 fail-closed 记 aggregate_verify。
            group_error = exc
            for proof in normalized:
                try:
                    single = verifier.verify(proof.public_inputs, proof.proof)
                except IncompatibleAggregationError:
                    raise
                except Exception as single_exc:
                    failures.append(
                        Failure(
                            proof.proof_id,
                            group_id,
                            STAGE_SINGLE_VERIFY,
                            CODE_VERIFY_ERROR,
                            _message(single_exc),
                        )
                    )
                    continue
                ensure_bool(single, protocol=protocol, method="verify")
                if single:
                    failures.append(
                        Failure(
                            proof.proof_id,
                            group_id,
                            STAGE_AGGREGATE_VERIFY,
                            CODE_AGGREGATE_VERIFY_ERROR,
                            _message(group_error),
                        )
                    )
                else:
                    failures.append(
                        Failure(
                            proof.proof_id,
                            group_id,
                            STAGE_SINGLE_VERIFY,
                            CODE_VERIFY_FAILED,
                            "verifier returned False",
                        )
                    )
            continue

        ensure_bool(verdict, protocol=protocol, method="verify_aggregate")
        if verdict is True:
            passed.extend(proof.proof_id for proof in normalized)
            continue

        # 4) 聚合验证返回 False：按组内顺序逐单证验证以定位。
        _single_verify_all(
            verifier, protocol, group_id, normalized, failures, passed
        )

    failures.sort(key=lambda item: item.proof_id)
    passed.sort()
    failed = [item.proof_id for item in failures]

    return BatchVerificationResult(
        batch_id=batch_id,
        aggregate_count=aggregate_count,
        passed=tuple(passed),
        failed=tuple(failed),
        failures=tuple(failures),
    )
