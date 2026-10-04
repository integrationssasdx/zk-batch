"""批量验证引擎。

流程（错误检测严格按需求顺序，靠前的错误优先抛出）：

1. 批次非空（``batch_id`` 与 ``proofs``）——否则 EmptyBatchError。
2. 逐证归一化字段 ——缺字段/类型错抛 InvalidProofError。
3. ``proof_id`` 唯一 ——重复抛 DuplicateProofIdError。
4. 按 (protocol, circuit_id, aggregation_key) 分组。
5. 每组解析验证器，未知 protocol 抛 UnsupportedProofSystemError。
6. 验证器契约：aggregate / verify_aggregate / verify 必须被实现、
   可调用，布尔返回值必须是 ``bool`` ——否则 VerifierContractError。
7. 逐组聚合验证：

   * ``aggregate`` 抛 IncompatibleAggregationError ——整批抛出；
   * ``aggregate`` 抛其他异常 ——组内单证记 stage=aggregate；
   * 聚合验证通过 ——整组通过；
   * 聚合验证返回 False 或抛异常 ——按原序回退单证验证，
     单证 False 记 rejected、抛异常记 verify_error（stage=single_verify）；
     若单证全部通过但整组失败，按聚合阶段归责到组内每证。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .errors import (
    DuplicateProofIdError,
    EmptyBatchError,
    IncompatibleAggregationError,
    InvalidProofError,
    UnsupportedProofSystemError,
    VerifierContractError,
)
from .models import (
    AGG_CALL_ERROR,
    AGG_CALL_SUCCEEDED,
    AGG_VERIFY_ERROR,
    AGG_VERIFY_NOT_RUN,
    AGG_VERIFY_PASSED,
    AGG_VERIFY_REJECTED,
    CODE_REJECTED,
    CODE_VERIFY_ERROR,
    SINGLE_NOT_RUN,
    SINGLE_PASSED,
    SINGLE_REJECTED,
    SINGLE_ERROR,
    STAGE_AGGREGATE,
    STAGE_AGGREGATE_VERIFY,
    STAGE_SINGLE_VERIFY,
    BatchVerificationReport,
    BatchVerificationResult,
    Failure,
    GroupKey,
    GroupVerificationReport,
    Proof,
    ProofVerificationDetail,
)
from .verifier import ZKVerifier

_REQUIRED_FIELDS = (
    "proof_id",
    "protocol",
    "circuit_id",
    "aggregation_key",
    "public_inputs",
    "proof",
)
_REQUIRED_METHODS = ("aggregate", "verify_aggregate", "verify")


def verify_batch(batch: Any, verifiers: Any) -> BatchVerificationResult:
    """验证一个批次，返回 :class:`BatchVerificationResult`。

    ``batch`` 为含 ``batch_id`` 与 ``proofs`` 的映射；``verifiers`` 为
    :class:`ZKVerifier` 实例（单个、列表/元组）或 ``{protocol: verifier}`` 字典。
    """
    return _run_pipeline(batch, verifiers).result


def verify_batch_detailed(batch: Any, verifiers: Any) -> BatchVerificationReport:
    """验证一个批次并返回 :class:`BatchVerificationReport`。

    与 :func:`verify_batch` 走完全相同的校验与分组流水线：空批次、字段缺失
    或类型错误、重复 ``proof_id``、未知 protocol、验证器契约违约、
    :class:`IncompatibleAggregationError`（不能聚合）在两个入口抛出的异常
    类型与优先级完全一致；其中不能聚合时异常仍向调用方传播，不返回报告。
    报告中的 ``result`` 与 :func:`verify_batch` 的返回值同值。
    """
    return _run_pipeline(batch, verifiers).report


@dataclass
class _PipelineOutput:
    """一次流水线的内部产物：普通结果与其同值的详细报告。"""

    result: BatchVerificationResult
    report: BatchVerificationReport


def _run_pipeline(batch: Any, verifiers: Any) -> _PipelineOutput:
    batch_id, raw_proofs = _validate_batch(batch)
    proofs = _normalize_proofs(raw_proofs)
    _ensure_unique_ids(proofs)

    groups = _group_proofs(proofs)
    protocol_map = _resolve_verifiers(verifiers)

    failures: List[Failure] = []
    group_reports: List[GroupVerificationReport] = []
    for key, members in groups:
        # 逐组流水线。跨组的异常出现顺序与需求清单一致：
        # 不能聚合(前一组) -> 未知系统(后一组) -> 契约违约（调用点惰性暴露）。
        verifier = protocol_map.get(key[0])
        if verifier is None:
            raise UnsupportedProofSystemError(key[0])
        group_failures, group_report = _verify_group(key, members, verifier)
        failures.extend(group_failures)
        group_reports.append(group_report)

    # failures 按 proof_id 排序是对外结果契约；分组报告内保持批次原序，
    # 二者互不影响。
    failures.sort(key=lambda f: f.proof_id)
    total = len(proofs)
    failed = len(failures)
    result = BatchVerificationResult(
        batch_id=batch_id,
        aggregate_count=len(groups),
        passed=total - failed,
        failed=failed,
        failures=failures,
    )
    return _PipelineOutput(
        result=result,
        report=BatchVerificationReport(result=result, groups=group_reports),
    )


# ================================================================ 输入校验

def _validate_batch(batch: Any) -> Tuple[Any, Any]:
    if not isinstance(batch, Mapping):
        raise EmptyBatchError("batch must be a mapping with 'batch_id' and 'proofs'")
    if "batch_id" not in batch:
        raise EmptyBatchError("batch is missing 'batch_id'")
    raw_proofs = batch.get("proofs")
    if not isinstance(raw_proofs, (list, tuple)) or len(raw_proofs) == 0:
        raise EmptyBatchError("batch 'proofs' must be a non-empty list")
    return batch["batch_id"], raw_proofs


def _normalize_proofs(raw_proofs: Iterable[Any]) -> List[Proof]:
    normalized: List[Proof] = []
    for index, raw in enumerate(raw_proofs):
        if not isinstance(raw, Mapping):
            raise InvalidProofError(
                f"proof at index {index} must be a mapping"
            )
        missing = [name for name in _REQUIRED_FIELDS if name not in raw]
        if missing:
            raise InvalidProofError(
                f"proof at index {index} missing fields: {', '.join(missing)}"
            )

        proof_id = raw["proof_id"]
        if not isinstance(proof_id, str) or not proof_id:
            raise InvalidProofError(
                f"proof at index {index}: 'proof_id' must be a non-empty str"
            )
        for name in ("protocol", "circuit_id", "aggregation_key"):
            value = raw[name]
            if not isinstance(value, str) or not value:
                raise InvalidProofError(
                    f"proof {proof_id!r}: {name!r} must be a non-empty str"
                )
        public_inputs = raw["public_inputs"]
        if not isinstance(public_inputs, list):
            raise InvalidProofError(
                f"proof {proof_id!r}: 'public_inputs' must be a list"
            )
        # proof 内容对引擎不透明，只要求字段存在；原样保留。
        normalized.append(
            Proof(
                proof_id=proof_id,
                protocol=raw["protocol"],
                circuit_id=raw["circuit_id"],
                aggregation_key=raw["aggregation_key"],
                public_inputs=public_inputs,
                proof=raw["proof"],
                index=index,
            )
        )
    return normalized


def _ensure_unique_ids(proofs: List[Proof]) -> None:
    seen: Dict[str, Proof] = {}
    for p in proofs:
        if p.proof_id in seen:
            raise DuplicateProofIdError(p.proof_id)
        seen[p.proof_id] = p


def _group_proofs(
    proofs: List[Proof],
) -> List[Tuple[GroupKey, List[Proof]]]:
    """按首次出现序返回 (分组键, 组成员) 列表；组内保持批次原序。"""
    groups: "Dict[GroupKey, List[Proof]]" = {}
    for p in proofs:
        groups.setdefault(p.group_key, []).append(p)
    return list(groups.items())


# ================================================================ 验证器解析

def _resolve_verifiers(verifiers: Any) -> Dict[str, ZKVerifier]:
    if isinstance(verifiers, Mapping):
        protocol_map: Dict[str, ZKVerifier] = {}
        for name, verifier in verifiers.items():
            if not isinstance(verifier, ZKVerifier):
                raise VerifierContractError(
                    f"verifier registered for {name!r} is not a ZKVerifier"
                )
            protocol_map[str(name)] = verifier
        return protocol_map

    if isinstance(verifiers, ZKVerifier):
        verifiers = [verifiers]

    protocol_map = {}
    try:
        iterator = iter(verifiers)
    except TypeError:
        raise VerifierContractError(
            "verifiers must be a ZKVerifier, a list of them, or a protocol mapping"
        )
    for verifier in iterator:
        if not isinstance(verifier, ZKVerifier):
            raise VerifierContractError(
                f"{verifier!r} is not a ZKVerifier instance"
            )
        names = verifier.supported_protocols()
        if not names:
            raise VerifierContractError(
                f"{type(verifier).__name__} declares no supported protocol"
            )
        for name in names:
            protocol_map[name] = verifier
    return protocol_map


def _check_contract(verifier: ZKVerifier) -> None:
    """三个方法全部实现（完整检查；正常流程里改为调用点惰性检查）。"""
    for name in _REQUIRED_METHODS:
        _require_method(verifier, name)


def _require_method(verifier: ZKVerifier, name: str) -> None:
    """在单个调用点惰性检查某方法存在且被子类实现。"""
    method = getattr(verifier, name, None)
    if method is None or not callable(method):
        raise VerifierContractError(
            f"{type(verifier).__name__} is missing method {name!r}"
        )
    # 未覆盖基类方法等价于缺方法（基类方法只会抛 NotImplementedError）
    underlying = getattr(method, "__func__", None)
    if underlying is getattr(ZKVerifier, name):
        raise VerifierContractError(
            f"{type(verifier).__name__} must implement {name!r}"
        )


def _require_bool(value: Any, verifier: ZKVerifier, what: str) -> bool:
    if not isinstance(value, bool):
        raise VerifierContractError(
            f"{type(verifier).__name__}.{what} returned non-bool: {value!r}"
        )
    return value


# ================================================================ 分组验证

def _verify_group(
    key: GroupKey, members: List[Proof], verifier: ZKVerifier
) -> Tuple[List[Failure], GroupVerificationReport]:
    group_id = f"{key[0]}:{key[1]}:{key[2]}"
    proof_ids = [proof.proof_id for proof in members]

    # ---- 聚合 -----------------------------------------------------------
    # 惰性契约检查：缺 aggregate 是 VerifierContractError，不进入归责流程。
    _require_method(verifier, "aggregate")
    try:
        aggregated = verifier.aggregate(members)
    except IncompatibleAggregationError:
        raise
    except Exception as exc:  # noqa: BLE001 - 归责到组内每证，不炸整批
        message = _exc_message(exc)
        failures = _group_wide_failures(
            members, STAGE_AGGREGATE, CODE_VERIFY_ERROR, message
        )
        report = GroupVerificationReport(
            group_id=group_id,
            proof_ids=proof_ids,
            aggregate_call_status=AGG_CALL_ERROR,
            aggregate_verify_status=AGG_VERIFY_NOT_RUN,
            fell_back=False,
            proofs=[
                ProofVerificationDetail(
                    proof_id=proof.proof_id,
                    status=SINGLE_NOT_RUN,
                    message=message,
                )
                for proof in members
            ],
        )
        return failures, report

    # ---- 整组验证 -------------------------------------------------------
    _require_method(verifier, "verify_aggregate")
    try:
        ok = _require_bool(
            verifier.verify_aggregate(members, aggregated),
            verifier,
            "verify_aggregate",
        )
        aggregate_error: Optional[Exception] = None
    except VerifierContractError:
        raise
    except Exception as exc:  # noqa: BLE001
        ok = False
        aggregate_error = exc

    if ok:
        report = GroupVerificationReport(
            group_id=group_id,
            proof_ids=proof_ids,
            aggregate_call_status=AGG_CALL_SUCCEEDED,
            aggregate_verify_status=AGG_VERIFY_PASSED,
            fell_back=False,
            proofs=[
                ProofVerificationDetail(
                    proof_id=proof.proof_id,
                    status=SINGLE_PASSED,
                    message="",
                )
                for proof in members
            ],
        )
        return [], report

    # ---- 回退：按批次原序逐证验证 ---------------------------------------
    # 进入回退才需要 verify；缺方法按 VerifierContractError 直接抛出。
    _require_method(verifier, "verify")
    failures: List[Failure] = []
    details: List[ProofVerificationDetail] = []
    for proof in members:
        try:
            single_ok = _require_bool(verifier.verify(proof), verifier, "verify")
        except VerifierContractError:
            raise
        except Exception as exc:  # noqa: BLE001
            message = _exc_message(exc)
            failures.append(
                Failure(
                    proof_id=proof.proof_id,
                    group_id=group_id,
                    stage=STAGE_SINGLE_VERIFY,
                    code=CODE_VERIFY_ERROR,
                    message=message,
                )
            )
            details.append(
                ProofVerificationDetail(
                    proof_id=proof.proof_id,
                    status=SINGLE_ERROR,
                    message=message,
                )
            )
            continue
        if single_ok:
            details.append(
                ProofVerificationDetail(
                    proof_id=proof.proof_id,
                    status=SINGLE_PASSED,
                    message="",
                )
            )
        else:
            failures.append(
                Failure(
                    proof_id=proof.proof_id,
                    group_id=group_id,
                    stage=STAGE_SINGLE_VERIFY,
                    code=CODE_REJECTED,
                    message="proof rejected by verifier",
                )
            )
            details.append(
                ProofVerificationDetail(
                    proof_id=proof.proof_id,
                    status=SINGLE_REJECTED,
                    message="proof rejected by verifier",
                )
            )

    if failures:
        # 回退后存在被拒/异常的单证
        report = GroupVerificationReport(
            group_id=group_id,
            proof_ids=proof_ids,
            aggregate_call_status=AGG_CALL_SUCCEEDED,
            aggregate_verify_status=(
                AGG_VERIFY_ERROR if aggregate_error is not None
                else AGG_VERIFY_REJECTED
            ),
            fell_back=True,
            proofs=details,
        )
        return failures, report

    # 单证全部通过但整组失败：归责到聚合阶段。逐证状态仍为 passed；
    # failures 的归责消息来自聚合阶段（异常或被拒），不与逐证消息混用。
    if aggregate_error is not None:
        failures = _group_wide_failures(
            members,
            STAGE_AGGREGATE_VERIFY,
            CODE_VERIFY_ERROR,
            _exc_message(aggregate_error),
        )
        verify_status = AGG_VERIFY_ERROR
    else:
        failures = _group_wide_failures(
            members,
            STAGE_AGGREGATE_VERIFY,
            CODE_REJECTED,
            "aggregate verification rejected the group but all single proofs passed",
        )
        verify_status = AGG_VERIFY_REJECTED
    report = GroupVerificationReport(
        group_id=group_id,
        proof_ids=proof_ids,
        aggregate_call_status=AGG_CALL_SUCCEEDED,
        aggregate_verify_status=verify_status,
        fell_back=True,
        proofs=details,
    )
    return failures, report


def _group_wide_failures(
    members: List[Proof], stage: str, code: str, message: str
) -> List[Failure]:
    return [
        Failure(
            proof_id=proof.proof_id,
            group_id=proof.group_id,
            stage=stage,
            code=code,
            message=message,
        )
        for proof in members
    ]


def _exc_message(exc: Exception) -> str:
    text = str(exc)
    prefix = type(exc).__name__
    return f"{prefix}: {text}" if text else prefix
