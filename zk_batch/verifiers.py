"""验证器契约与注册中心。

调用方提供的验证器需实现四个方法（均为同步、无额外副作用要求）：

- normalize(proof) -> Proof
    校验/规范化单证。类型不符等问题应抛出异常；verify_batch 会把异常
    归入 stage=normalize 的失败项。原样返回入参亦合法。
- aggregate(proofs) -> aggregate
    把同组（protocol/circuit_id/aggregation_key 相同）的单证聚合成
    一个不透明聚合对象。若数据确定无法聚合，应抛出
    IncompatibleAggregationError（verify_batch 原样上抛，整批失败）。
- verify_aggregate(aggregate, proofs, public_inputs) -> bool
    验证聚合对象，必须返回纯 bool。返回 False 时 verify_batch 会
    降级为逐单证验证以定位失败单证；返回非 bool 触发 VerifierContractError。
- verify(public_inputs, proof) -> bool
    单证验证，必须返回纯 bool。

ZKVerifier 是 protocol -> 验证器的注册中心，同时记录该协议是否支持聚合。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .errors import IncompatibleAggregationError
from .models import Proof

#: 验证器契约要求的全部方法及其参数形态（仅用于文档与错误信息）。
VERIFIER_METHODS = ("normalize", "aggregate", "verify_aggregate", "verify")


@runtime_checkable
class Verifier(Protocol):
    """调用方验证器的结构化协议。"""

    def normalize(self, proof: Proof) -> Proof: ...

    def aggregate(self, proofs: list[Proof]) -> Any: ...

    def verify_aggregate(
        self,
        aggregate: Any,
        proofs: list[Proof],
        public_inputs: list[Any],
    ) -> bool: ...

    def verify(self, public_inputs: Any, proof: Any) -> bool: ...


@dataclass(frozen=True)
class _Registration:
    verifier: Any
    aggregatable: bool


class ZKVerifier:
    """protocol 到验证器实例的注册中心。

    用法::

        registry = ZKVerifier()
        registry.register("groth16", MyVerifier())
        verify_batch(batch, registry)
    """

    def __init__(self) -> None:
        self._registry: dict[str, _Registration] = {}

    def register(
        self,
        protocol: str,
        verifier: Any,
        *,
        aggregatable: bool = True,
    ) -> None:
        """注册一个证明系统的验证器。

        - protocol:      Proof.protocol 使用的名称
        - verifier:      满足 Verifier 契约的对象
        - aggregatable:  该协议是否支持聚合验证；False 时同组单证直接走
                         逐单证验证，不调用 aggregate/verify_aggregate
        """
        if not isinstance(protocol, str) or not protocol:
            raise ValueError("protocol must be a non-empty string")
        if protocol in self._registry:
            raise ValueError(f"protocol {protocol!r} is already registered")
        self._registry[protocol] = _Registration(verifier, aggregatable)

    def unregister(self, protocol: str) -> None:
        self._registry.pop(protocol, None)

    def is_supported(self, protocol: str) -> bool:
        return protocol in self._registry

    def supports_aggregation(self, protocol: str) -> bool:
        return self._registry[protocol].aggregatable

    def get(self, protocol: str) -> Any:
        return self._registry[protocol].verifier

    def protocols(self) -> tuple[str, ...]:
        return tuple(sorted(self._registry))


class DefaultVerifier:
    """内置参考验证器，使 zk_batch 开箱即可自测。

    约定一套极简且确定的规则：

    - normalize: proof 必须是非空 bytes；public_inputs 必须是 int 的
      list/tuple。否则抛 ValueError。
    - aggregate: 同组 proof 长度必须一致，否则抛 IncompatibleAggregationError；
      聚合体为 (proofs, 拼接字节) 二元组。
    - verify_aggregate: 全部单证均不等于 b"invalid" 即通过。
    - verify: proof != b"invalid"。
    """

    def normalize(self, proof: Proof) -> Proof:
        blob = proof.proof
        if not isinstance(blob, (bytes, bytearray)) or len(blob) == 0:
            raise ValueError("proof must be non-empty bytes")
        inputs = proof.public_inputs
        if not isinstance(inputs, (list, tuple)) or not all(
            isinstance(x, int) and not isinstance(x, bool) for x in inputs
        ):
            raise ValueError("public_inputs must be a list/tuple of ints")
        return proof

    def aggregate(self, proofs: list[Proof]) -> tuple[list[Proof], bytes]:
        lengths = {len(bytes(p.proof)) for p in proofs}
        if len(lengths) > 1:
            raise IncompatibleAggregationError(
                f"proof length mismatch within group: {sorted(lengths)}"
            )
        return list(proofs), b"".join(bytes(p.proof) for p in proofs)

    def verify_aggregate(
        self,
        aggregate: Any,
        proofs: list[Proof],
        public_inputs: list[Any],
    ) -> bool:
        members, _merged = aggregate
        if len(members) != len(proofs) or len(proofs) != len(public_inputs):
            return False
        return all(bytes(p.proof) != b"invalid" for p in proofs)

    def verify(self, public_inputs: Any, proof: Any) -> bool:
        return bytes(proof) != b"invalid"


def ensure_bool(value: Any, *, protocol: str, method: str) -> bool:
    """验证器布尔返回值的契约检查。"""
    from .errors import VerifierContractError

    if not isinstance(value, bool):
        raise VerifierContractError(
            f"verifier {protocol!r}.{method} must return bool, "
            f"got {type(value).__name__}",
            protocol=protocol,
            method=method,
        )
    return value


def check_contract(protocol: str, verifier: Any) -> None:
    """验证器方法完备性的契约检查，缺方法即抛 VerifierContractError。"""
    from .errors import VerifierContractError

    for name in VERIFIER_METHODS:
        attr = getattr(verifier, name, None)
        if attr is None or not callable(attr):
            raise VerifierContractError(
                f"verifier for protocol {protocol!r} is missing callable "
                f"method {name!r}",
                protocol=protocol,
                method=name,
            )


def default_registry() -> ZKVerifier:
    """构造一个只含内置 DefaultVerifier（协议名 groth16）的注册中心。"""
    registry = ZKVerifier()
    registry.register("groth16", DefaultVerifier(), aggregatable=True)
    return registry
