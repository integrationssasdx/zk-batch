""":class:`ZKVerifier` 契约基类。

调用方继承本类并提供三个方法：

* ``aggregate(proofs)`` —— 把同组若干 :class:`~zk_batch.models.Proof`
  聚合成一个不透明的聚合证明对象。无法聚合时应抛
  :class:`~zk_batch.errors.IncompatibleAggregationError`。
* ``verify_aggregate(proofs, aggregated)`` —— 整组验证，必须返回 ``bool``。
* ``verify(proof)`` —— 单证验证，必须返回 ``bool``。

子类用类属性 ``protocol`` 或 ``protocols`` 声明自己支持的证明系统名，
对应批次证明里的 ``protocol`` 字段。

三个验证方法的布尔契约由引擎检查：缺方法、未实现或返回非布尔都会抛
:class:`~zk_batch.errors.VerifierContractError`。
"""

from __future__ import annotations

from typing import Any, List

from .models import Proof


class ZKVerifier:
    """具体证明系统验证器的契约基类。"""

    #: 子类覆盖：本验证器服务的 protocol 名
    protocol: str = ""
    #: 可选：一个验证器服务多个 protocol 名
    protocols: Any = ()

    def aggregate(self, proofs: List[Proof]) -> Any:
        """聚合同组证明。无法聚合时抛 IncompatibleAggregationError。"""
        raise NotImplementedError

    def verify_aggregate(self, proofs: List[Proof], aggregated: Any) -> bool:
        """验证聚合证明，必须返回布尔。"""
        raise NotImplementedError

    def verify(self, proof: Proof) -> bool:
        """验证单证，必须返回布尔。"""
        raise NotImplementedError

    def supported_protocols(self) -> tuple:
        """收集本实例声明的 protocol 名（实例属性优先）。"""
        names: List[str] = []
        multi = getattr(self, "protocols", ())
        if isinstance(multi, str):
            if multi:
                names.append(multi)
        else:
            try:
                names.extend(str(p) for p in multi)
            except TypeError:
                pass
        single = getattr(self, "protocol", "")
        if isinstance(single, str) and single:
            names.append(single)
        return tuple(dict.fromkeys(names))
