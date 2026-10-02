"""zk_batch 的数据结构。

全部使用 dataclass(frozen=True)，对调用方只读；批次与单证按原样透传给验证器，
本模块不复制、不规范化其内部内容。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# 失败阶段，按 verify_batch 的执行顺序：
# normalize         单证规范化（由验证器完成）
# aggregate         整组聚合
# aggregate_verify  聚合结果验证
# single_verify     降级单证验证
STAGE_NORMALIZE = "normalize"
STAGE_AGGREGATE = "aggregate"
STAGE_AGGREGATE_VERIFY = "aggregate_verify"
STAGE_SINGLE_VERIFY = "single_verify"


@dataclass(frozen=True)
class Proof:
    """单个零知识证明（调用方提供，原样透传）。

    - proof_id:      批次内唯一标识
    - protocol:      证明系统名，也是分组键之一
    - circuit_id:    电路标识，分组键之一
    - aggregation_key: 聚合键，分组键之一
    - public_inputs: 公开输入，原样透传
    - proof:         证明体，原样透传
    """

    proof_id: str
    protocol: str
    circuit_id: str
    aggregation_key: str
    public_inputs: Any
    proof: Any


@dataclass(frozen=True)
class Batch:
    """一批待验证证明。"""

    batch_id: str
    proofs: tuple[Proof, ...]


@dataclass(frozen=True)
class Failure:
    """单个证明的失败定位信息。"""

    proof_id: str
    group_id: str
    stage: str
    code: str
    message: str


@dataclass(frozen=True)
class BatchVerificationResult:
    """整批验证结果。

    - batch_id:       与入参一致
    - aggregate_count: 实际走聚合验证路径的分组数
    - passed:         通过的 proof_id 列表（按 proof_id 排序）
    - failed:         失败的 proof_id 列表（按 proof_id 排序）
    - failures:       失败明细，按 proof_id 排序
    """

    batch_id: str
    aggregate_count: int
    passed: tuple[str, ...]
    failed: tuple[str, ...]
    failures: tuple[Failure, ...] = field(default=())
