"""数据流转编排服务。

修复要点：
1. get_graph 不再按列表相邻位置连线，而是读取 topology_state 构建的流转 DAG 快照，
   每条边都必须来自某条管道声明的起始/中间/终止节点；
2. 状态只能沿业务顺序（草稿 -> 待校验 -> 已启用 -> 已停用）推进，切换、变更与回退
   都必须带原因并写入状态履历，回退不允许静默发生；
3. 停用或未通过校验的管道不产生可执行边，避免数据被送进没有发布的任务。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.topology import (
    CONFLICT_POLICIES,
    POLICY_LABELS,
    topology_state,
)
from app.store import store

MODULE = "flow"
REQUIRED_FIELDS = ["管道编号", "管道名称", "起始节点"]
# 节点声明字段：起始节点为必填，终止/中间用于完整声明校验
NODE_DECL_FIELDS = ["起始节点", "终止节点", "中间节点"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用管道": "已启用", "停用管道": "已停用"}
NEGATIVE_ACTIONS = ["停用管道"]
# 允许的前向状态：只能顺着业务顺序推进
FORWARD_RULES: dict[str, str] = {
    "草稿": "待校验",
    "待校验": "已启用",
    "已启用": "已停用",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class FlowService:
    # ---- DAG 快照 -------------------------------------------------------
    def get_snapshot(self) -> dict[str, Any]:
        """返回当前流转 DAG 快照（含管道拓扑结果）。"""
        return topology_state.current()

    def rebuild_snapshot(self, *, reason: str, conflict_policy: str | None = None) -> dict[str, Any]:
        return topology_state.rebuild(reason=reason, conflict_policy=conflict_policy)

    def set_conflict_policy(self, policy: str, reason: str) -> tuple[dict[str, Any] | None, str]:
        if policy not in CONFLICT_POLICIES:
            return None, f"冲突取舍「{policy}」不支持，可选：{'、'.join(CONFLICT_POLICIES)}"
        if not reason.strip():
            return None, "切换冲突取舍策略必须填写原因"
        snapshot = topology_state.rebuild(
            reason=f"冲突取舍切换为「{POLICY_LABELS[policy]}」：{reason.strip()}",
            conflict_policy=policy,
        )
        return snapshot, f"冲突取舍已切换为：{POLICY_LABELS[policy]}"

    def get_graph(self) -> dict[str, Any]:
        """供 DAG 页面与导出使用：节点 + 声明边（标注可执行性）。

        边的 from/to 是数字节点 id，与节点列表中的 id 对应；不再有任何
        「列表相邻项连线」逻辑。
        """
        snapshot = topology_state.current()
        id_map = {node["name"]: node["id"] for node in snapshot["nodes"]}
        nodes = [
            {
                "id": node["id"],
                "name": node["name"],
                "label": node["name"],
                "status": "已启用" if node["reachable"] else "草稿",
                "shared": node["shared"],
                "conflict": node["conflict"],
                "paused": node["paused"],
                "pipes": node["pipes"],
            }
            for node in snapshot["nodes"]
        ]
        edges = []
        for edge in snapshot["declared_edges"]:
            edges.append({
                "from": id_map[edge["from"]],
                "to": id_map[edge["to"]],
                "label": "可执行" if edge["executable"] else "已隔离",
                "pipe_id": edge["pipe_id"],
                "管道编号": edge["管道编号"],
                "declared": True,
                "executable": edge["executable"],
                "blocked_reasons": edge["blocked_reasons"],
                "source_name": edge["from"],
                "target_name": edge["to"],
            })
        return {
            "nodes": nodes,
            "edges": edges,
            "snapshot_version": snapshot["version"],
            "summary": snapshot["summary"],
        }

    # ---- 管道台账 -------------------------------------------------------
    def list_pipelines(self) -> list[dict[str, Any]]:
        return store.rows(MODULE)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any], remark: str | None = None) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry: dict[str, Any] = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for field in ["终止节点", "中间节点", "流转条件", "并行数量", "管道状态"]:
            if field in values:
                entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["status_history"] = [{
            "time": _now(),
            "from": None,
            "to": STATUS_ORDER[0],
            "action": "登记管道",
            "reason": (remark or "登记新流转管道").strip(),
        }]
        rows.append(entry)
        topology_state.rebuild(reason=f"登记管道 {entry.get('管道编号', entry['id'])}，按声明重算 DAG 快照")
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        remark: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流转管道 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据流转编排可执行范围"

        current = str(entry.get("status", STATUS_ORDER[0]))
        target = ACTION_RULES[action]
        current_idx = STATUS_ORDER.index(current) if current in STATUS_ORDER else -1
        target_idx = STATUS_ORDER.index(target)
        reason = (remark or "").strip()

        # 状态只能顺着业务顺序流转；任何切换、变更、回退都必须保留原因。
        is_forward = target_idx == current_idx + 1 and FORWARD_RULES.get(current) == target
        if not is_forward:
            if not reason:
                return None, (
                    f"状态只能沿业务顺序流转（{' -> '.join(STATUS_ORDER)}）；"
                    f"从「{current}」切换/变更/回退到「{target}」必须填写原因"
                )
            kind = "回退" if target_idx <= current_idx else "顺序跳转"
        else:
            kind = "顺序推进"
        if action in NEGATIVE_ACTIONS and not reason:
            reason = f"执行{action}，管道退出可执行拓扑"

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        entry.setdefault("status_history", []).append({
            "time": _now(),
            "from": current,
            "to": target,
            "action": action,
            "reason": reason or f"{kind}：{action}",
            "kind": kind,
        })

        # 状态变化后按声明重算快照：已启用链路不中断，以当前生效声明为准，原摘要保留。
        snapshot = topology_state.rebuild(
            reason=f"管道 {entry.get('管道编号', entry_id)} 执行{action}（{current}->{target}），重算 DAG 快照"
        )
        return entry, f"流转管道已{action}，DAG 快照已重算至 v{snapshot['version']}"

    def status_history(self, entry_id: int) -> list[dict[str, Any]] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return list(entry.get("status_history", []))

    # ---- 拓扑结果分项清单（流转页查看） ---------------------------------
    def topology_result(self) -> dict[str, Any]:
        snapshot = topology_state.current()
        return {
            "version": snapshot["version"],
            "summary": snapshot["summary"],
            "previous_summary": snapshot["previous_summary"],
            "conflict_policy": snapshot["conflict_policy"],
            "conflict_policy_label": snapshot["conflict_policy_label"],
            "declared_edges": snapshot["declared_edges"],
            "shared_nodes": [
                {k: node[k] for k in ("id", "name", "pipes", "enabled_pipes", "disabled_pipes", "conflict", "paused")}
                for node in snapshot["nodes"] if node["shared"]
            ],
            "isolation": snapshot["isolation"],
        }

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
