"""采集转换管道服务。

修复要点：
1. 转换管道服务不再自己按列表相邻项连线，而是把流转 DAG 快照及管道拓扑结果当作
   只读输入，只为落在有效路径（快照可执行边）上的转换节点生成台账项；
2. 转换页只接收有效路径的数据，停用/未通过校验管道上的节点进入停用隔离，不生成可执行边；
3. 同步读取快照后落入「待确认台账」，状态随快照版本变化重新标记为待核对；
4. 一批提交里仅允许一条确认，其余一律拒绝并写明原因，核对结果回传数据流转编排。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.topology import topology_state
from app.store import store

MODULE = "transform"
REQUIRED_FIELDS = ["节点编号", "输入来源", "输出目标"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用节点": "已启用", "停用节点": "已停用"}
NEGATIVE_ACTIONS = ["停用节点"]

LEDGER_PENDING = "待确认"
LEDGER_CONFIRMED = "已确认"
LEDGER_REJECTED = "已拒绝"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class TransformService:
    def __init__(self) -> None:
        # 待确认台账：key 为转换节点 id，值为台账项
        self._ledger: dict[int, dict[str, Any]] = {}
        self._synced_version: int | None = None
        # 回传给数据流转编排的核对结果
        self._reconciliation: list[dict[str, Any]] = []

    # ---- 只读消费流转 DAG 快照 -----------------------------------------
    def _snapshot(self) -> dict[str, Any]:
        return topology_state.current()

    def _valid_node_names(self) -> set[str]:
        snapshot = self._snapshot()
        names: set[str] = set()
        for edge in snapshot["executable_edges"]:
            names.add(edge["from"])
            names.add(edge["to"])
        return names

    def _edge_for_transform(self, row: dict[str, Any]) -> dict[str, Any] | None:
        """为进入节点匹配对应的流转边与转换表达式：输入来源->输出目标命中可执行边。"""
        source = str(row.get("输入来源", "")).strip()
        target = str(row.get("输出目标", "")).strip()
        for edge in self._snapshot()["executable_edges"]:
            if edge["from"] == source and edge["to"] == target:
                return edge
        return None

    def sync_snapshot(self) -> dict[str, Any]:
        """同步读取流转 DAG 快照及管道拓扑结果，落入待确认台账。

        快照版本变化后，既有确认/拒绝结果保留，受影响项重新进入待核对状态。
        """
        snapshot = self._snapshot()
        version = snapshot["version"]
        valid_names = self._valid_node_names()
        rows = store.rows(MODULE)

        ledger: list[dict[str, Any]] = []
        for row in rows:
            node_id = int(row.get("id", 0))
            source = str(row.get("输入来源", "")).strip()
            target = str(row.get("输出目标", "")).strip()
            on_valid_path = source in valid_names and target in valid_names
            edge = self._edge_for_transform(row)
            previous = self._ledger.get(node_id)

            if not on_valid_path or edge is None:
                # 不在有效路径上：不生成可执行台账，仅登记隔离原因。
                reasons = []
                if str(row.get("status")) == "已停用":
                    reasons.append("转换节点已停用")
                if edge is None:
                    reasons.append("输入来源->输出目标未命中快照中的可执行流转边")
                item = {
                    "ledger_id": f"LEDG-{node_id:04d}",
                    "transform_id": node_id,
                    "节点编号": row.get("节点编号", ""),
                    "输入来源": source,
                    "输出目标": target,
                    "转换表达式": row.get("转换表达式", ""),
                    "snapshot_version": version,
                    "state": "隔离",
                    "matched_edge": None,
                    "reasons": reasons,
                    "updated_at": _now(),
                }
                self._ledger[node_id] = item
                ledger.append(item)
                continue

            if previous and previous.get("state") in (LEDGER_CONFIRMED, LEDGER_REJECTED) \
                    and previous.get("snapshot_version") == version:
                ledger.append(previous)
                continue

            # 新进入或快照版本变化：落入/回到待确认（待核对）。
            prior_state = previous.get("state") if previous else None
            item = {
                "ledger_id": f"LEDG-{node_id:04d}",
                "transform_id": node_id,
                "节点编号": row.get("节点编号", ""),
                "输入来源": source,
                "输出目标": target,
                "转换表达式": row.get("转换表达式", ""),
                "snapshot_version": version,
                "state": LEDGER_PENDING,
                "matched_edge": {
                    "from": edge["from"],
                    "to": edge["to"],
                    "pipe_id": edge["pipe_id"],
                    "管道编号": edge["管道编号"],
                },
                "reasons": [] if prior_state is None else [
                    f"快照升级至 v{version}，原{prior_state}结果回到待核对"
                ],
                "updated_at": _now(),
            }
            self._ledger[node_id] = item
            ledger.append(item)

        self._synced_version = version
        return {
            "ok": True,
            "snapshot_version": version,
            "summary": snapshot["summary"],
            "conflict_policy": snapshot["conflict_policy"],
            "items": ledger,
        }

    def list_ledger(self, state: str | None = None) -> list[dict[str, Any]]:
        if not self._ledger:
            self.sync_snapshot()
        items = list(self._ledger.values())
        if state:
            items = [item for item in items if item["state"] == state]
        return items

    def review_ledger(
        self,
        reviews: list[dict[str, Any]],
        remark: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """一批核对提交：仅允许一条确认，其余必须拒绝。

        规则：
        - 0 条确认 -> 拒绝受理（无法确定唯一确认项）；
        - 多于 1 条确认 -> 整批拒绝受理并说明原因；
        - 恰好 1 条确认 -> 该条确认，同批其余待确认项一律拒绝并写明原因。
        """
        if not reviews:
            return None, "没有提交任何核对项"
        confirms = [r for r in reviews if str(r.get("decision", "")).strip() == "确认"]
        if len(confirms) == 0:
            return None, "一批提交必须且只能包含一条确认，当前没有确认项，整批不予受理"
        if len(confirms) > 1:
            return None, f"一批提交仅允许一条确认，当前提交了 {len(confirms)} 条确认，整批拒绝受理"

        if not self._ledger:
            self.sync_snapshot()
        confirm_id = int(confirms[0].get("transform_id", 0))
        confirm_item = self._ledger.get(confirm_id)
        if confirm_item is None or confirm_item["state"] == "隔离":
            return None, f"转换节点 {confirm_id} 不在有效路径上，不能确认"
        if confirm_item["state"] != LEDGER_PENDING:
            return None, f"转换节点 {confirm_id} 当前为{confirm_item['state']}，不能重复确认"

        reason = (remark or "").strip() or "同批唯一确认"
        timestamp = _now()
        review_version = self._synced_version
        confirm_item["state"] = LEDGER_CONFIRMED
        confirm_item["reasons"] = [reason]
        confirm_item["snapshot_version"] = review_version
        confirm_item["updated_at"] = timestamp
        confirm_item["confirmed_at"] = timestamp

        rejected: list[dict[str, Any]] = []
        target_ids = {int(r.get("transform_id", 0)) for r in reviews}
        for node_id, item in self._ledger.items():
            if node_id == confirm_id or item["state"] != LEDGER_PENDING:
                continue
            # 同批显式提交的其余项拒绝；未在本批提交的待确认项保持待确认。
            if node_id in target_ids:
                item["state"] = LEDGER_REJECTED
                item["reasons"] = [f"同批仅允许一条确认（已确认 {confirm_item['节点编号']}），该项拒绝"]
                item["snapshot_version"] = review_version
                item["updated_at"] = timestamp
                rejected.append({"transform_id": node_id, "节点编号": item["节点编号"]})

        result = {
            "ok": True,
            "snapshot_version": review_version,
            "confirmed": {"transform_id": confirm_id, "节点编号": confirm_item["节点编号"], "reason": reason},
            "rejected": rejected,
            "reviewed_at": timestamp,
        }
        self._reconciliation.append(result)
        return result, "核对完成：一条确认，同批其余已拒绝"

    def reconciliation_results(self) -> list[dict[str, Any]]:
        """回传给数据流转编排的核对结果（只读）。"""
        return list(self._reconciliation)

    # ---- DAG 图：只渲染有效路径 ----------------------------------------
    def get_graph(self) -> dict[str, Any]:
        snapshot = self._snapshot()
        rows = store.rows(MODULE)
        valid_names = self._valid_node_names()

        nodes: list[dict[str, Any]] = []
        valid_rows: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            source = str(row.get("输入来源", "")).strip()
            target = str(row.get("输出目标", "")).strip()
            on_valid_path = source in valid_names and target in valid_names
            nodes.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("节点编号", "")),
                "label": str(row.get("节点编号", "")),
                "status": str(row.get("status", STATUS_ORDER[0])),
                "输入来源": source,
                "输出目标": target,
                "转换表达式": row.get("转换表达式", ""),
                "on_valid_path": on_valid_path,
                **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")},
            })
            if on_valid_path:
                valid_rows.append(row)

        # 边只来自快照可执行边；连线两端统一使用转换节点 id：
        # 上游转换节点的输出目标 == 下游转换节点的输入来源，且该衔接是可执行流转边。
        executable_keys = {(edge["from"], edge["to"]): edge for edge in snapshot["executable_edges"]}
        edges: list[dict[str, Any]] = []
        seen_pairs: set[tuple[int, int]] = set()
        for row in valid_rows:
            source = str(row.get("输入来源", "")).strip()
            target = str(row.get("输出目标", "")).strip()
            flow_edge = executable_keys.get((source, target))
            if flow_edge is None:
                continue
            downstream = next(
                (r for r in valid_rows if str(r.get("输入来源", "")).strip() == target),
                None,
            )
            if downstream is None:
                # 末段转换节点（输出目标即流转终止节点）：没有下游转换节点，不制造假边。
                continue
            pair = (int(row["id"]), int(downstream["id"]))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            edges.append({
                "from": pair[0],
                "to": pair[1],
                "label": f"{flow_edge['管道编号']}·有效路径",
                "pipe_id": flow_edge["pipe_id"],
                "管道编号": flow_edge["管道编号"],
                "executable": True,
                "转换表达式": row.get("转换表达式", ""),
                "carrier": row.get("节点编号", ""),
                "source_name": source,
                "target_name": target,
            })

        # 有效路径上缺少对应转换节点的流转边：提示补登记，而不是静默丢数据。
        covered = {(str(r.get("输入来源", "")).strip(), str(r.get("输出目标", "")).strip())
                   for r in valid_rows}
        unmatched = [
            {"from": edge["from"], "to": edge["to"], "管道编号": edge["管道编号"]}
            for edge in snapshot["executable_edges"] if (edge["from"], edge["to"]) not in covered
        ]
        return {
            "nodes": nodes,
            "edges": edges,
            "unmatched_flow_edges": unmatched,
            "snapshot_version": snapshot["version"],
            "summary": snapshot["summary"],
            "read_only_source": "flow_dag_snapshot",
        }

    # ---- 转换节点自身台账（保留原模块能力） -----------------------------
    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        # 新增转换节点后重新同步快照台账
        self.sync_snapshot()
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"转换节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集转换管道可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        self.sync_snapshot()
        return entry, f"转换节点已{action}，已按最新 DAG 快照重新核对台账"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]


# 单例，路由层共享同一份待确认台账
transform_service = TransformService()
