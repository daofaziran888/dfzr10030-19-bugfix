"""流转 DAG 快照与管道拓扑状态。

这里是修复「按列表相邻项连线」问题的核心：流转边不再来自记录在列表里的先后位置，
而是逐条解析流转管道声明的起始节点、中间节点、终止节点，并结合管道状态校验，
产出一份带版本号的 DAG 快照。转换管道服务只能把这份快照当只读输入。

两种共享节点冲突取舍（围绕「数据只能沿已启用且声明完整的流转边移动」）：
- edge（边级启停，默认）：只保留已启用管道的边，停用/未通过校验的管道不产生可执行边，
  共享节点本身仍保留并显式标出；
- node（节点级冲突）：只要某共享节点还被停用管道占用，就整组暂停该节点上的所有边，
  直到停用管道清理完成。
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.store import store

FLOW_MODULE = "flow"

# 管道必须声明完整才允许生成可执行边
NODE_FIELDS = ("起始节点", "中间节点", "终止节点")
# 只有处于该状态的管道才能产生可执行边
EXECUTABLE_STATUS = "已启用"

CONFLICT_EDGE = "edge"
CONFLICT_NODE = "node"
CONFLICT_POLICIES = (CONFLICT_EDGE, CONFLICT_NODE)
POLICY_LABELS = {
    CONFLICT_EDGE: "边级启停：只保留已启用管道的边，共享节点保留并标记",
    CONFLICT_NODE: "节点级冲突：共享节点被停用管道占用时整组暂停",
}


def _split_nodes(value: Any) -> list[str]:
    """中间节点允许用逗号、顿号、分号或斜杠分隔多个节点。"""
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    for sep in ("、", ",", "，", ";", "；", "/"):
        text = text.replace(sep, "|")
    return [part.strip() for part in text.split("|") if part.strip()]


def declared_chain(row: dict[str, Any]) -> list[str]:
    """按声明顺序还原一条管道的节点链：起始 -> 中间... -> 终止。"""
    chain: list[str] = []
    start = str(row.get("起始节点", "")).strip()
    if start:
        chain.append(start)
    for middle in _split_nodes(row.get("中间节点")):
        if middle not in chain:
            chain.append(middle)
    end = str(row.get("终止节点", "")).strip()
    if end and end not in chain:
        chain.append(end)
    return chain


class TopologyState:
    """内存态的快照仓库：保存当前版本、冲突取舍策略与最近一次快照。"""

    def __init__(self) -> None:
        self.version = 0
        self.conflict_policy: str = CONFLICT_EDGE
        self.snapshot: dict[str, Any] | None = None
        self.summary: str = ""
        self.rebuild_reason = "初始化构建"

    # ---- 节点与边的解析 -------------------------------------------------
    @staticmethod
    def _node_index(chains: dict[int, list[str]]) -> dict[str, int]:
        """给每个声明节点分配稳定的数字 id（按首次出现顺序）。"""
        index: dict[str, int] = {}
        for pipe_id in sorted(chains):
            for name in chains[pipe_id]:
                if name not in index:
                    index[name] = len(index) + 1
        return index

    @staticmethod
    def _declaration_complete(row: dict[str, Any]) -> bool:
        return all(str(row.get(field, "")).strip() for field in NODE_FIELDS)

    def rebuild(self, *, reason: str, conflict_policy: str | None = None) -> dict[str, Any]:
        """按当前流转管道声明重算 DAG 快照。

        已启用链路不中断：重算时以当前仍在生效的管道声明为准，版本号递增，
        上一版摘要原样保留在 previous_summary 中。
        """
        previous_summary = self.summary
        if conflict_policy in CONFLICT_POLICIES:
            self.conflict_policy = conflict_policy
        rows = store.rows(FLOW_MODULE)

        chains: dict[int, list[str]] = {}
        pipe_views: dict[int, dict[str, Any]] = {}
        for row in rows:
            pipe_id = int(row.get("id", 0))
            chains[pipe_id] = declared_chain(row)
            pipe_views[pipe_id] = {
                "pipe_id": pipe_id,
                "管道编号": row.get("管道编号", ""),
                "管道名称": row.get("管道名称", ""),
                "status": str(row.get("status", "草稿")),
                "chain": chains[pipe_id],
                "declaration_complete": self._declaration_complete(row),
            }

        node_index = self._node_index(chains)

        # 声明边：每条符合声明的相邻节点对，无论管道是否启用都登记，可执行性单独标注。
        declared_edges: list[dict[str, Any]] = []
        node_owners: dict[str, set[int]] = {}
        enabled_owners: dict[str, set[int]] = {}
        disabled_owners: dict[str, set[int]] = {}
        for pipe_id in sorted(chains):
            view = pipe_views[pipe_id]
            chain = chains[pipe_id]
            executable_pipe = view["status"] == EXECUTABLE_STATUS and view["declaration_complete"]
            for pos in range(len(chain) - 1):
                source, target = chain[pos], chain[pos + 1]
                declared_edges.append({
                    "pipe_id": pipe_id,
                    "管道编号": view["管道编号"],
                    "from": source,
                    "to": target,
                    "seq": pos + 1,
                    "pipe_status": view["status"],
                    "declaration_complete": view["declaration_complete"],
                    "declared": True,
                    "executable": False,  # 先默认不可执行，下面统一裁决
                    "blocked_reasons": [] if executable_pipe else [
                        "管道声明不完整（缺起始/中间/终止节点）"
                        if not view["declaration_complete"]
                        else f"管道状态为{view['status']}，未启用或未通过校验"
                    ],
                })
            for name in chain:
                node_owners.setdefault(name, set()).add(pipe_id)
                if executable_pipe:
                    enabled_owners.setdefault(name, set()).add(pipe_id)
                else:
                    disabled_owners.setdefault(name, set()).add(pipe_id)

        # 共享节点：同一节点被两条及以上管道声明。
        shared_nodes = sorted(
            name for name, owners in node_owners.items() if len(owners) > 1
        )
        # 边界冲突：共享节点同时落在已启用管道与停用/未通过校验管道上。
        conflict_nodes = sorted(
            name for name in shared_nodes
            if enabled_owners.get(name) and disabled_owners.get(name)
        )

        # 节点级冲突取舍：冲突节点上的所有边整组暂停。
        paused_nodes = set(conflict_nodes) if self.conflict_policy == CONFLICT_NODE else set()

        for edge in declared_edges:
            if edge["pipe_status"] == EXECUTABLE_STATUS and edge["declaration_complete"]:
                if edge["from"] in paused_nodes or edge["to"] in paused_nodes:
                    edge["blocked_reasons"] = [
                        "节点级冲突：共享节点仍被停用管道占用，整组暂停至停用管道清理"
                    ]
                else:
                    edge["executable"] = True
                    edge["blocked_reasons"] = []

        executable_edges = [e for e in declared_edges if e["executable"]]
        isolated_edges = [e for e in declared_edges if not e["executable"]]

        # 节点视图：汇聚节点的共享/冲突/暂停标记以及可执行出入边。
        nodes: list[dict[str, Any]] = []
        for name, nid in sorted(node_index.items(), key=lambda item: item[1]):
            owners = sorted(node_owners.get(name, set()))
            incoming = [e for e in executable_edges if e["to"] == name]
            outgoing = [e for e in executable_edges if e["from"] == name]
            nodes.append({
                "id": nid,
                "name": name,
                "shared": name in shared_nodes,
                "conflict": name in conflict_nodes,
                "paused": name in paused_nodes,
                "pipes": owners,
                "enabled_pipes": sorted(enabled_owners.get(name, set())),
                "disabled_pipes": sorted(disabled_owners.get(name, set())),
                "is_start": not incoming,
                "is_end": not outgoing,
                "reachable": bool(incoming or outgoing or any(
                    e["from"] == name or e["to"] == name for e in executable_edges
                )),
            })

        # 停用隔离清单：停用/未通过校验/声明不完整的管道及其被隔离的边。
        isolation: list[dict[str, Any]] = []
        for pipe_id in sorted(chains):
            view = pipe_views[pipe_id]
            blocked = [e for e in isolated_edges if e["pipe_id"] == pipe_id]
            if blocked or not view["declaration_complete"]:
                reasons = []
                if not view["declaration_complete"]:
                    reasons.append("管道声明不完整，无法生成可执行边")
                if view["status"] != EXECUTABLE_STATUS:
                    reasons.append(f"管道状态为{view['status']}，边被停用隔离")
                if self.conflict_policy == CONFLICT_NODE and any(
                    node in conflict_nodes for node in view["chain"]
                ) and view["status"] == EXECUTABLE_STATUS:
                    reasons.append("共享节点冲突未清理，节点级暂停")
                isolation.append({
                    "pipe_id": pipe_id,
                    "管道编号": view["管道编号"],
                    "管道名称": view["管道名称"],
                    "status": view["status"],
                    "declaration_complete": view["declaration_complete"],
                    "blocked_edges": [
                        {"from": e["from"], "to": e["to"], "reasons": e["blocked_reasons"]}
                        for e in blocked
                    ],
                    "reasons": reasons,
                })

        self.version += 1
        self.summary = (
            f"v{self.version}：{len(nodes)} 个声明节点、{len(declared_edges)} 条声明边、"
            f"{len(executable_edges)} 条可执行边、{len(shared_nodes)} 个共享节点、"
            f"{len(isolation)} 条管道停用隔离"
        )
        self.rebuild_reason = reason
        self.snapshot = {
            "version": self.version,
            "conflict_policy": self.conflict_policy,
            "conflict_policy_label": POLICY_LABELS[self.conflict_policy],
            "generated_reason": reason,
            "summary": self.summary,
            "previous_summary": previous_summary,
            "nodes": nodes,
            "declared_edges": declared_edges,
            "executable_edges": executable_edges,
            "shared_nodes": shared_nodes,
            "conflict_nodes": conflict_nodes,
            "paused_nodes": sorted(paused_nodes),
            "isolation": isolation,
            "pipelines": [pipe_views[pid] for pid in sorted(pipe_views)],
        }
        return deepcopy(self.snapshot)

    def current(self) -> dict[str, Any]:
        if self.snapshot is None:
            return self.rebuild(reason="首次读取按需构建")
        return deepcopy(self.snapshot)

    def node_id_map(self) -> dict[str, int]:
        return {node["name"]: node["id"] for node in self.current()["nodes"]}


# 单例：flow 与 transform 两个服务共享同一份快照状态
topology_state = TopologyState()
