"""数据流转编排 DAG 拓扑接口。

返回按管道声明构建的流转 DAG 快照、声明边/共享节点/停用隔离清单，
并提供带原因的状态流转、冲突取舍切换与状态履历查询。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.flow import FlowService
from app.services.transform import transform_service

router = APIRouter(prefix="/api/flow", tags=["数据流转编排"])
service = FlowService()
LIST_FIELDS = ["管道编号", "管道名称", "起始节点", "终止节点", "中间节点", "流转条件", "并行数量", "管道状态"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据（边全部来自管道声明，不再按列表相邻连线）。"""
    return service.get_graph()


@router.get("/snapshot")
def get_snapshot() -> dict[str, Any]:
    """流转 DAG 快照及管道拓扑结果。"""
    return service.get_snapshot()


@router.get("/topology")
def get_topology() -> dict[str, Any]:
    """声明边、共享节点与停用隔离清单（流转页查看）。"""
    return service.topology_result()


@router.get("/pipelines")
def list_pipelines() -> dict[str, Any]:
    """流转管道声明台账。"""
    return {"items": service.list_pipelines(), "total": len(service.list_pipelines())}


@router.get("/reconciliation")
def get_reconciliation() -> dict[str, Any]:
    """转换管道服务回传的核对结果。"""
    return {"items": transform_service.reconciliation_results()}


@router.post("/snapshot/rebuild")
def rebuild_snapshot(payload: ActionPayload) -> ActionResult:
    """以旧版本生效声明为准手动重算快照（链路不中断，原摘要保留）。"""
    reason = (payload.remark or "").strip()
    if not reason:
        return ActionResult(ok=False, message="重算快照必须填写原因")
    snapshot = service.rebuild_snapshot(reason=reason)
    return ActionResult(ok=True, message=f"DAG 快照已重算至 v{snapshot['version']}", entry=snapshot)


@router.post("/conflict-policy")
def set_conflict_policy(payload: dict[str, Any]) -> ActionResult:
    """切换共享节点冲突取舍：edge（边级启停）或 node（节点级冲突）。"""
    policy = str(payload.get("policy", "")).strip()
    reason = str(payload.get("reason", "") or "").strip()
    snapshot, message = service.set_conflict_policy(policy, reason)
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id", ""), csv_cell(node.get("name", "")), "", "", "", node.get("status", "")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from", ""), edge.get("to", ""), edge.get("label", ""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=flow.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流转管道 {entry_id} 不存在")
    return entry


@router.get("/{entry_id}/history")
def get_history(entry_id: int) -> dict[str, Any]:
    history = service.status_history(entry_id)
    if history is None:
        raise HTTPException(status_code=404, detail=f"流转管道 {entry_id} 不存在")
    return {"items": history, "total": len(history)}


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values, payload.remark)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流转管道已登记，DAG 快照已按声明重算", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action, payload.remark)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
