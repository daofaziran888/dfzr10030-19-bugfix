"""采集转换管道 DAG 拓扑接口。

转换管道服务把流转 DAG 快照及管道拓扑结果作为只读输入：同步后落入待确认台账，
核对时一批仅允许一条确认、其余拒绝，核对结果回传数据流转编排。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.transform import transform_service as service

router = APIRouter(prefix="/api/transform", tags=["采集转换管道"])
LIST_FIELDS = ["节点编号", "输入来源", "输出目标", "转换表达式", "执行顺序", "依赖节点", "失败策略", "节点状态"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回有效路径上的节点与边（只读来自流转 DAG 快照）。"""
    return service.get_graph()


@router.get("/snapshot")
def get_snapshot() -> dict[str, Any]:
    """只读查看转换服务消费的流转 DAG 快照。"""
    return service._snapshot()


@router.get("/ledger")
def get_ledger(state: str | None = Query(default=None)) -> dict[str, Any]:
    """待确认台账：可按 待确认/已确认/已拒绝/隔离 过滤。"""
    items = service.list_ledger(state)
    return {"items": items, "total": len(items), "snapshot_version": service._synced_version}


@router.post("/ledger/sync")
def sync_ledger() -> ActionResult:
    """同步读取流转 DAG 快照及管道拓扑结果并落入待确认台账。"""
    result = service.sync_snapshot()
    return ActionResult(ok=True, message=f"已同步快照 v{result['snapshot_version']} 并刷新待确认台账", entry=result)


@router.post("/ledger/review")
def review_ledger(payload: dict[str, Any]) -> ActionResult:
    """一批核对：仅一条确认，其余拒绝。"""
    reviews = payload.get("reviews") or []
    remark = payload.get("remark")
    result, message = service.review_ledger(reviews, remark)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/reconciliation")
def get_reconciliation() -> dict[str, Any]:
    """回传给数据流转编排的核对结果。"""
    return {"items": service.reconciliation_results()}


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
                             headers={"Content-Disposition": "attachment; filename=transform.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"转换节点 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="转换节点已登记并同步待确认台账", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
