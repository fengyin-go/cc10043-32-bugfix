"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    ConflictResolutionPayload,
    EntryPayload,
    PageResult,
    ParsePayload,
)
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ---- 消息解析链路：解析、冲突复核、交付与依据查询 ----
# 固定路径先注册，避免被通用的 /{module}/{entry_id} 动态路径抢先匹配。

def _parser_module(module: str) -> None:
    if module != "message_parser":
        raise HTTPException(status_code=404, detail=f"模块 {module} 不提供解析接口")


@router.post("/{module}/{entry_id}/parse")
def parse_message(module: str, entry_id: int, payload: ParsePayload) -> dict[str, Any]:
    """把原载荷与模板一起交给解析服务，返回字段、错误明细与交付结论。"""
    _parser_module(module)
    service = require_service(module)
    if payload.payload is None:
        raise HTTPException(status_code=422, detail="缺少待解析的消息载荷 payload")
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"解析模板 {entry_id} 不存在")
    return service.parse_message(
        entry_id,
        payload.payload,
        confirmed_sources=payload.confirmed_sources,
        source=f"接口{('/' + payload.message_id) if payload.message_id else ''}",
    )


@router.get("/{module}/{entry_id}/snapshots")
def list_parse_snapshots(module: str, entry_id: int) -> dict[str, Any]:
    """按修订号列出解析依据，含原文摘要、映射和每个字段采用的原路径。"""
    _parser_module(module)
    service = require_service(module)
    snapshots = service.list_snapshots(entry_id)
    return {"items": snapshots, "total": len(snapshots)}


@router.get("/{module}/{entry_id}/snapshots/{parse_id}")
def get_parse_snapshot(module: str, entry_id: int, parse_id: int) -> dict[str, Any]:
    _parser_module(module)
    service = require_service(module)
    snapshot = service.get_snapshot(entry_id, parse_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"解析快照 {parse_id} 不存在")
    return snapshot


@router.get("/{module}/{entry_id}/conflicts")
def list_parse_conflicts(
    module: str, entry_id: int, status: str | None = Query(default=None),
) -> dict[str, Any]:
    """列出字段映射冲突；未裁决的冲突保留原始载荷与映射待复核。"""
    _parser_module(module)
    service = require_service(module)
    items = service.list_conflicts(entry_id, status=status)
    return {"items": items, "total": len(items)}


@router.post("/{module}/{entry_id}/conflicts/{conflict_id}/resolve")
def resolve_parse_conflict(
    module: str, entry_id: int, conflict_id: int, payload: ConflictResolutionPayload,
) -> dict[str, Any]:
    """人工裁决冲突来源；随后用最新载荷按已确认来源重新解析。"""
    _parser_module(module)
    service = require_service(module)
    return service.resolve_conflict(
        entry_id, conflict_id, payload.path,
        operator=payload.operator, remark=payload.remark,
    )


@router.get("/{module}/{entry_id}/deliveries")
def list_parse_deliveries(
    module: str, entry_id: int, status: str | None = Query(default=None),
) -> dict[str, Any]:
    """列出交付记录，可筛选「暂停交付」查看未交付理由。"""
    _parser_module(module)
    service = require_service(module)
    items = service.list_deliveries(entry_id, status=status)
    return {"items": items, "total": len(items)}


@router.get("/{module}/{entry_id}/transformations")
def list_parse_transformations(module: str, entry_id: int) -> dict[str, Any]:
    """最近一次解析的字段转换列表：解析字段、采用路径、原值与实际转换值。"""
    _parser_module(module)
    service = require_service(module)
    items = service.list_transformations(entry_id)
    return {"items": items, "total": len(items)}


@router.post("/api_connector/{entry_id}/ingest")
def ingest_payload(entry_id: int, payload: ParsePayload) -> dict[str, Any]:
    """连接器接入入口：原载荷落库后与匹配模板一起送进解析接口。"""
    service = require_service("api_connector")
    if payload.payload is None:
        raise HTTPException(status_code=422, detail="缺少待接入的消息载荷 payload")
    return service.ingest_to_parser(entry_id, payload.payload, payload.message_id)
