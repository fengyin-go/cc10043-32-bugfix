"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class ParsePayload(BaseModel):
    """解析接口入参：原载荷（字符串或JSON对象）与可选的消息标识、已确认来源。"""

    payload: Any = None
    message_id: str | None = None
    confirmed_sources: dict[str, str] = Field(default_factory=dict)


class ConflictResolutionPayload(BaseModel):
    path: str
    operator: str = ""
    remark: str = ""
