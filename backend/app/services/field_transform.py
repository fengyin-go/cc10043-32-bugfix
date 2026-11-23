"""字段转换器领域服务：对消息字段执行类型转换、脱敏、编码和单位统一，管理转换规则链。

数据链路位置：数据变换层，在解析完成后对字段执行业务语义转换，输出标准化数据字段。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class FieldTransformService(BaseModuleService):
    MODULE = "field_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则名称", "源字段", "目标字段", "转换函数", "错误处理"]
    REQUIRED_FIELDS = ["规则名称", "源字段", "目标字段"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "已启用", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ---- 业务扩展：字段转换与脱敏 ----

    def apply_transform(self, entry_id: int, source_value: Any) -> dict[str, Any]:
        """应用转换规则到源字段值。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "规则不可用", "result": source_value}
        fn_name = entry.get("转换函数", "")
        error_policy = entry.get("错误处理", "忽略")
        try:
            value = str(source_value)
            if "int" in fn_name:
                result = int(float(value))
            elif "float" in fn_name:
                result = float(value)
            elif "mask" in fn_name or "脱敏" in fn_name:
                result = value[:3] + "***" + value[-4:] if len(value) > 7 else "***"
            elif "upper" in fn_name:
                result = value.upper()
            elif "lower" in fn_name:
                result = value.lower()
            elif "strip" in fn_name:
                result = value.strip()
            elif "datetime" in fn_name or "日期" in fn_name:
                result = datetime.now(timezone.utc).isoformat()
            else:
                result = value
            return {"ok": True, "message": "转换成功", "result": result}
        except (ValueError, TypeError) as exc:
            if error_policy == "中断":
                return {"ok": False, "message": f"转换失败: {exc}", "result": None}
            return {"ok": False, "message": f"转换异常已忽略: {exc}", "result": source_value}

    def transform_parsed_fields(
        self, fields: dict[str, Any], sources: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """把解析结果按已启用规则转换，输出“解析字段 → 实际转换值”清单。

        每条记录都绑定解析字段、该字段实际采用的载荷路径、原值和转换后的值，
        转换失败时保留原值与错误说明，不把值悄悄换成别的东西。
        """
        sources = sources or {}
        transforms: list[dict[str, Any]] = []
        rules = [
            rule for rule in store.rows(self.MODULE)
            if rule.get("status") == "已启用" and rule.get("源字段", "") in fields
        ]
        for rule in rules:
            field = str(rule.get("源字段", ""))
            raw_value = fields[field]
            if raw_value is None:
                # null 是载荷中的真实值，不是转换失败；保留原值并标记为不适用。
                transforms.append({
                    "field": field,
                    "source_path": sources.get(field),
                    "value": None,
                    "rule_id": rule.get("id"),
                    "rule_name": rule.get("规则名称", ""),
                    "target_field": rule.get("目标字段", ""),
                    "transform": rule.get("转换函数", ""),
                    "ok": True,
                    "transformed_value": None,
                    "error": None,
                    "skipped": "源值为 null，不执行转换",
                    "error_policy": rule.get("错误处理", "忽略"),
                })
                continue
            outcome = self.apply_transform(int(rule.get("id", 0)), raw_value)
            transforms.append({
                "field": field,
                "source_path": sources.get(field),
                "value": raw_value,
                "rule_id": rule.get("id"),
                "rule_name": rule.get("规则名称", ""),
                "target_field": rule.get("目标字段", ""),
                "transform": rule.get("转换函数", ""),
                "ok": outcome.get("ok", False),
                "transformed_value": outcome.get("result"),
                "error": None if outcome.get("ok") else outcome.get("message"),
                "error_policy": rule.get("错误处理", "忽略"),
            })
        return transforms

    def validate_transform_chain(self, source_field: str) -> dict[str, Any]:
        """校验同一源字段的转换规则链是否存在冲突。"""
        rules = store.rows(self.MODULE)
        chain = [r for r in rules if r.get("源字段", "") == source_field and r.get("status") == "已启用"]
        conflicts = []
        targets = [r.get("目标字段", "") for r in chain]
        if len(set(targets)) < len(targets):
            conflicts.append("存在多个规则转换到相同目标字段")
        return {"source": source_field, "rules": len(chain), "conflicts": conflicts}

    def route_transformed_to_sink(self, entry_id: int, result: Any) -> str | None:
        """转换后的数据路由到数据落地配置。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        target_field = entry.get("目标字段", "")
        sinks = store.rows("data_sink")
        for sink in sinks:
            if sink.get("status") == "已启用" and target_field in str(sink.get("配置名称", "")):
                return sink.get("配置名称", "")
        return None

    def handle_transform_error(self, entry_id: int, source_value: Any, error: str) -> None:
        """处理转换错误：按规则配置的错误处理策略。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        policy = entry.get("错误处理", "忽略")
        if policy == "隔离":
            now = datetime.now(timezone.utc).isoformat()
            isolation_rows = store.rows("failure_isolation")
            fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
            isolation_rows.append({
                "id": fid, "失败消息ID": f"tf-err-{entry_id}",
                "失败原因": f"转换失败: {error}, 源值={source_value}",
                "原始Topic": entry.get("规则名称", ""),
                "隔离时间": now, "重试次数": "0",
                "status": "待处理", "pending": True, "abnormal": False,
            })