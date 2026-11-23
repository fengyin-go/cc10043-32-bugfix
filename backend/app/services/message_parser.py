"""消息解析器领域服务：解析JSON/Avro/Protobuf消息为结构化字段，管理解析模板和字段映射。

数据链路位置：消息解析层，将原始消息载荷按模板解析为结构化数据，处理脏数据策略和嵌套展开。

字段映射语法（模板「字段映射」列）：

    目标字段:载荷路径[:期望类型], 目标字段:载荷路径[:期望类型]

路径使用点号访问嵌套对象、方括号访问数组，例如 ``order.qty``、``items[0].count``。
解析结果来自载荷本身，不会把路径字符串当成字段值。缺失路径、类型不匹配、
同一目标字段被多条路径写入且值不一致时，结果会给出可核查的明细，并暂停交付。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 内部审计表：不注册为业务模块，避免污染模块列表和运营概览。
SNAPSHOTS = "_parser_snapshots"
CONFLICTS = "_parser_conflicts"
DELIVERIES = "_parser_deliveries"
RESOLUTIONS = "_parser_resolutions"

_TYPE_ALIASES = {
    "int": "int", "integer": "int", "整数": "int",
    "float": "float", "小数": "float",
    "number": "number", "数字": "number",
    "str": "str", "string": "str", "字符串": "str",
    "bool": "bool", "boolean": "bool", "布尔": "bool",
}
_SEGMENT_RE = re.compile(r"([^\[\]]+)((?:\[\d+\])*)")
_INDEX_RE = re.compile(r"\[(\d+)\]")


class MessageParserService(BaseModuleService):
    MODULE = "message_parser"
    ENTITY = "解析模板"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["模板名称", "消息格式", "字段映射", "嵌套展开深度", "脏数据策略"]
    REQUIRED_FIELDS = ["模板名称", "消息格式", "字段映射"]
    STATUS_ORDER = ["草稿", "已启用", "已废弃"]
    ACTION_RULES = {"启用模板": "已启用", "废弃模板": "已废弃"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：消息解析与字段结构化 ----

    def parse_message(
        self,
        entry_id: int,
        payload: Any,
        *,
        confirmed_sources: dict[str, str] | None = None,
        source: str = "接口",
    ) -> dict[str, Any]:
        """使用指定模板解析消息载荷，返回字段值及各字段采用的原路径。

        - 载荷中的 ``0`` / ``False`` 会被原样保留，``None`` 标记为 null，
          与“路径不存在”分开表达。
        - 同一目标字段有多条映射且值一致时保留这些路径；值不一致时默认不取值，
          生成待复核冲突并暂停交付。已在模板中确认过来源的字段按确认来源取值。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._failed_result(f"解析模板 {entry_id} 不存在")
        if entry.get("status") != "已启用":
            return self._failed_result(f"解析模板「{entry.get('模板名称', '')}」未启用")

        mapping_text = str(entry.get("字段映射", ""))
        pairs, mapping_errors = self._parse_mapping(mapping_text)
        root, payload_text, payload_error = self._normalize_payload(payload)
        parse_id = store.next_id(SNAPSHOTS)
        revision = max(
            (int(row.get("revision", 0)) for row in store.artifacts(SNAPSHOTS)
             if int(row.get("template_id", 0)) == entry_id),
            default=0,
        ) + 1

        details, errors = self._resolve_mappings(pairs, root, payload_error)
        resolutions = self._stored_resolutions(entry_id)
        # 本次请求显式携带的确认来源优先于历史确认，但不改变历史确认记录。
        request_resolutions = dict(confirmed_sources or {})
        resolutions.update(request_resolutions)
        resolved_fields, conflicts = self._group_by_target(
            details, errors, resolutions, entry_id, parse_id, payload_text, mapping_text,
            request_resolutions)
        resolved_targets = {item["field"] for item in resolved_fields}
        for error in errors:
            # 同一目标字段还有其他路径取到了值时，这条路径错误不阻断交付，
            # 只作为字段错误明细保留；目标字段完全没有可用值时才是阻断项。
            error["blocking"] = error.get("field") not in resolved_targets

        fields = {item["field"]: item["value"] for item in resolved_fields}
        transformations = self._transform_fields(fields, resolved_fields)
        delivery = self._decide_delivery(
            parse_id, payload_error, mapping_errors, errors, conflicts, fields)

        field_paths: dict[str, list[str]] = {}
        for pair in pairs:
            field_paths.setdefault(pair["target"], [])
            if pair["path"] not in field_paths[pair["target"]]:
                field_paths[pair["target"]].append(pair["path"])
        payload_sha256 = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        basis = {
            "payload_sha256": payload_sha256,
            "mapping": mapping_text,
            "field_paths": field_paths,
            "revision": revision,
        }
        snapshot = {
            "id": parse_id,
            "template_id": entry_id,
            "template_name": entry.get("模板名称", ""),
            "revision": revision,
            "source": source,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "payload_text": payload_text,
            "payload_sha256": payload_sha256,
            "mapping": mapping_text,
            "field_paths": field_paths,
            "fields": fields,
            "resolved_fields": resolved_fields,
            "field_details": details,
            "errors": errors,
            "mapping_errors": mapping_errors,
            "conflicts": conflicts,
            "transformations": transformations,
            "delivery": delivery,
            "basis": basis,
        }
        store.artifacts(SNAPSHOTS).append(snapshot)
        store.artifacts(DELIVERIES).append({
            "id": store.next_id(DELIVERIES),
            "parse_id": parse_id,
            "template_id": entry_id,
            "status": delivery["status"],
            "reason": delivery["reason"],
            "fields": fields,
            "created_at": snapshot["created_at"],
        })
        self._supersede_earlier_conflicts(entry_id, parse_id, resolved_fields, conflicts)

        ok = delivery["status"] == "已交付"
        return {
            "ok": ok,
            "message": delivery["message"],
            "parse_id": parse_id,
            "revision": revision,
            "payload_sha256": payload_sha256,
            "template": {
                "id": entry_id,
                "name": entry.get("模板名称", ""),
                "format": entry.get("消息格式", ""),
            },
            "basis": snapshot["basis"],
            "fields": fields,
            "resolved_fields": resolved_fields,
            "field_details": details,
            "field_errors": errors,
            "mapping_errors": mapping_errors,
            "conflicts": conflicts,
            "transformations": transformations,
            "delivery": delivery,
        }

    def confirm_mapping(
        self,
        entry_id: int,
        target: str,
        path: str,
        *,
        operator: str = "",
        remark: str = "",
        reparse: bool = True,
    ) -> dict[str, Any]:
        """确认某个目标字段采用哪条映射路径，并用最新载荷重新解析。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"解析模板 {entry_id} 不存在"}
        pairs, _ = self._parse_mapping(str(entry.get("字段映射", "")))
        allowed = [pair["path"] for pair in pairs if pair["target"] == target]
        if not allowed:
            return {"ok": False, "message": f"目标字段「{target}」没有配置映射路径"}
        if path not in allowed:
            return {
                "ok": False,
                "message": f"路径「{path}」不是字段「{target}」的候选来源",
                "candidates": allowed,
            }
        confirmation = {
            "id": store.next_id(RESOLUTIONS),
            "template_id": entry_id,
            "target": target,
            "path": path,
            "operator": operator,
            "remark": remark,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        store.artifacts(RESOLUTIONS).append(confirmation)
        latest = self._latest_snapshot(entry_id)
        if not reparse or latest is None:
            return {"ok": True, "message": f"已确认「{target}」采用路径 {path}", "confirmation": confirmation}
        pending = [
            row for row in store.artifacts(CONFLICTS)
            if int(row.get("template_id", 0)) == entry_id
            and row.get("target") == target and row.get("status") == "待复核"
            and row.get("raw_payload") == latest["payload_text"]
        ]
        reparsed = self.parse_message(
            entry_id,
            latest["payload_text"],
            confirmed_sources={target: path},
            source=f"裁决后重放(解析#{latest['id']})",
        )
        resolved_at = datetime.now(timezone.utc).isoformat()
        for conflict in pending:
            conflict["status"] = "已裁决"
            conflict["resolved_path"] = path
            conflict["resolved_by"] = operator
            conflict["resolution_remark"] = remark
            conflict["resolved_by_parse_id"] = reparsed.get("parse_id")
            conflict["resolved_at"] = resolved_at
        reparsed["confirmation"] = confirmation
        return reparsed

    def resolve_conflict(
        self,
        entry_id: int,
        conflict_id: int,
        path: str,
        *,
        operator: str = "",
        remark: str = "",
    ) -> dict[str, Any]:
        """人工裁决一条待复核冲突，并用最新载荷重新解析。"""
        conflict = next(
            (row for row in store.artifacts(CONFLICTS)
             if int(row.get("id", 0)) == conflict_id
             and int(row.get("template_id", 0)) == entry_id),
            None,
        )
        if conflict is None:
            return {"ok": False, "message": f"冲突记录 {conflict_id} 不存在"}
        if conflict.get("status") != "待复核":
            return {
                "ok": False,
                "message": f"冲突记录 {conflict_id} 已{conflict.get('status', '处理')}，无需重复裁决",
                "conflict": conflict,
            }
        candidates = [item["path"] for item in conflict.get("candidates", [])]
        if path not in candidates:
            return {
                "ok": False,
                "message": f"路径「{path}」不是冲突字段「{conflict.get('target', '')}」的候选来源",
                "candidates": candidates,
            }
        result = self.confirm_mapping(
            entry_id, str(conflict.get("target", "")), path,
            operator=operator, remark=remark,
        )
        if not result.get("ok"):
            return result
        result["resolved_conflict_id"] = conflict_id
        return result

    def list_snapshots(self, entry_id: int) -> list[dict[str, Any]]:
        """按修订号列出模板的解析快照，作为“更新后的解析依据”。"""
        rows = [
            row for row in store.artifacts(SNAPSHOTS)
            if int(row.get("template_id", 0)) == entry_id
        ]
        return sorted(rows, key=lambda row: int(row.get("revision", 0)))

    def get_snapshot(self, entry_id: int, parse_id: int) -> dict[str, Any] | None:
        """读取单次解析快照。"""
        return next(
            (row for row in store.artifacts(SNAPSHOTS)
             if int(row.get("id", 0)) == parse_id
             and int(row.get("template_id", 0)) == entry_id),
            None,
        )

    def list_conflicts(self, entry_id: int, status: str | None = None) -> list[dict[str, Any]]:
        """列出模板产生的冲突记录，默认包含待复核记录。"""
        rows = [
            row for row in store.artifacts(CONFLICTS)
            if int(row.get("template_id", 0)) == entry_id
        ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        return rows

    def list_deliveries(self, entry_id: int, status: str | None = None) -> list[dict[str, Any]]:
        """列出模板的交付记录，可筛选暂停交付的记录以查看原因。"""
        rows = [
            row for row in store.artifacts(DELIVERIES)
            if int(row.get("template_id", 0)) == entry_id
        ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        return rows

    def list_transformations(self, entry_id: int) -> list[dict[str, Any]]:
        """取最近一次解析的字段转换列表（解析字段 → 实际转换值）。"""
        latest = self._latest_snapshot(entry_id)
        return list(latest.get("transformations", [])) if latest else []

    # ---- 映射解析辅助 ----

    def _parse_mapping(self, mapping: str) -> tuple[list[dict[str, Any]], list[str]]:
        """把「目标:路径[:类型]」映射串拆成可核查的条目。"""
        pairs: list[dict[str, Any]] = []
        errors: list[str] = []
        for raw in re.split(r"[,，\n]", mapping):
            token = raw.strip().replace("：", ":")
            if not token:
                continue
            parts = [part.strip() for part in token.split(":")]
            if len(parts) < 2 or not parts[0] or not parts[1]:
                errors.append(f"映射「{token}」缺少目标字段或载荷路径")
                continue
            expected = None
            if len(parts) >= 3 and parts[2]:
                expected = _TYPE_ALIASES.get(parts[2].casefold())
                if expected is None:
                    errors.append(f"映射「{token}」的期望类型「{parts[2]}」无法识别")
                    continue
            pairs.append({
                "target": parts[0],
                "path": parts[1],
                "expected_type": expected,
                "raw": token,
            })
        if not pairs and not errors:
            errors.append("模板未配置字段映射")
        return pairs, errors

    def _normalize_payload(self, payload: Any) -> tuple[Any, str, str | None]:
        """统一载荷为可解析对象，并保留原文用于审计。"""
        if isinstance(payload, str):
            text = payload
            try:
                return json.loads(text), text, None
            except (TypeError, ValueError):
                return None, text, "消息载荷不是合法JSON"
        try:
            text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            text = str(payload)
        return payload, text, None

    def _resolve_mappings(
        self, pairs: list[dict[str, Any]], root: Any, payload_error: str | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """逐条读取载荷路径，区分零值、null、缺失和类型不匹配。"""
        details: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for pair in pairs:
            target, path = pair["target"], pair["path"]
            base = {"field": target, "path": path, "expected_type": pair["expected_type"]}
            if payload_error:
                details.append({**base, "status": "payload_error", "value": None})
                continue
            found, value = _resolve_path(root, path)
            if not found:
                details.append({**base, "status": "missing", "value": None})
                errors.append({
                    "field": target, "path": path, "code": "missing_path",
                    "error": "路径不存在", "expected": pair["expected_type"], "actual": None,
                })
                continue
            expected = pair["expected_type"]
            # null 是载荷里真实存在的值，和“路径不存在”分开表达；不做类型判定。
            if value is not None and expected and not _type_matches(value, expected):
                details.append({
                    **base, "status": "type_mismatch", "value": value,
                    "actual_type": _type_name(value),
                })
                errors.append({
                    "field": target, "path": path, "code": "type_mismatch",
                    "error": "类型不匹配", "expected": expected,
                    "actual": _type_name(value), "value": value,
                })
                continue
            details.append({
                **base,
                "status": "null" if value is None else "resolved",
                "value": value,
                "actual_type": _type_name(value),
            })
        if payload_error:
            errors.append({
                "field": None, "path": None, "code": "invalid_payload",
                "error": payload_error, "expected": "JSON对象", "actual": "字符串",
            })
        return details, errors

    def _group_by_target(
        self,
        details: list[dict[str, Any]],
        errors: list[dict[str, Any]],
        resolutions: dict[str, str],
        entry_id: int,
        parse_id: int,
        payload_text: str,
        mapping_text: str,
        request_resolutions: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """按目标字段归并：同值合并、异值裁决或生成待复核冲突。"""
        request_resolutions = request_resolutions or {}
        resolved_fields: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        targets: list[str] = []
        for detail in details:
            if detail["field"] not in targets:
                targets.append(detail["field"])
        for target in targets:
            group = [detail for detail in details if detail["field"] == target]
            paths = [detail["path"] for detail in group]
            valid = [detail for detail in group if detail["status"] in ("resolved", "null")]
            for detail in group:
                detail["chosen"] = False
            if not valid:
                continue
            confirmed_path = resolutions.get(target)
            confirmed_detail = next(
                (detail for detail in valid if detail["path"] == confirmed_path), None)
            if confirmed_path and confirmed_detail is None:
                # 已确认的来源在当前载荷里取不到值：不能悄悄改用别的路径，
                # 也不能让别的路径的值冒充已确认来源，只能暂停等复核。
                conflict = self._record_conflict(
                    entry_id, parse_id, target, paths, valid, payload_text, mapping_text,
                    extra_reason=(
                        f"已确认来源 {confirmed_path} 在当前载荷中不可用（"
                        + self._unavailable_reason(group, confirmed_path) + "）"
                    ),
                )
                for detail in valid:
                    detail["status"] = "conflict_candidate"
                conflicts.append(conflict)
                continue
            if confirmed_detail is not None and len(valid) > 1:
                confirmed_detail["chosen"] = True
                resolved_fields.append({
                    "field": target,
                    "value": confirmed_detail["value"],
                    "status": "resolved_confirmed",
                    "chosen_path": confirmed_detail["path"],
                    "paths": paths,
                    "resolution": {
                        "type": "confirmed_source",
                        "path": confirmed_detail["path"],
                        "operator": self._resolution_operator(
                            entry_id, target, confirmed_detail["path"], request_resolutions),
                    },
                })
                continue
            distinct = _dedupe_values([detail["value"] for detail in valid])
            if len(valid) == 1 or len(distinct) == 1:
                chosen = confirmed_detail or valid[0]
                chosen["chosen"] = True
                resolution: Any = None if len(valid) == 1 else "多条路径值一致"
                resolved_fields.append({
                    "field": target,
                    "value": chosen["value"],
                    "status": chosen["status"],
                    "chosen_path": chosen["path"],
                    "paths": paths,
                    "resolution": resolution,
                })
                continue
            conflict = self._record_conflict(
                entry_id, parse_id, target, paths, valid, payload_text, mapping_text)
            for detail in valid:
                detail["status"] = "conflict_candidate"
            conflicts.append(conflict)
        return resolved_fields, conflicts

    def _record_conflict(
        self,
        entry_id: int,
        parse_id: int,
        target: str,
        paths: list[str],
        valid: list[dict[str, Any]],
        payload_text: str,
        mapping_text: str,
        *,
        extra_reason: str = "",
    ) -> dict[str, Any]:
        """登记一条待复核冲突，保留当时的原始载荷和映射。"""
        candidates_text = "、".join(
            f"{item['path']}={item['value']!r}" for item in valid)
        if extra_reason:
            reason = (
                f"{extra_reason}；当前候选 " + candidates_text
                + f"，未确认前不交付字段「{target}」"
            )
        else:
            reason = (
                f"字段「{target}」被 {len(valid)} 条路径写入且值不一致（{candidates_text}），"
                "未确认来源前不交付该字段"
            )
        conflict: dict[str, Any] = {
            "id": store.next_id(CONFLICTS),
            "template_id": entry_id,
            "parse_id": parse_id,
            "target": target,
            "paths": paths,
            "candidates": [
                {"path": item["path"], "value": item["value"], "type": _type_name(item["value"])}
                for item in valid
            ],
            "status": "待复核",
            "reason": reason,
            "raw_payload": payload_text,
            "mapping": mapping_text,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        store.artifacts(CONFLICTS).append(conflict)
        return conflict

    @staticmethod
    def _unavailable_reason(group: list[dict[str, Any]], path: str) -> str:
        detail = next((item for item in group if item["path"] == path), None)
        if detail is None:
            return "该路径不在模板映射中"
        if detail["status"] == "missing":
            return "路径不存在"
        if detail["status"] == "type_mismatch":
            return f"类型不符，实际 {detail.get('actual_type', '')}"
        return detail["status"]

    def _transform_fields(
        self, fields: dict[str, Any], resolved_fields: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """根据转换规则生成“解析字段 → 实际转换值”清单。"""
        if not fields:
            return []
        from app.services.field_transform import FieldTransformService

        sources = {item["field"]: item["chosen_path"] for item in resolved_fields}
        return FieldTransformService().transform_parsed_fields(fields, sources)

    def _decide_delivery(
        self,
        parse_id: int,
        payload_error: str | None,
        mapping_errors: list[str],
        errors: list[dict[str, Any]],
        conflicts: list[dict[str, Any]],
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """决定是否交付：错误或未裁决冲突一律暂停并给出理由。"""
        reasons: list[str] = []
        if payload_error:
            reasons.append(payload_error)
        reasons.extend(mapping_errors)
        for error in errors:
            if error.get("code") == "invalid_payload" or not error.get("blocking", True):
                continue
            reasons.append(
                f"{error['field']}({error['path']}): {error['error']}"
                + (f"，期望 {error['expected']}，实际 {error['actual']}"
                   if error.get("code") == "type_mismatch" else "")
            )
        skipped = [
            f"{error['field']}({error['path']}): {error['error']}"
            for error in errors
            if error.get("code") != "invalid_payload" and not error.get("blocking", True)
        ]
        for conflict in conflicts:
            reasons.append(f"字段映射冲突待复核：{conflict['reason']}")
        if reasons:
            undelivered_reason = "；".join(reasons)
            return {
                "status": "暂停交付",
                "delivered": False,
                "undelivered_reason": undelivered_reason,
                "reason": undelivered_reason,
                "reasons": reasons,
                "skipped_field_errors": skipped,
                "parse_id": parse_id,
                "field_count": len(fields),
                "message": f"解析未交付：{undelivered_reason}",
            }
        return {
            "status": "已交付",
            "delivered": True,
            "undelivered_reason": "",
            "reason": "",
            "reasons": [],
            "skipped_field_errors": skipped,
            "parse_id": parse_id,
            "field_count": len(fields),
            "message": f"解析完成，{len(fields)} 个字段已交付",
        }

    def _failed_result(self, message: str) -> dict[str, Any]:
        return {
            "ok": False, "message": message, "delivered": False, "fields": {},
            "resolved_fields": [], "field_details": [], "field_errors": [],
            "mapping_errors": [], "conflicts": [], "transformations": [],
            "delivery": {
                "status": "暂停交付", "delivered": False,
                "undelivered_reason": message, "reason": message,
                "reasons": [message],
            },
        }

    def _stored_resolutions(self, entry_id: int) -> dict[str, str]:
        """读取已确认的来源，后确认的覆盖先确认的。"""
        chosen: dict[str, str] = {}
        for row in store.artifacts(RESOLUTIONS):
            if int(row.get("template_id", 0)) == entry_id:
                chosen[str(row.get("target", ""))] = str(row.get("path", ""))
        return chosen

    def _resolution_operator(
        self,
        entry_id: int,
        target: str,
        path: str,
        request_resolutions: dict[str, str] | None = None,
    ) -> str:
        operator = ""
        for row in store.artifacts(RESOLUTIONS):
            if (int(row.get("template_id", 0)) == entry_id
                    and row.get("target") == target and row.get("path") == path):
                operator = str(row.get("operator", ""))
        if not operator and request_resolutions and request_resolutions.get(target) == path:
            return "本次请求指定"
        return operator

    def _latest_snapshot(self, entry_id: int) -> dict[str, Any] | None:
        rows = self.list_snapshots(entry_id)
        return rows[-1] if rows else None

    def _supersede_earlier_conflicts(
        self,
        entry_id: int,
        parse_id: int,
        resolved_fields: list[dict[str, Any]],
        current_conflicts: list[dict[str, Any]],
    ) -> None:
        """原文更正后重送时，给旧冲突标注被哪次解析取代（保留原记录）。"""
        resolved_targets = {item["field"] for item in resolved_fields}
        current_targets = {item["target"] for item in current_conflicts}
        for row in store.artifacts(CONFLICTS):
            if (int(row.get("template_id", 0)) == entry_id
                    and row.get("status") == "待复核"
                    and int(row.get("parse_id", 0)) != parse_id
                    and row.get("target") in resolved_targets
                    and row.get("target") not in current_targets):
                superseded = row.setdefault("superseded_by_parse_ids", [])
                if parse_id not in superseded:
                    superseded.append(parse_id)
                row["superseded"] = True
                row["superseded_at"] = datetime.now(timezone.utc).isoformat()

    # ---- 原有链路能力 ----

    def validate_schema_compliance(self, entry_id: int, fields: dict[str, Any]) -> list[str]:
        """校验解析结果与Schema注册中心的版本兼容性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["模板不存在"]
        schemas = store.rows("schema_registry")
        template_name = entry.get("模板名称", "")
        matching = [s for s in schemas if s.get("status") == "已注册"
                    and template_name in str(s.get("Schema名称", ""))]
        if not matching:
            return [f"模板 {template_name} 未在Schema注册中心找到匹配版本"]
        schema = matching[0]
        schema_fields = str(schema.get("字段定义", "")).split(",")
        for key, value in fields.items():
            if key.startswith("_"):
                continue
            if key not in schema_fields:
                if str(entry.get("脏数据策略", "")) == "丢弃":
                    errors.append(f"未知字段 {key}，将按脏数据丢弃")
                else:
                    errors.append(f"未知字段 {key}，将按不匹配字段忽略")
        return errors

    def handle_dirty_data(self, entry_id: int, errors: list[str]) -> str:
        """处理脏数据：根据策略决定丢弃、隔离或忽略。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or not errors:
            return "无脏数据"
        strategy = entry.get("脏数据策略", "忽略")
        if strategy == "丢弃":
            return f"已丢弃 {len(errors)} 条脏数据"
        if strategy == "隔离":
            isolation_rows = store.rows("failure_isolation")
            fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
            now = datetime.now(timezone.utc).isoformat()
            isolation_rows.append({
                "id": fid, "失败消息ID": f"dirty-{entry_id}", "失败原因": "; ".join(errors),
                "原始Topic": entry.get("模板名称", ""), "隔离时间": now, "重试次数": "0",
                "status": "待处理", "pending": True, "abnormal": False,
            })
            return f"已隔离 {len(errors)} 条脏数据"
        return f"已忽略 {len(errors)} 条格式错误"

    def route_to_transform(self, entry_id: int, fields: dict[str, Any]) -> list[dict[str, Any]]:
        """解析后的字段根据映射关系路由到字段转换器。"""
        rules = store.rows("field_transform")
        matched = []
        for rule in rules:
            if rule.get("status") != "已启用":
                continue
            if rule.get("源字段", "") in fields:
                matched.append(rule)
        return matched

    def audit_parse_stat(self, entry_id: int, success: int, failed: int) -> None:
        """记录解析统计到审计明细。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        total = success + failed
        status = "偏差超标" if failed > 0 and failed >= total * 0.3 else "正常"
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": entry.get("模板名称", ""),
            "消息总数": str(total), "成功数": str(success),
            "失败原因分布": f"脏数据{failed}条" if failed else "",
            "status": status, "pending": True, "abnormal": failed > 0,
        })


def _resolve_path(root: Any, path: str) -> tuple[bool, Any]:
    """按 ``a.b[0].c`` 读取载荷；返回 (是否存在, 值)，存在但为 null 时值就是 None。"""
    tokens = _split_path(path)
    if tokens is None:
        return False, None
    current = root
    for token in tokens:
        if isinstance(token, int):
            if not isinstance(current, list) or token >= len(current):
                return False, None
            current = current[token]
            continue
        if not isinstance(current, dict) or token not in current:
            return False, None
        current = current[token]
    return True, current


def _split_path(path: str) -> list[str | int] | None:
    text = path.strip()
    if text.startswith("$"):
        text = text[1:].lstrip(".")
    if not text:
        return []
    tokens: list[str | int] = []
    for segment in text.split("."):
        match = _SEGMENT_RE.fullmatch(segment)
        if match is None:
            return None
        tokens.append(match.group(1))
        tokens.extend(int(index) for index in _INDEX_RE.findall(match.group(2)))
    return tokens


def _type_matches(value: Any, expected: str) -> bool:
    if expected == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected in ("float", "number"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "str":
        return isinstance(value, str)
    if expected == "bool":
        return isinstance(value, bool)
    return True


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _same_value(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    return type(left) is type(right) and left == right


def _dedupe_values(values: list[Any]) -> list[Any]:
    unique: list[Any] = []
    for value in values:
        if not any(_same_value(value, seen) for seen in unique):
            unique.append(value)
    return unique
