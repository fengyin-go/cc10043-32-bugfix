"""消息解析链路回归测试：单路径、双路径冲突、更正原文后重送。

覆盖点：
- 解析值来自载荷，而不是“目标字段:路径”里的路径字符串；
- 0 / null / 路径不存在三种情况可区分；
- 双路径写入同一目标且值不一致时不静默择一，给出未交付理由并保留待复核记录；
- 确认来源后可重新解析并交付，解析依据随载荷修订号更新；
- 字段转换列表绑定解析字段实际采用的原路径与转换后的值；
- 连接器接入入口把原载荷与匹配模板一起送进解析服务。

运行：.venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import unittest

from app.services.api_connector import ApiConnectorService
from app.services.message_parser import MessageParserService
from app.store import store

DUAL_TEMPLATE_ID = 2       # 订单JSON解析（生产）：qty 同时映射 order.qty 与 items.count
SINGLE_TEMPLATE_ID = 4     # 订单JSON解析（单路径）：qty 只映射 order.qty
CONNECTOR_ID = 2           # 订单API连接器，绑定生产模板

ORDER_PAYLOAD = {
    "order": {"id": "A-1001", "qty": 12, "amount": 99.5},
    "items": {"count": 10},
}
CORRECTED_PAYLOAD = {
    "order": {"id": "A-1001", "qty": 12, "amount": 99.5},
    "items": {"count": 12},
}


def _detail(result: dict, field: str, path: str) -> dict:
    return next(
        item for item in result["field_details"]
        if item["field"] == field and item["path"] == path
    )


class SinglePathDeliveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MessageParserService()

    def test_quantity_comes_from_payload(self) -> None:
        result = self.service.parse_message(SINGLE_TEMPLATE_ID, ORDER_PAYLOAD)

        self.assertTrue(result["ok"])
        self.assertEqual(result["delivery"]["status"], "已交付")
        self.assertEqual(result["fields"]["qty"], 12)
        self.assertNotEqual(result["fields"]["qty"], "order.qty")
        self.assertEqual(_detail(result, "qty", "order.qty")["value"], 12)

    def test_transform_list_binds_parsed_field_and_actual_value(self) -> None:
        result = self.service.parse_message(SINGLE_TEMPLATE_ID, ORDER_PAYLOAD)

        qty_transform = next(t for t in result["transformations"] if t["field"] == "qty")
        self.assertEqual(qty_transform["source_path"], "order.qty")
        self.assertEqual(qty_transform["value"], 12)
        self.assertEqual(qty_transform["transformed_value"], 12)
        self.assertEqual(qty_transform["target_field"], "quantity")
        self.assertTrue(qty_transform["ok"])

    def test_zero_null_and_missing_are_distinct(self) -> None:
        zero = self.service.parse_message(
            SINGLE_TEMPLATE_ID,
            {"order": {"id": "A-1", "qty": 0, "amount": 0.0}},
        )
        self.assertEqual(zero["delivery"]["status"], "已交付")
        self.assertEqual(zero["fields"]["qty"], 0)
        self.assertIsNotNone(zero["fields"]["qty"])
        self.assertEqual(_detail(zero, "qty", "order.qty")["status"], "resolved")

        null = self.service.parse_message(
            SINGLE_TEMPLATE_ID,
            {"order": {"id": "A-2", "qty": None, "amount": None}},
        )
        self.assertEqual(null["delivery"]["status"], "已交付")
        self.assertIn("qty", null["fields"])
        self.assertIsNone(null["fields"]["qty"])
        self.assertEqual(_detail(null, "qty", "order.qty")["status"], "null")

        missing = self.service.parse_message(
            SINGLE_TEMPLATE_ID,
            {"order": {"id": "A-3", "amount": 1.0}},
        )
        self.assertEqual(missing["delivery"]["status"], "暂停交付")
        self.assertNotIn("qty", missing["fields"])
        self.assertEqual(_detail(missing, "qty", "order.qty")["status"], "missing")
        error = next(e for e in missing["field_errors"] if e["field"] == "qty")
        self.assertEqual(error["code"], "missing_path")
        self.assertEqual(error["path"], "order.qty")

    def test_type_mismatch_reports_expected_and_actual(self) -> None:
        result = self.service.parse_message(
            SINGLE_TEMPLATE_ID,
            {"order": {"id": "A-4", "qty": "12", "amount": 1.0}},
        )

        self.assertEqual(result["delivery"]["status"], "暂停交付")
        error = next(e for e in result["field_errors"] if e["field"] == "qty")
        self.assertEqual(error["code"], "type_mismatch")
        self.assertEqual(error["expected"], "int")
        self.assertEqual(error["actual"], "str")
        self.assertEqual(error["path"], "order.qty")

    def test_identical_duplicate_paths_do_not_conflict(self) -> None:
        result = self.service.parse_message(DUAL_TEMPLATE_ID, CORRECTED_PAYLOAD)

        self.assertEqual(result["delivery"]["status"], "已交付")
        self.assertEqual(result["fields"]["qty"], 12)
        self.assertEqual(result["conflicts"], [])
        qty_paths = next(f for f in result["resolved_fields"] if f["field"] == "qty")["paths"]
        self.assertEqual(qty_paths, ["order.qty", "items.count"])

    def test_disabled_template_is_rejected(self) -> None:
        result = self.service.parse_message(1, ORDER_PAYLOAD)

        self.assertFalse(result["ok"])
        self.assertEqual(result["delivery"]["status"], "暂停交付")
        self.assertIn("未启用", result["message"])


class DualPathConflictTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MessageParserService()
        self.template_id = self._new_template(
            "冲突测试模板",
            "qty:order.qty:int, qty:items.count:int, amount:order.amount:float",
        )

    def _new_template(self, name: str, mapping: str) -> int:
        rows = store.rows("message_parser")
        template_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        rows.append({
            "id": template_id, "status": "已启用", "pending": False, "abnormal": False,
            "模板名称": name, "消息格式": "json", "字段映射": mapping,
            "嵌套展开深度": "2", "脏数据策略": "隔离",
        })
        return template_id

    def test_conflict_pauses_delivery_and_keeps_raw_record(self) -> None:
        result = self.service.parse_message(self.template_id, ORDER_PAYLOAD)

        self.assertFalse(result["ok"])
        self.assertEqual(result["delivery"]["status"], "暂停交付")
        self.assertNotIn("qty", result["fields"])
        self.assertIn("order.qty=12", result["delivery"]["undelivered_reason"])
        self.assertIn("items.count=10", result["delivery"]["undelivered_reason"])
        self.assertEqual(len(result["conflicts"]), 1)

        conflict = result["conflicts"][0]
        self.assertEqual(conflict["target"], "qty")
        self.assertEqual(conflict["status"], "待复核")
        self.assertEqual(
            {c["path"]: c["value"] for c in conflict["candidates"]},
            {"order.qty": 12, "items.count": 10},
        )
        self.assertEqual(json.loads(conflict["raw_payload"]), ORDER_PAYLOAD)
        self.assertEqual(
            self.service.list_conflicts(self.template_id, "待复核")[0]["id"], conflict["id"])

    def test_unresolved_conflict_does_not_write_transformation(self) -> None:
        result = self.service.parse_message(self.template_id, ORDER_PAYLOAD)

        self.assertEqual([t for t in result["transformations"] if t["field"] == "qty"], [])
        paused = self.service.list_deliveries(self.template_id, "暂停交付")
        self.assertTrue(any("冲突" in row["reason"] for row in paused))

    def test_confirmed_source_is_used_and_auditable(self) -> None:
        first = self.service.parse_message(self.template_id, ORDER_PAYLOAD)
        conflict_id = first["conflicts"][0]["id"]

        resolved = self.service.resolve_conflict(
            self.template_id, conflict_id, "items.count",
            operator="李四", remark="以明细数量为准",
        )

        self.assertTrue(resolved["ok"])
        self.assertEqual(resolved["delivery"]["status"], "已交付")
        self.assertEqual(resolved["fields"]["qty"], 10)
        qty_field = next(f for f in resolved["resolved_fields"] if f["field"] == "qty")
        self.assertEqual(qty_field["chosen_path"], "items.count")
        self.assertEqual(qty_field["resolution"]["type"], "confirmed_source")
        self.assertEqual(qty_field["resolution"]["operator"], "李四")
        qty_transform = next(t for t in resolved["transformations"] if t["field"] == "qty")
        self.assertEqual(qty_transform["source_path"], "items.count")
        self.assertEqual(qty_transform["transformed_value"], 10)
        self.assertEqual(
            self.service.list_conflicts(self.template_id, "待复核"), [])

    def test_invalid_resolution_path_is_rejected(self) -> None:
        first = self.service.parse_message(self.template_id, ORDER_PAYLOAD)
        conflict_id = first["conflicts"][0]["id"]

        rejected = self.service.resolve_conflict(
            self.template_id, conflict_id, "order.amount")

        self.assertFalse(rejected["ok"])
        self.assertIn("候选来源", rejected["message"])
        self.assertEqual(len(self.service.list_conflicts(self.template_id, "待复核")), 1)

    def test_confirmed_source_missing_from_payload_pauses_instead_of_falling_back(self) -> None:
        first = self.service.parse_message(self.template_id, ORDER_PAYLOAD)
        conflict_id = first["conflicts"][0]["id"]
        self.service.resolve_conflict(self.template_id, conflict_id, "order.qty")

        # 已确认 order.qty，但新载荷里只有 items.count；不能悄悄改用 items.count。
        result = self.service.parse_message(
            self.template_id, {"items": {"count": 10}})

        self.assertEqual(result["delivery"]["status"], "暂停交付")
        self.assertNotIn("qty", result["fields"])
        self.assertIn("order.qty", result["delivery"]["undelivered_reason"])
        self.assertIn("不可用", result["delivery"]["undelivered_reason"])
        self.assertEqual(result["transformations"], [])

    def test_corrected_payload_resend_updates_basis(self) -> None:
        first = self.service.parse_message(self.template_id, ORDER_PAYLOAD)
        conflict_id = first["conflicts"][0]["id"]
        self.service.resolve_conflict(self.template_id, conflict_id, "order.qty")

        resent = self.service.parse_message(self.template_id, CORRECTED_PAYLOAD)
        self.service.parse_message(self.template_id, CORRECTED_PAYLOAD)  # 同文重放也应产生新修订

        self.assertEqual(resent["delivery"]["status"], "已交付")
        self.assertEqual(resent["fields"]["qty"], 12)
        self.assertEqual(resent["revision"], 3)
        self.assertNotEqual(resent["payload_sha256"], first["payload_sha256"])
        self.assertEqual(resent["basis"]["revision"], 3)
        self.assertEqual(resent["basis"]["field_paths"]["qty"], ["order.qty", "items.count"])
        qty_transform = next(t for t in resent["transformations"] if t["field"] == "qty")
        self.assertEqual(qty_transform["source_path"], "order.qty")
        self.assertEqual(qty_transform["value"], 12)

        snapshots = self.service.list_snapshots(self.template_id)
        self.assertEqual([s["revision"] for s in snapshots], [1, 2, 3, 4])
        self.assertEqual(
            [s["payload_sha256"] for s in snapshots],
            [first["payload_sha256"], first["payload_sha256"],
             resent["payload_sha256"], resent["payload_sha256"]],
        )


class ConnectorIngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ApiConnectorService()

    def test_ingest_sends_payload_and_matched_template_to_parser(self) -> None:
        payload_text = json.dumps(ORDER_PAYLOAD, ensure_ascii=False)
        result = self.service.ingest_to_parser(
            CONNECTOR_ID, payload_text, message_id="api-test-1")

        self.assertFalse(result["ok"])  # 冲突未裁决，本次接入暂停交付
        self.assertEqual(result["matched_template"]["id"], DUAL_TEMPLATE_ID)
        self.assertEqual(result["connector"]["message_id"], "api-test-1")
        self.assertTrue(result["connector"]["payload_stored"])
        self.assertEqual(result["fields"], {"order_id": "A-1001", "amount": 99.5})
        self.assertEqual(result["delivery"]["status"], "暂停交付")
        self.assertIn("qty", result["delivery"]["undelivered_reason"])

        stored = [
            row for row in store.rows("payload_store")
            if row.get("消息ID") == "api-test-1"
        ]
        self.assertEqual(len(stored), 1)
        self.assertEqual(json.loads(stored[0]["原始载荷"]), ORDER_PAYLOAD)

    def test_ingest_with_unknown_connector_is_rejected(self) -> None:
        result = self.service.ingest_to_parser(9999, ORDER_PAYLOAD)
        self.assertFalse(result["ok"])


class ParseAuditIsolationTest(unittest.TestCase):
    def test_internal_audit_tables_do_not_leak_into_modules(self) -> None:
        MessageParserService().parse_message(SINGLE_TEMPLATE_ID, ORDER_PAYLOAD)

        self.assertFalse(
            [name for name in store.module_names() if name.startswith("_parser")])
        self.assertEqual(
            sorted(row["name"] for row in store.overview()["modules"]),
            sorted(store.module_names()),
        )


if __name__ == "__main__":
    unittest.main()
