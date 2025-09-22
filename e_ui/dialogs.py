from __future__ import annotations
from typing import List
from e_ui.base_dialog import BaseTableDialog

class StatusDialog(BaseTableDialog):
    def __init__(self, parent, equip_code: str, rows: List[dict]):
        headers = ["equip_code", "equip_status", "event_time"]
        super().__init__(parent, f"Status {equip_code}", headers)
        self.set_status_column(1)
        self.set_time_column_by_header("event_time")
        formatted = [[r.get("equip_code", ""), r.get("equip_status", ""), r.get("event_time", "")] for r in rows]
        self.load_rows(formatted)

class WipDialog(BaseTableDialog):
    def __init__(self, parent, equip_code: str, rows: List[dict]):
        headers = ["equip_code", "equip_status", "event_time"]
        super().__init__(parent, f"WIP {equip_code}", headers)
        self.set_status_column(1)
        self.set_time_column_by_header("event_time")
        formatted = [[r.get("equip_code", ""), r.get("equip_status", ""), r.get("event_time", "")] for r in rows]
        self.load_rows(formatted)

class EipDialog(BaseTableDialog):
    def __init__(self, parent, equip_code: str, rows: List[dict]):
        headers = ["equip_code", "equip_status", "event_time"]
        super().__init__(parent, f"EIP {equip_code}", headers)
        self.set_status_column(1)
        self.set_time_column_by_header("event_time")
        formatted = [[r.get("equip_code", ""), r.get("equip_status", ""), r.get("event_time", "")] for r in rows]
        self.load_rows(formatted)

class InputDialog(BaseTableDialog):
    def __init__(self, parent, equip_code: str, rows: List[dict]):
        headers = ["equip_code", "material_batch", "feeding_time"]
        super().__init__(parent, f"Input {equip_code}", headers)
        self.set_time_column_by_header("feeding_time")
        formatted = [[r.get("equip_code", ""), r.get("material_batch", ""), r.get("feeding_time", "")] for r in rows]
        self.load_rows(formatted)