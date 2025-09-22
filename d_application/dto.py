from __future__ import annotations
from typing import TypedDict, Optional
from datetime import datetime

class StatusEvent(TypedDict):
    equip_code: str
    equip_status: Optional[str]
    event_time: datetime

class InputEvent(TypedDict):
    equip_code: str
    material_batch: str
    feeding_time: datetime

class StatusSnapshot(TypedDict):
    equip_code: str
    equip_status: Optional[str]
    as_of: datetime

class InputSnapshot(TypedDict):
    equip_code: str
    material_batch: str
    feeding_time: datetime