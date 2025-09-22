from __future__ import annotations
import asyncio
from typing import List
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.database.models.models_cold import StatusHistory
from b_infrastructure.utils.sqlite import ensure_table_once
from b_infrastructure.utils.time import to_local_naive
from d_application.dto import StatusEvent

class StatusHistoryRepository:
    def __init__(self, db: DatabaseOrchestrator, batch: int = 2000) -> None:
        self.db = db
        self.batch = max(1, batch)
        self._init_done = False

    async def initialize(self) -> None:
        if not self._init_done:
            await ensure_table_once(self.db.cold.engine, StatusHistory)
            self._init_done = True

    async def insert_events(self, events: List[StatusEvent]) -> int:
        if not events:
            return 0
        def _run():
            total = 0
            with self.db.cold.sync_engine.begin() as conn:
                stmt = sqlite_insert(StatusHistory).on_conflict_do_nothing(index_elements=["equip_code", "create_date"])
                for i in range(0, len(events), self.batch):
                    chunk = [{"equip_code": e["equip_code"], "equip_status": e.get("equip_status"), "create_date": to_local_naive(e["event_time"])} for e in events[i:i+self.batch]]
                    conn.execute(stmt, chunk)
                    total += len(chunk)
            return total
        return await asyncio.to_thread(_run)

    async def query_period(self, code: str, start, end) -> List[StatusEvent]:
        def _read():
            with self.db.cold.sync_engine_read.connect() as conn:
                stmt = (
                    select(StatusHistory.equip_code, StatusHistory.equip_status, StatusHistory.create_date)
                    .where(StatusHistory.equip_code == code)
                    .where(StatusHistory.create_date >= start)
                    .where(StatusHistory.create_date <= end)
                    .order_by(StatusHistory.create_date.desc())
                )
                return conn.execute(stmt).all()
        rows = await asyncio.to_thread(_read)
        return [{"equip_code": r[0], "equip_status": r[1], "event_time": to_local_naive(r[2])} for r in rows]