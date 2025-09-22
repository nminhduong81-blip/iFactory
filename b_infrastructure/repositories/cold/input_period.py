from __future__ import annotations
import asyncio
from datetime import datetime
from typing import List, Tuple, Dict, Any, cast
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.database.models.models_cold import InputPeriod
from b_infrastructure.utils.sqlite import ensure_table_once
from b_infrastructure.utils.time import to_local_naive
from d_application.dto import InputEvent

class InputPeriodRepository:
    def __init__(self, db: DatabaseOrchestrator, batch: int = 2000) -> None:
        self.db = db
        self.batch = max(1, batch)
        self._init_done = False

    async def initialize(self) -> None:
        if not self._init_done:
            await ensure_table_once(self.db.cold.engine, InputPeriod)
            self._init_done = True

    async def insert_events(self, events: List[InputEvent]) -> int:
        if not events:
            return 0
        def _run():
            seen: set[Tuple[str, datetime]] = set()
            rows: List[Dict[str, Any]] = []
            for e in events:
                code = str(e["equip_code"])
                ft = to_local_naive(cast(datetime, e["feeding_time"]))
                if not code or not ft:
                    continue
                k = (code, ft)
                if k in seen:
                    continue
                seen.add(k)
                rows.append({"equip_code": code, "material_batch": e["material_batch"], "feeding_time": ft, "create_date": ft})
            if not rows:
                return 0
            total = 0
            with self.db.cold.sync_engine.begin() as conn:
                stmt = sqlite_insert(InputPeriod).on_conflict_do_nothing(index_elements=["equip_code", "feeding_time"])
                for i in range(0, len(rows), self.batch):
                    chunk = rows[i:i+self.batch]
                    conn.execute(stmt, chunk)
                    total += len(chunk)
            return total
        return await asyncio.to_thread(_run)

    async def query_period(self, codes: List[str], start: datetime, end: datetime) -> List[InputEvent]:
        if not codes:
            return []
        codes = list({str(c) for c in codes if c})
        def _chunks(xs: List[str], n: int):
            for i in range(0, len(xs), n):
                yield xs[i:i+n]
        def _read():
            out = []
            with self.db.cold.sync_engine_read.connect() as conn:
                for part in _chunks(codes, 800):
                    stmt = (
                        select(InputPeriod.equip_code, InputPeriod.material_batch, InputPeriod.feeding_time)
                        .where(InputPeriod.equip_code.in_(part))
                        .where(InputPeriod.feeding_time >= start)
                        .where(InputPeriod.feeding_time <= end)
                        .order_by(InputPeriod.feeding_time.desc())
                    )
                    out.extend(conn.execute(stmt).all())
            return out
        rows = await asyncio.to_thread(_read)
        items: List[InputEvent] = []
        for r in rows:
            items.append({"equip_code": str(r[0]), "material_batch": str(r[1]), "feeding_time": to_local_naive(cast(datetime, r[2]))})
        items.sort(key=lambda x: x["feeding_time"], reverse=True)
        return items