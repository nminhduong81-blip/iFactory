from __future__ import annotations
import asyncio
from datetime import datetime
from typing import List, Tuple, Dict, Any, cast
from sqlalchemy import select, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.database.models.models_cold import StatusPeriod
from b_infrastructure.utils.sqlite import ensure_table_once
from b_infrastructure.utils.time import to_local_naive
from d_application.dto import StatusEvent

class StatusPeriodRepository:
    def __init__(self, db: DatabaseOrchestrator, batch: int = 2000) -> None:
        self.db = db
        self.batch = max(1, batch)
        self._init_done = False

    async def initialize(self) -> None:
        if not self._init_done:
            await ensure_table_once(self.db.cold.engine, StatusPeriod)
            self._init_done = True

    async def insert_events(self, events: List[StatusEvent]) -> int:
        if not events:
            return 0
        def _run():
            seen: set[Tuple[str, datetime]] = set()
            rows: List[Dict[str, Any]] = []
            for e in events:
                code = str(e["equip_code"])
                ts = cast(datetime, e["event_time"])
                if not code or not ts:
                    continue
                k = (code, ts)
                if k in seen:
                    continue
                seen.add(k)
                rows.append({"equip_code": code, "equip_status": e.get("equip_status"), "end_time": ts})
            if not rows:
                return 0
            total = 0
            with self.db.cold.sync_engine.begin() as conn:
                stmt = sqlite_insert(StatusPeriod).on_conflict_do_nothing(index_elements=["equip_code", "end_time"])
                for i in range(0, len(rows), self.batch):
                    chunk = rows[i:i+self.batch]
                    conn.execute(stmt, chunk)
                    total += len(chunk)
            return total
        return await asyncio.to_thread(_run)

    async def query_period(self, codes: List[str], start: datetime, end: datetime) -> List[StatusEvent]:
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
                    prev_sq = (
                        select(
                            StatusPeriod.equip_code.label("equip_code"),
                            StatusPeriod.equip_status.label("equip_status"),
                            StatusPeriod.end_time.label("event_time"),
                            func.row_number().over(
                                partition_by=StatusPeriod.equip_code,
                                order_by=StatusPeriod.end_time.desc(),
                            ).label("rn"),
                        )
                        .where(StatusPeriod.equip_code.in_(part))
                        .where(StatusPeriod.end_time < start)
                    ).subquery("prev")
                    prev_sel = select(prev_sq.c.equip_code, prev_sq.c.equip_status, prev_sq.c.event_time).where(prev_sq.c.rn == 1)
                    curr_sel = (
                        select(
                            StatusPeriod.equip_code.label("equip_code"),
                            StatusPeriod.equip_status.label("equip_status"),
                            StatusPeriod.end_time.label("event_time"),
                        )
                        .where(StatusPeriod.equip_code.in_(part))
                        .where(StatusPeriod.end_time >= start)
                        .where(StatusPeriod.end_time <= end)
                    )
                    union_sub = prev_sel.union_all(curr_sel).subquery("u")
                    stmt = select(union_sub.c.equip_code, union_sub.c.equip_status, union_sub.c.event_time).order_by(union_sub.c.event_time.asc())
                    out.extend(conn.execute(stmt).all())
            return out
        rows = await asyncio.to_thread(_read)
        items: List[StatusEvent] = []
        for r in rows:
            ev = r[2]
            if isinstance(ev, str):
                try:
                    ev = datetime.fromisoformat(ev)
                except Exception:
                    continue
            equip_code = str(r[0])
            equip_status = None if r[1] is None else str(r[1])
            event_time = to_local_naive(cast(datetime, ev))
            items.append(cast(StatusEvent, {"equip_code": equip_code, "equip_status": equip_status, "event_time": event_time}))
        return items