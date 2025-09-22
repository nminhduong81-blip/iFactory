from __future__ import annotations
import asyncio
import contextlib
from typing import List, Tuple, cast
from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncEngine
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.repositories.cold.input_period import InputPeriodRepository
from b_infrastructure.repositories.cold.status_period import StatusPeriodRepository
from d_application.dto import InputEvent, StatusEvent

class OutboxRepo:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def initialize(self):
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS outbox_input (
                    equip_code TEXT NOT NULL,
                    material_batch TEXT NOT NULL,
                    feeding_time DATETIME NOT NULL,
                    PRIMARY KEY (equip_code, feeding_time)
                )
            """))
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS outbox_status (
                    equip_code TEXT NOT NULL,
                    equip_status TEXT,
                    event_time DATETIME NOT NULL,
                    PRIMARY KEY (equip_code, event_time)
                )
            """))
            await conn.execute(text("""
                CREATE TRIGGER IF NOT EXISTS trg_latest_input_to_outbox_ins
                AFTER INSERT ON latest_input
                BEGIN
                    INSERT OR IGNORE INTO outbox_input (equip_code, material_batch, feeding_time)
                    VALUES (NEW.equip_code, NEW.material_batch, NEW.feeding_time);
                END
            """))
            await conn.execute(text("""
                CREATE TRIGGER IF NOT EXISTS trg_latest_input_to_outbox_upd
                AFTER UPDATE ON latest_input
                WHEN NEW.feeding_time > OLD.feeding_time
                BEGIN
                    INSERT OR IGNORE INTO outbox_input (equip_code, material_batch, feeding_time)
                    VALUES (NEW.equip_code, NEW.material_batch, NEW.feeding_time);
                END
            """))
            await conn.execute(text("""
                CREATE TRIGGER IF NOT EXISTS trg_latest_status_to_outbox_ins
                AFTER INSERT ON latest_status
                BEGIN
                    INSERT OR IGNORE INTO outbox_status (equip_code, equip_status, event_time)
                    VALUES (NEW.equip_code, NEW.equip_status, NEW.last_update);
                END
            """))
            await conn.execute(text("""
                CREATE TRIGGER IF NOT EXISTS trg_latest_status_to_outbox_upd
                AFTER UPDATE ON latest_status
                WHEN NEW.last_update > OLD.last_update
                BEGIN
                    INSERT OR IGNORE INTO outbox_status (equip_code, equip_status, event_time)
                    VALUES (NEW.equip_code, NEW.equip_status, NEW.last_update);
                END
            """))

class OutboxDrainer:
    def __init__(self, db: DatabaseOrchestrator, batch: int = 2000, interval: float = 0.2):
        self.db = db
        self.batch = max(1, batch)
        self.interval = max(0.01, interval)
        self._task = None
        self._stop = asyncio.Event()

    async def start(self):
        ih = InputPeriodRepository(self.db)
        sh = StatusPeriodRepository(self.db)
        await asyncio.gather(ih.initialize(), sh.initialize())
        self._task = asyncio.create_task(self._run(ih, sh))

    async def stop(self):
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()
                with contextlib.suppress(Exception):
                    await self._task

    def _pop(self) -> Tuple[List[InputEvent], List[StatusEvent]]:
        rows_inp: List[InputEvent] = []
        rows_sta: List[StatusEvent] = []
        with self.db.hot.sync_engine.begin() as conn:
            r_inp = conn.execute(text("""
                SELECT rowid, equip_code, material_batch, feeding_time
                FROM outbox_input
                ORDER BY feeding_time
                LIMIT :n
            """), {"n": self.batch}).all()
            if r_inp:
                ids = [x[0] for x in r_inp]
                del_in = text("DELETE FROM outbox_input WHERE rowid IN :ids").bindparams(bindparam("ids", expanding=True))
                conn.execute(del_in, {"ids": ids})
                rows_inp = cast(List[InputEvent], [{"equip_code": x[1], "material_batch": x[2], "feeding_time": x[3]} for x in r_inp])
            r_sta = conn.execute(text("""
                SELECT rowid, equip_code, equip_status, event_time
                FROM outbox_status
                ORDER BY event_time
                LIMIT :n
            """), {"n": self.batch}).all()
            if r_sta:
                ids = [x[0] for x in r_sta]
                del_st = text("DELETE FROM outbox_status WHERE rowid IN :ids").bindparams(bindparam("ids", expanding=True))
                conn.execute(del_st, {"ids": ids})
                rows_sta = cast(List[StatusEvent], [{"equip_code": x[1], "equip_status": x[2], "event_time": x[3]} for x in r_sta])
        return rows_inp, rows_sta

    async def _run(self, ih: InputPeriodRepository, sh: StatusPeriodRepository):
        try:
            while not self._stop.is_set():
                rows_inp, rows_sta = await asyncio.to_thread(self._pop)
                if rows_inp:
                    await ih.insert_events(rows_inp)
                if rows_sta:
                    await sh.insert_events(rows_sta)
                if not rows_inp and not rows_sta:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                    except asyncio.TimeoutError:
                        pass
        except asyncio.CancelledError:
            return