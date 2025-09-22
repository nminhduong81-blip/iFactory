from __future__ import annotations
import asyncio
import logging
import os
from datetime import datetime
from typing import List, Set, cast
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.remotes.mssql.remote_status import fetch_latest_status, stream_status_period
from b_infrastructure.remotes.mssql.remote_input import fetch_latest_input, stream_input_period
from b_infrastructure.repositories.hot.status_snapshot import StatusSnapshotRepository
from b_infrastructure.repositories.hot.input_snapshot import InputSnapshotRepository
from b_infrastructure.repositories.cold.status_period import StatusPeriodRepository
from b_infrastructure.repositories.cold.input_period import InputPeriodRepository
from b_infrastructure.utils.layout import load_layout
from d_application.dto import StatusEvent, InputEvent, StatusSnapshot, InputSnapshot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def _resolve_codes() -> List[str]:
    layout = await load_layout()
    floors = layout.get("floors", {})
    return [str(item["id"]) for f in floors.values() for item in f.get("items", []) if "id" in item and item["id"]]

async def sync_latest_status(db: DatabaseOrchestrator) -> None:
    codes = await _resolve_codes()
    events = cast(List[StatusEvent], await fetch_latest_status(db, codes))
    if not events:
        return
    snaps: List[StatusSnapshot] = [{"equip_code": e["equip_code"], "equip_status": e.get("equip_status"), "as_of": e["event_time"]} for e in events]
    repo = StatusSnapshotRepository(db)
    await repo.initialize()
    await repo.upsert_many(snaps)

async def sync_latest_input(db: DatabaseOrchestrator) -> None:
    codes = await _resolve_codes()
    events = cast(List[InputEvent], await fetch_latest_input(db, codes))
    if not events:
        return
    snaps: List[InputSnapshot] = [{"equip_code": e["equip_code"], "material_batch": e["material_batch"], "feeding_time": e["feeding_time"]} for e in events]
    repo = InputSnapshotRepository(db)
    await repo.initialize()
    await repo.upsert_many(snaps)

async def sync_status_period(db: DatabaseOrchestrator, codes: List[str], start: datetime, end: datetime) -> None:
    repo = StatusPeriodRepository(db)
    await repo.initialize()
    pending: Set[asyncio.Task[int]] = set()
    limit = max(2, min(8, (os.cpu_count() or 4) // 2))
    async for chunk in stream_status_period(db, codes, start, end):
        t = asyncio.create_task(repo.insert_events(cast(List[StatusEvent], chunk)))
        pending.add(t)
        if len(pending) >= limit:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for d in done:
                _ = d.result()
    if pending:
        await asyncio.gather(*pending)

async def sync_input_period(db: DatabaseOrchestrator, codes: List[str], start: datetime, end: datetime) -> None:
    repo = InputPeriodRepository(db)
    await repo.initialize()
    pending: Set[asyncio.Task[int]] = set()
    limit = max(2, min(8, (os.cpu_count() or 4) // 2))
    async for chunk in stream_input_period(db, codes, start, end):
        t = asyncio.create_task(repo.insert_events(cast(List[InputEvent], chunk)))
        pending.add(t)
        if len(pending) >= limit:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for d in done:
                _ = d.result()
    if pending:
        await asyncio.gather(*pending)

async def full_sync(db: DatabaseOrchestrator) -> None:
    codes = await _resolve_codes()
    if not codes:
        return
    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now
    await asyncio.gather(
        sync_latest_status(db),
        sync_latest_input(db),
        sync_status_period(db, codes, start, end),
        sync_input_period(db, codes, start, end),
    )