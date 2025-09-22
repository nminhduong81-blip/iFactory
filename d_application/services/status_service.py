from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Optional, List, Callable, cast, Set
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.remotes.mssql.remote_status import fetch_latest_status, stream_status_period
from b_infrastructure.utils.layout import load_layout
from d_application.dto import StatusEvent, StatusSnapshot
from d_application.ports import StatusSnapshotRepoPort, StatusHistoryRepoPort, StatusPeriodRepoPort, SyncMetaRepoPort

async def _resolve_codes() -> List[str]:
    layout = await load_layout()
    floors = layout.get("floors") or {}
    return [str(it.get("id")) for f in floors.values() for it in f.get("items", []) if it.get("id")]

class StatusLoaderService:
    def __init__(self, db: DatabaseOrchestrator, snapshots: StatusSnapshotRepoPort, history: StatusHistoryRepoPort, periods: StatusPeriodRepoPort, meta: SyncMetaRepoPort) -> None:
        self.db = db
        self.snapshots = snapshots
        self.history = history
        self.periods = periods
        self.meta = meta
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        await asyncio.gather(self.snapshots.initialize(), self.history.initialize(), self.periods.initialize(), self.meta.initialize())
        self._initialized = True

    async def sync_latest(self, codes: Optional[List[str]] = None) -> int:
        codes = codes or await _resolve_codes()
        events = cast(List[StatusEvent], await fetch_latest_status(self.db, codes))
        if not events:
            return 0
        snaps: List[StatusSnapshot] = [{"equip_code": e["equip_code"], "equip_status": e.get("equip_status"), "as_of": e["event_time"]} for e in events]
        n1, n2 = await asyncio.gather(self.snapshots.upsert_many(snaps), self.history.insert_events(events))
        return max(n1, n2)

    async def get_latest(self, codes: Optional[List[str]] = None) -> List[StatusSnapshot]:
        return await self.snapshots.fetch_all(codes)

    async def sync_period(self, codes: List[str], start: datetime, end: datetime, progress_cb: Optional[Callable[[str, int], None]] = None, backfill: bool = True) -> int:
        codes = codes or await _resolve_codes()
        if backfill:
            fetch_start = start
        else:
            last_synced = await self.meta.get_last_synced("status_period")
            fetch_start = max(start, last_synced) if last_synced else start
        total = 0
        pending: Set[asyncio.Task[int]] = set()
        limit = 4
        async for chunk in stream_status_period(self.db, codes, fetch_start, end):
            t = asyncio.create_task(self.periods.insert_events(cast(List[StatusEvent], chunk)))
            pending.add(t)
            if len(pending) >= limit:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    total += d.result()
                    if progress_cb:
                        progress_cb("status", total)
        if pending:
            results = await asyncio.gather(*pending)
            for r in results:
                total += r
                if progress_cb:
                    progress_cb("status", total)
        last_synced = await self.meta.get_last_synced("status_period")
        if not last_synced or end > last_synced:
            await self.meta.set_last_synced("status_period", end)
        return total

    async def query_period(self, codes: List[str], start: datetime, end: datetime) -> List[StatusEvent]:
        res = await self.periods.query_period(codes, start, end)
        return cast(List[StatusEvent], res)