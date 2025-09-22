from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Callable, Dict, Any
from b_infrastructure.database.manager import DatabaseOrchestrator
from d_application.services.status_service import StatusLoaderService
from d_application.services.input_service import InputLoaderService
from d_application.ports import (
    StatusSnapshotRepoPort,
    StatusHistoryRepoPort,
    StatusPeriodRepoPort,
    InputSnapshotRepoPort,
    InputHistoryRepoPort,
    InputPeriodRepoPort,
    SyncMetaRepoPort,
)

logger = logging.getLogger(__name__)

class FullLoaderService:
    def __init__(
        self,
        db: DatabaseOrchestrator,
        status_snapshots: StatusSnapshotRepoPort,
        status_history: StatusHistoryRepoPort,
        status_periods: StatusPeriodRepoPort,
        input_snapshots: InputSnapshotRepoPort,
        input_history: InputHistoryRepoPort,
        input_periods: InputPeriodRepoPort,
        status_meta: SyncMetaRepoPort,
        input_meta: SyncMetaRepoPort,
    ) -> None:
        self.status_service = StatusLoaderService(db, status_snapshots, status_history, status_periods, status_meta)
        self.input_service = InputLoaderService(db, input_snapshots, input_history, input_periods, input_meta)
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        await asyncio.gather(self.status_service.initialize(), self.input_service.initialize())
        self._initialized = True

    async def full_sync(self, codes: Optional[List[str]] = None) -> None:
        await asyncio.gather(self.status_service.sync_latest(codes), self.input_service.sync_latest(codes))

    async def sync_from_remote(self, codes: Optional[List[str]] = None) -> None:
        await self.full_sync(codes)

    async def Sync_from_remote(self, codes: Optional[List[str]] = None) -> None:
        await self.sync_from_remote(codes)

    async def sync_status(self, codes: Optional[List[str]] = None):
        await self.status_service.sync_latest(codes)
        return await self.status_service.get_latest(codes)

    async def sync_input(self, codes: Optional[List[str]] = None):
        await self.input_service.sync_latest(codes)
        return await self.input_service.get_latest(codes)

    async def sync_period(self, codes: List[str], start: datetime, end: datetime, progress_cb: Optional[Callable[[str, int], None]] = None) -> None:
        await asyncio.gather(
            self.status_service.sync_period(codes, start, end, progress_cb=progress_cb),
            self.input_service.sync_period(codes, start, end, progress_cb=progress_cb),
        )

    async def query_status_period(self, codes: List[str], start: datetime, end: datetime):
        return await self.status_service.query_period(codes, start, end)

    async def query_input_period(self, codes: List[str], start: datetime, end: datetime):
        return await self.input_service.query_period(codes, start, end)

    async def full_sync_quick(self, codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        await self.full_sync(codes)
        statuses, inputs = await asyncio.gather(self.status_service.get_latest(codes), self.input_service.get_latest(codes))
        return [{"status": statuses}, {"input": inputs}]