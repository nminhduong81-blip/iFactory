from __future__ import annotations
import asyncio
import logging
from d_application.services.full_service import FullLoaderService

logger = logging.getLogger(__name__)

class FullLoaderRunner:
    def __init__(self, service: FullLoaderService, interval_sec: int = 30) -> None:
        self.service = service
        self.interval_sec = interval_sec
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        await self.service.initialize()
        try:
            while not self._stopping.is_set():
                try:
                    await self.service.full_sync()
                except Exception as e:
                    logger.error("full_sync failed: %s", e, exc_info=True)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self.interval_sec)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._stopping.set()