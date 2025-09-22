from __future__ import annotations
import asyncio
import traceback
from typing import List, Optional, Protocol, runtime_checkable, Callable, Union, Any
from PySide6.QtCore import QObject, QTimer, Signal
from qasync import asyncSlot

@runtime_checkable
class ILoaderService(Protocol):
    async def sync_from_remote(self, codes: Optional[List[str]] = None) -> None: ...

@runtime_checkable
class IFullLoaderService(ILoaderService, Protocol):
    async def full_sync_quick(self, codes: Optional[List[str]] = None) -> List[dict]: ...
    async def sync_period(self, codes: List[str], start, end, progress_cb: Optional[Callable[[str, int], None]] = None) -> None: ...

@runtime_checkable
class Initializable(Protocol):
    async def initialize(self) -> None: ...

class LoadController(QObject):
    progress = Signal(str)
    first_batch_ready = Signal(list)

    def __init__(self, service: Union[ILoaderService, IFullLoaderService], refresh_ms: int, codes: Optional[List[str]] = None, quick_mode: bool = False):
        super().__init__()
        self.service = service
        self.codes = list(codes or [])
        self.quick_mode = quick_mode
        self.poll_timer = QTimer(self)
        self.poll_timer.setInterval(refresh_ms)
        self.poll_timer.timeout.connect(self._poll_status)
        self._started = False
        self._stop_flag = False
        self._first_ready_emitted = False
        self._lock = asyncio.Lock()
        self._current_task: Optional[asyncio.Task[Any]] = None

    def start(self):
        if self._started:
            return
        self._started = True
        self._stop_flag = False
        if isinstance(self.service, Initializable):
            asyncio.create_task(self._safe_initialize())
        self.poll_timer.start()
        self.progress.emit("LoadController started.")

    def stop(self):
        self._stop_flag = True
        self.poll_timer.stop()
        self._started = False
        if self._current_task and not self._current_task.done():
            self._current_task.cancel()
        self.progress.emit("LoadController stopped.")

    async def _safe_initialize(self):
        try:
            await self.service.initialize()  # type: ignore[attr-defined]
            self.progress.emit("Service initialized.")
        except Exception as ex:
            msg = "".join(traceback.format_exception_only(type(ex), ex)).strip()
            self.progress.emit(f"Init error: {msg}")

    @asyncSlot()
    async def _poll_status(self):
        if self._stop_flag or self._lock.locked():
            return
        async with self._lock:
            try:
                self.progress.emit("🔄 Fetching status...")
                if self.quick_mode and hasattr(self.service, "full_sync_quick"):
                    self._current_task = asyncio.create_task(self.service.full_sync_quick(self.codes))  # type: ignore[attr-defined]
                    data = await self._current_task
                    self.progress.emit("✅ HOT quick sync done")
                    if not self._first_ready_emitted:
                        self._first_ready_emitted = True
                        self.first_batch_ready.emit(data)
                        self.progress.emit("First batch ready (quick)")
                else:
                    self._current_task = asyncio.create_task(self.service.sync_from_remote(self.codes))
                    await self._current_task
                    self.progress.emit("✅ Synced from remote")
                    if not self._first_ready_emitted:
                        self._first_ready_emitted = True
                        self.first_batch_ready.emit([{"status": []}, {"input": []}])
                        self.progress.emit("First batch ready")
            except asyncio.CancelledError:
                self.progress.emit("Polling cancelled.")
            except Exception as ex:
                err_msg = "".join(traceback.format_exception_only(type(ex), ex)).strip()
                self.progress.emit(f"❌ Error polling: {err_msg}")
            finally:
                self._current_task = None