from __future__ import annotations
import asyncio
import queue
import threading
from typing import Any

class AsyncIterFromThread:
    def __init__(self, gen):
        self._gen = gen
        self._q: queue.Queue[Any] = queue.Queue(maxsize=2048)
        self._sentinel = object()
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()

    def _run(self):
        try:
            for item in self._gen:
                self._q.put(item)
        finally:
            self._q.put(self._sentinel)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await asyncio.to_thread(self._q.get)
        if item is self._sentinel:
            raise StopAsyncIteration
        return item