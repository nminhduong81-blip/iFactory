# path: b_infrastructure/utils/layout.py
from __future__ import annotations
import asyncio
import json
from pathlib import Path
from typing import Any, Optional
from a_core.configs.config import get_config

_layout_path_key: Optional[str] = None
_layout_cache: dict[str, Any] = {}
_layout_mtime: float = 0.0
_layout_lock = asyncio.Lock()

async def load_layout() -> dict[str, Any]:
    global _layout_cache, _layout_mtime, _layout_path_key
    async with _layout_lock:
        cfg = get_config()
        path = Path(cfg.layout_path)
        key = str(path.resolve())
        if not path.exists():
            _layout_cache = {}
            _layout_mtime = 0.0
            _layout_path_key = key
            return _layout_cache
        mtime = path.stat().st_mtime
        if _layout_path_key != key or mtime != _layout_mtime:
            def _load() -> dict[str, Any]:
                with path.open("r", encoding="utf-8") as f:
                    return json.load(f)
            _layout_cache = await asyncio.to_thread(_load)
            _layout_mtime = mtime
            _layout_path_key = key
        return _layout_cache