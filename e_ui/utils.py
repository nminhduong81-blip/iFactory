from __future__ import annotations
from typing import Any, Optional
import unicodedata
from datetime import datetime, timezone

def strip_accents(text: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", text) if not unicodedata.combining(ch))

def to_datetime(v: Any) -> Optional[datetime]:
    dt: Optional[datetime] = None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v)
        except Exception:
            try:
                dt = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
    else:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt