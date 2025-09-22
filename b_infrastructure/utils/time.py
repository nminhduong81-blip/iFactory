from __future__ import annotations
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def local_tz():
    if ZoneInfo:
        try:
            return ZoneInfo("Asia/Ho_Chi_Minh")
        except Exception:
            pass
    return timezone(timedelta(hours=7))

def to_local_naive(dt: datetime) -> datetime:
    if dt is None:
        return dt
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(local_tz()).replace(tzinfo=None)