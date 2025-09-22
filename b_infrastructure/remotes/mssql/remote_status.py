from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from b_infrastructure.utils.time import to_local_naive
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.utils.threads import AsyncIterFromThread

SQL_MSSQL_LATEST_STATUS = text("""
WITH latest AS (
    SELECT EQUIP_CODE, EQUIP_STATUS, START_TIME,
           ROW_NUMBER() OVER (PARTITION BY EQUIP_CODE ORDER BY START_TIME DESC) rn
    FROM TT_EQ_STATUS
    WHERE EQUIP_CODE IN :codes
)
SELECT EQUIP_CODE, EQUIP_STATUS, START_TIME AS EVENT_TIME
FROM latest WHERE rn=1
""").bindparams(bindparam("codes", expanding=True))

SQL_MSSQL_STATUS_PERIOD = text("""
SELECT EQUIP_CODE, EQUIP_STATUS, START_TIME
FROM TT_EQ_STATUS
WHERE EQUIP_CODE IN :codes AND (
    (START_TIME BETWEEN :start AND :end)
    OR (END_TIME BETWEEN :start AND :end)
    OR (START_TIME < :start AND (END_TIME IS NULL OR END_TIME >= :start))
)
ORDER BY START_TIME ASC
""").bindparams(bindparam("codes", expanding=True))

def _engine(db: DatabaseOrchestrator) -> Engine:
    return db.mssql.engine

async def fetch_latest_status(db: DatabaseOrchestrator, codes: List[str]) -> List[Dict[str, Any]]:
    codes = sorted({str(c) for c in codes if c})
    if not codes:
        return []
    eng = _engine(db)
    def _exec() -> List[Dict[str, Any]]:
        with eng.connect() as conn:
            res = conn.execution_options(stream_results=True).execute(SQL_MSSQL_LATEST_STATUS, {"codes": codes})
            out: List[Dict[str, Any]] = []
            append = out.append
            to_local = to_local_naive
            for r in res:
                ev = r[2]
                if ev:
                    append({"equip_code": str(r[0]), "equip_status": None if r[1] is None else str(r[1]), "event_time": to_local(ev)})
            return out
    return await asyncio.to_thread(_exec)

async def stream_status_period(db: DatabaseOrchestrator, codes: List[str], start: datetime, end: datetime, chunk: int = 5000) -> AsyncGenerator[List[Dict[str, Any]], None]:
    eng = _engine(db)
    codes = sorted({str(c) for c in codes if c})
    if not codes:
        if False:
            yield []
        return
    def _iter():
        with eng.connect() as conn:
            res = conn.execution_options(stream_results=True).execute(SQL_MSSQL_STATUS_PERIOD, {"codes": codes, "start": start, "end": end})
            batch: List[Dict[str, Any]] = []
            append = batch.append
            to_local = to_local_naive
            for row in res:
                ev = row[2]
                if ev:
                    append({"equip_code": str(row[0]), "equip_status": None if row[1] is None else str(row[1]), "event_time": to_local(ev)})
                    if len(batch) >= chunk:
                        yield batch
                        batch = []
                        append = batch.append
            if batch:
                yield batch
    async for b in AsyncIterFromThread(_iter()):
        yield b