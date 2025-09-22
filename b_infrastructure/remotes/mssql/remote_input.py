from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from b_infrastructure.utils.time import to_local_naive
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.utils.threads import AsyncIterFromThread

SQL_MSSQL_LATEST_INPUT = text("""
WITH latest AS (
    SELECT EQUIP_CODE, MATERIAL_BATCH, FEED_TIME, ROW_NUMBER() OVER (PARTITION BY EQUIP_CODE ORDER BY FEED_TIME DESC) rn
    FROM yntti.dbo.RPT_FEEDING_DETAIL
    WHERE EQUIP_CODE IN :codes
)
SELECT EQUIP_CODE, MATERIAL_BATCH, FEED_TIME FROM latest WHERE rn=1
""").bindparams(bindparam("codes", expanding=True))

SQL_MSSQL_INPUT_PERIOD = text("""
SELECT EQUIP_CODE, MATERIAL_BATCH, FEED_TIME
FROM yntti.dbo.RPT_FEEDING_DETAIL
WHERE EQUIP_CODE IN :codes AND FEED_TIME BETWEEN :start AND :end
ORDER BY FEED_TIME ASC
""").bindparams(bindparam("codes", expanding=True))

def _engine(db: DatabaseOrchestrator) -> Engine:
    return db.mssql.engine

async def fetch_latest_input(db: DatabaseOrchestrator, codes: List[str]) -> List[Dict[str, Any]]:
    codes = sorted({str(c) for c in codes if c})
    if not codes:
        return []
    eng = _engine(db)
    def _exec() -> List[Dict[str, Any]]:
        with eng.connect() as conn:
            res = conn.execution_options(stream_results=True).execute(SQL_MSSQL_LATEST_INPUT, {"codes": codes})
            out: List[Dict[str, Any]] = []
            append = out.append
            for r in res:
                ft = r[2]
                if not ft:
                    continue
                append({"equip_code": str(r[0]), "material_batch": r[1], "feeding_time": to_local_naive(ft)})
            return out
    return await asyncio.to_thread(_exec)

async def stream_input_period(db: DatabaseOrchestrator, codes: List[str], start: datetime, end: datetime, chunk: int = 5000) -> AsyncGenerator[List[Dict[str, Any]], None]:
    eng = _engine(db)
    codes = sorted({str(c) for c in codes if c})
    if not codes:
        if False:
            yield []
        return
    def _iter():
        with eng.connect() as conn:
            res = conn.execution_options(stream_results=True).execute(SQL_MSSQL_INPUT_PERIOD, {"codes": codes, "start": start, "end": end})
            batch: List[Dict[str, Any]] = []
            append = batch.append
            for row in res:
                ft = row[2]
                if ft:
                    append({"equip_code": str(row[0]), "material_batch": row[1], "feeding_time": to_local_naive(ft)})
                    if len(batch) >= chunk:
                        yield batch
                        batch = []
                        append = batch.append
            if batch:
                yield batch
    async for b in AsyncIterFromThread(_iter()):
        yield b