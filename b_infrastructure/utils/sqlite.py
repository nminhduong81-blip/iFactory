from __future__ import annotations
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

_table_init_lock = asyncio.Lock()
_sqlite_pragma_applied: set[int] = set()

async def ensure_table_once(engine: AsyncEngine, model) -> None:
    async with _table_init_lock:
        async with engine.begin() as conn:
            await conn.run_sync(lambda sc: model.metadata.create_all(sc, tables=[model.__table__], checkfirst=True))
            if engine.dialect.name == "sqlite":
                eid = id(engine)
                if eid not in _sqlite_pragma_applied:
                    await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                    await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
                    await conn.exec_driver_sql("PRAGMA temp_store=MEMORY")
                    await conn.exec_driver_sql("PRAGMA cache_size=-65536")
                    await conn.exec_driver_sql("PRAGMA mmap_size=268435456")
                    await conn.exec_driver_sql("PRAGMA busy_timeout=15000")
                    _sqlite_pragma_applied.add(eid)