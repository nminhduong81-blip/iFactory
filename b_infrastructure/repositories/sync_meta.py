# path: b_infrastructure/repositories/sync_meta.py
from __future__ import annotations
from sqlalchemy import text
from datetime import datetime
from typing import Optional
from sqlalchemy import Table, Column, String, DateTime, MetaData, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncEngine

metadata = MetaData()

sync_meta = Table(
    "sync_meta",
    metadata,
    Column("table_name", String(64), primary_key=True),
    Column("last_synced", DateTime),
)

class SyncMetaRepo:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def initialize(self):
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sync_meta (
                    table_name VARCHAR(64) PRIMARY KEY,
                    last_synced DATETIME
                )
            """))

    async def get_last_synced(self, table_name: str) -> Optional[datetime]:
        async with self.engine.begin() as conn:
            result = await conn.execute(select(sync_meta.c.last_synced).where(sync_meta.c.table_name == table_name))
            row = result.first()
            return row[0] if row else None

    async def set_last_synced(self, table_name: str, ts: datetime) -> None:
        async with self.engine.begin() as conn:
            stmt = insert(sync_meta).values(table_name=table_name, last_synced=ts).on_conflict_do_update(
                index_elements=[sync_meta.c.table_name], set_={"last_synced": ts}
            )
            await conn.execute(stmt)