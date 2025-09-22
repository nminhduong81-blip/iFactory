from __future__ import annotations
from typing import List, Optional, Dict
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.database.models.models_hot import LatestStatus
from b_infrastructure.utils.sqlite import ensure_table_once
from b_infrastructure.utils.time import to_local_naive
from d_application.dto import StatusSnapshot

class StatusSnapshotRepository:
    def __init__(self, db: DatabaseOrchestrator) -> None:
        self.db = db
        self._init_done = False

    async def initialize(self) -> None:
        if not self._init_done:
            await ensure_table_once(self.db.hot.engine, LatestStatus)
            self._init_done = True

    async def upsert_many(self, items: List[StatusSnapshot]) -> int:
        if not items:
            return 0
        uniq: Dict[str, StatusSnapshot] = {}
        for it in items:
            code = it["equip_code"]
            if not code:
                continue
            uniq[code] = it
        payload = [{"equip_code": it["equip_code"], "equip_status": it.get("equip_status"), "last_update": to_local_naive(it["as_of"])} for it in uniq.values()]
        total = 0
        async with self.db.hot.session() as s:
            base = sqlite_insert(LatestStatus)
            stmt = base.on_conflict_do_update(
                index_elements=[LatestStatus.equip_code],
                set_={"equip_status": base.excluded.equip_status, "last_update": base.excluded.last_update},
                where=base.excluded.last_update > LatestStatus.last_update
            )
            for i in range(0, len(payload), 1000):
                chunk = payload[i:i+1000]
                await s.execute(stmt, chunk)
                total += len(chunk)
        return total

    async def fetch_all(self, codes: Optional[List[str]] = None) -> List[StatusSnapshot]:
        async with self.db.hot.session_read() as s:
            q = select(LatestStatus)
            if codes:
                q = q.where(LatestStatus.equip_code.in_(list({str(c) for c in codes if c})))
            res = await s.execute(q)
            rows = res.scalars().all()
        return [{"equip_code": r.equip_code, "equip_status": r.equip_status, "as_of": r.last_update} for r in rows]