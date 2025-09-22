from __future__ import annotations
from typing import List, Optional, Dict
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from b_infrastructure.database.manager import DatabaseOrchestrator
from b_infrastructure.database.models.models_hot import LatestInput
from b_infrastructure.utils.sqlite import ensure_table_once
from b_infrastructure.utils.time import to_local_naive
from d_application.dto import InputSnapshot

class InputSnapshotRepository:
    def __init__(self, db: DatabaseOrchestrator) -> None:
        self.db = db
        self._init_done = False

    async def initialize(self) -> None:
        if not self._init_done:
            await ensure_table_once(self.db.hot.engine, LatestInput)
            self._init_done = True

    async def upsert_many(self, items: List[InputSnapshot]) -> int:
        if not items:
            return 0
        uniq: Dict[str, InputSnapshot] = {}
        for it in items:
            code = it["equip_code"]
            if not code:
                continue
            uniq[code] = it
        payload = [{"equip_code": it["equip_code"], "material_batch": it["material_batch"], "feeding_time": to_local_naive(it["feeding_time"])} for it in uniq.values()]
        total = 0
        async with self.db.hot.session() as s:
            base = sqlite_insert(LatestInput)
            stmt = base.on_conflict_do_update(
                index_elements=[LatestInput.equip_code],
                set_={"material_batch": base.excluded.material_batch, "feeding_time": base.excluded.feeding_time},
                where=base.excluded.feeding_time > LatestInput.feeding_time
            )
            for i in range(0, len(payload), 1000):
                chunk = payload[i:i+1000]
                await s.execute(stmt, chunk)
                total += len(chunk)
        return total

    async def fetch_all(self, codes: Optional[List[str]] = None) -> List[InputSnapshot]:
        async with self.db.hot.session_read() as s:
            q = select(LatestInput)
            if codes:
                q = q.where(LatestInput.equip_code.in_(list({str(c) for c in codes if c})))
            res = await s.execute(q)
            rows = res.scalars().all()
        return [{"equip_code": r.equip_code, "material_batch": r.material_batch, "feeding_time": r.feeding_time} for r in rows]