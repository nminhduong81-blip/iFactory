from __future__ import annotations
from pathlib import Path
from typing import Literal, cast
from a_core.configs.config import get_config
from b_infrastructure.database.manager import DatabaseOrchestrator, DBConfig, RemoteDBParams
from b_infrastructure.repositories.hot.status_snapshot import StatusSnapshotRepository
from b_infrastructure.repositories.hot.input_snapshot import InputSnapshotRepository
from b_infrastructure.repositories.cold.status_history import StatusHistoryRepository
from b_infrastructure.repositories.cold.input_history import InputHistoryRepository
from b_infrastructure.repositories.cold.status_period import StatusPeriodRepository
from b_infrastructure.repositories.cold.input_period import InputPeriodRepository
from b_infrastructure.repositories.sync_meta import SyncMetaRepo
from d_application.services.full_service import FullLoaderService

def build_container() -> tuple[FullLoaderService, DatabaseOrchestrator]:
    cfg = get_config()
    auth_val = cast(Literal["sql"], "sql")
    remote = RemoteDBParams(
        host=cfg.mssql_host,
        port=cfg.mssql_port,
        database=cfg.mssql_db,
        user=cfg.mssql_user,
        password=cfg.mssql_password,
        auth=auth_val,
    )
    base_dir = Path(getattr(cfg, "base_dir", Path.cwd()))
    dbm = DatabaseOrchestrator(base_dir, remote, DBConfig())
    status_snapshots = StatusSnapshotRepository(dbm)
    input_snapshots = InputSnapshotRepository(dbm)
    status_history = StatusHistoryRepository(dbm)
    input_history = InputHistoryRepository(dbm)
    status_periods = StatusPeriodRepository(dbm)
    input_periods = InputPeriodRepository(dbm)
    meta_repo_status = SyncMetaRepo(dbm.cold.engine)
    meta_repo_input = SyncMetaRepo(dbm.cold.engine)
    full_service = FullLoaderService(
        db=dbm,
        status_snapshots=status_snapshots,
        status_history=status_history,
        status_periods=status_periods,
        input_snapshots=input_snapshots,
        input_history=input_history,
        input_periods=input_periods,
        status_meta=meta_repo_status,
        input_meta=meta_repo_input,
    )
    return full_service, dbm