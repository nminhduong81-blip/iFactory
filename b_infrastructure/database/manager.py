from __future__ import annotations
import asyncio
import functools
import os
import importlib.util
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, AsyncGenerator, Literal, Union, Dict, Any, List, Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from tenacity import retry, stop_after_attempt, wait_fixed
from b_infrastructure.database.base import HotBase, ColdBase

ToSqlMethod = Union[Literal["multi"], None, Callable[[Any, Any, List[str], Iterable[tuple[Any, ...]]], Optional[int]]]

@dataclass
class DBConfig:
    echo: bool = False
    cache_size: int = -262144
    mmap_size: int = 2147483648
    pool_size: int = 20
    max_overflow: int = 40
    pool_timeout: int = 30
    pool_recycle: int = 1800
    pool_use_lifo: bool = True
    dtype_backend: Union[Literal["pyarrow"], Literal["numpy_nullable"]] = "pyarrow"
    sqlite_isolation: Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] = "IMMEDIATE"
    sqlite_bulk_synchronous: Union[Literal["OFF"], Literal["NORMAL"]] = "OFF"
    sqlite_bulk_locking: Union[Literal["NORMAL"], Literal["EXCLUSIVE"]] = "EXCLUSIVE"
    sqlite_page_size: int = 32768
    sqlite_threads: int = max(2, (os.cpu_count() or 4))
    mssql_mars: bool = False
    mssql_encrypt: bool = True
    mssql_trust_server_certificate: bool = True
    mssql_multi_subnet_failover: bool = True
    mssql_application_intent: Literal["ReadWrite", "ReadOnly"] = "ReadWrite"
    mssql_connect_timeout: int = 15
    mssql_fast_executemany: bool = True
    mssql_odbc_pooling: bool = False

@dataclass
class RemoteDBParams:
    host: Optional[str] = None
    port: Optional[int] = 1433
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    auth: Literal["sql"] = "sql"

class BaseDBManager(ABC):
    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def dispose(self) -> None: ...

def _listen_sqlite_pragmas(engine: Engine, pragmas: Dict[str, Any]) -> None:
    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn, rec):
        c = dbapi_conn.cursor()
        for k, v in pragmas.items():
            c.execute(f"PRAGMA {k}={v}")
        c.close()

def _listen_mssql_session(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn, rec):
        cur = dbapi_conn.cursor()
        cur.execute("SET NOCOUNT ON")
        cur.execute("SET XACT_ABORT ON")
        cur.execute("SET LOCK_TIMEOUT 30000")
        cur.execute("SET ARITHABORT ON")
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cur.close()

class AsyncSQLiteManager(BaseDBManager):
    def __init__(self, db_file: Path, base, config: DBConfig):
        self.db_file = db_file
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        self.base = base
        self.config = config
        self.engine_write: AsyncEngine = create_async_engine(
            f"sqlite+aiosqlite:///{db_file}",
            echo=config.echo,
            future=True,
            connect_args={"timeout": 60, "isolation_level": config.sqlite_isolation},
            pool_pre_ping=True,
            pool_use_lifo=config.pool_use_lifo,
        )
        self.engine_read: AsyncEngine = create_async_engine(
            f"sqlite+aiosqlite:///{db_file}",
            echo=config.echo,
            future=True,
            connect_args={"timeout": 60, "isolation_level": "DEFERRED"},
            pool_pre_ping=True,
            pool_use_lifo=config.pool_use_lifo,
        )
        _listen_sqlite_pragmas(self.engine_write.sync_engine, {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "temp_store": "MEMORY",
            "cache_size": self.config.cache_size,
            "mmap_size": self.config.mmap_size,
            "wal_autocheckpoint": 100000,
            "foreign_keys": "ON",
            "journal_size_limit": 1073741824,
            "automatic_index": "ON",
            "busy_timeout": 15000,
            "cache_spill": "OFF",
            "locking_mode": "NORMAL",
            "query_only": 0,
            "threads": self.config.sqlite_threads,
        })
        _listen_sqlite_pragmas(self.engine_read.sync_engine, {
            "synchronous": "NORMAL",
            "temp_store": "MEMORY",
            "cache_size": self.config.cache_size,
            "mmap_size": self.config.mmap_size,
            "wal_autocheckpoint": 100000,
            "foreign_keys": "ON",
            "journal_size_limit": 1073741824,
            "automatic_index": "ON",
            "busy_timeout": 15000,
            "cache_spill": "OFF",
            "locking_mode": "NORMAL",
            "query_only": 1,
            "read_uncommitted": 1,
            "threads": self.config.sqlite_threads,
        })
        self.session_factory_write = async_sessionmaker(self.engine_write, expire_on_commit=False)
        self.session_factory_read = async_sessionmaker(self.engine_read, expire_on_commit=False)
        self.sync_engine_write = create_engine(
            f"sqlite:///{db_file}",
            echo=config.echo,
            future=True,
            connect_args={"check_same_thread": False, "timeout": 60, "isolation_level": config.sqlite_isolation},
            pool_pre_ping=True,
            pool_use_lifo=config.pool_use_lifo,
        )
        self.sync_engine_read = create_engine(
            f"sqlite:///{db_file}",
            echo=config.echo,
            future=True,
            connect_args={"check_same_thread": False, "timeout": 60, "isolation_level": "DEFERRED"},
            pool_pre_ping=True,
            pool_use_lifo=config.pool_use_lifo,
        )
        _listen_sqlite_pragmas(self.sync_engine_write, {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "temp_store": "MEMORY",
            "cache_size": self.config.cache_size,
            "mmap_size": self.config.mmap_size,
            "wal_autocheckpoint": 100000,
            "foreign_keys": "ON",
            "journal_size_limit": 1073741824,
            "automatic_index": "ON",
            "busy_timeout": 15000,
            "cache_spill": "OFF",
            "locking_mode": "NORMAL",
            "query_only": 0,
            "threads": self.config.sqlite_threads,
        })
        _listen_sqlite_pragmas(self.sync_engine_read, {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "temp_store": "MEMORY",
            "cache_size": self.config.cache_size,
            "mmap_size": self.config.mmap_size,
            "wal_autocheckpoint": 100000,
            "foreign_keys": "ON",
            "journal_size_limit": 1073741824,
            "automatic_index": "ON",
            "busy_timeout": 15000,
            "cache_spill": "OFF",
            "locking_mode": "NORMAL",
            "query_only": 1,
            "read_uncommitted": 1,
            "threads": self.config.sqlite_threads,
        })
        workers = min(32, max(4, (os.cpu_count() or 4) * 4))
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sqlite")

    @property
    def engine(self) -> AsyncEngine:
        return self.engine_write

    @property
    def sync_engine(self) -> Engine:
        return self.sync_engine_write

    def _run(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(self._executor, functools.partial(fn, *args, **kwargs))

    async def connect(self) -> None:
        is_new = not self.db_file.exists() or self.db_file.stat().st_size == 0
        async with self.engine_write.begin() as conn:
            if is_new:
                await conn.execute(text(f"PRAGMA page_size={self.config.sqlite_page_size}"))
            await conn.execute(text("PRAGMA optimize"))
            await conn.run_sync(self.base.metadata.create_all)
        async with self.engine_read.begin() as conn:
            pass
        def _apply():
            size = self.db_file.stat().st_size if self.db_file.exists() else 0
            mmap = min(self.config.mmap_size, max(self.config.sqlite_page_size, size + 536870912))
            with self.sync_engine_write.begin() as conn:
                conn.execute(text(f"PRAGMA mmap_size={mmap}"))
                conn.execute(text("PRAGMA optimize"))
            with self.sync_engine_read.begin() as conn:
                conn.execute(text(f"PRAGMA mmap_size={mmap}"))
        await self._run(_apply)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.session_factory_write() as s:
            try:
                yield s
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    @asynccontextmanager
    async def session_read(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.session_factory_read() as s:
            try:
                yield s
            finally:
                ...

    async def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        def _exec():
            with self.sync_engine_write.begin() as conn:
                conn.execute(text(sql), params or {})
        await self._run(_exec)

    async def read_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        def _read():
            with self.sync_engine_read.connect() as conn:
                return pd.read_sql(text(query), conn, params=params, dtype_backend=self.config.dtype_backend)
        df = await self._run(_read)
        return df.convert_dtypes()

    async def read_df_chunks(self, query: str, params: Optional[Dict[str, Any]] = None, chunksize: int = 100000) -> Iterable[pd.DataFrame]:
        def _gen():
            with self.sync_engine_read.connect() as conn:
                it = pd.read_sql(text(query), conn, params=params, dtype_backend=self.config.dtype_backend, chunksize=chunksize)
                for chunk in it:
                    yield chunk.convert_dtypes()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: _gen())

    async def to_sql(self, table: str, df: pd.DataFrame, if_exists: Literal["fail", "replace", "append"] = "append", index: bool = False, chunksize: int = 100000, method: ToSqlMethod = "multi") -> None:
        if df is None or df.empty:
            return
        def _write():
            with self.sync_engine_write.begin() as conn:
                conn.execute(text(f"PRAGMA synchronous={self.config.sqlite_bulk_synchronous}"))
                conn.execute(text("PRAGMA temp_store=MEMORY"))
                conn.execute(text(f"PRAGMA locking_mode={self.config.sqlite_bulk_locking}"))
                df.to_sql(table, conn, if_exists=if_exists, index=index, chunksize=chunksize, method=method)
        await self._run(_write)

    async def dispose(self) -> None:
        async with self.engine_write.begin() as conn:
            await conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
        await self.engine_write.dispose()
        await self.engine_read.dispose()
        self.sync_engine_write.dispose()
        self.sync_engine_read.dispose()
        self._executor.shutdown(wait=True)

class MSSQLManager(BaseDBManager):
    def __init__(self, config: DBConfig, remote: RemoteDBParams):
        self.config = config
        self.remote = remote
        self._engine: Optional[Engine] = None
        self.session_factory: Optional[sessionmaker] = None
        self._executor: Optional[ThreadPoolExecutor] = None

    @property
    def engine(self) -> Engine:
        if not self._engine:
            raise RuntimeError("MSSQL engine not initialized")
        return self._engine

    def _run(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(self._executor, functools.partial(fn, *args, **kwargs))

    def _build_engine_pyodbc(self) -> Optional[Engine]:
        if importlib.util.find_spec("pyodbc") is None:
            return None
        import pyodbc
        pyodbc.pooling = self.config.mssql_odbc_pooling
        driver = "ODBC Driver 18 for SQL Server"
        server = (self.remote.host or "").strip()
        port = self.remote.port or 1433
        database = (self.remote.database or "master").strip()
        user = (self.remote.user or "").strip()
        pwd = (self.remote.password or "").strip()
        mars = "Yes" if self.config.mssql_mars else "No"
        encrypt = "Yes" if self.config.mssql_encrypt else "No"
        tsc = "Yes" if self.config.mssql_trust_server_certificate else "No"
        msf = "Yes" if self.config.mssql_multi_subnet_failover else "No"
        app_intent = self.config.mssql_application_intent
        timeout = self.config.mssql_connect_timeout
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};PWD={pwd};"
            f"MARS Connection={mars};"
            f"TrustServerCertificate={tsc};"
            f"Encrypt={encrypt};"
            f"MultiSubnetFailover={msf};"
            f"Application Intent={app_intent};"
            f"Connect Timeout={timeout};"
        )
        u = quote_plus(conn_str)
        return create_engine(
            f"mssql+pyodbc:///?odbc_connect={u}",
            echo=self.config.echo,
            future=True,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            pool_pre_ping=True,
            pool_reset_on_return="rollback",
            pool_use_lifo=self.config.pool_use_lifo,
            fast_executemany=self.config.mssql_fast_executemany,
        )

    def _build_engine_pytds_sql(self) -> Engine:
        import pytds
        def creator():
            return pytds.connect(
                self.remote.host or "",
                self.remote.database or "master",
                self.remote.user or "",
                self.remote.password or "",
                port=self.remote.port or 1433,
                use_mars=self.config.mssql_mars,
            )
        return create_engine(
            "mssql+pytds://",
            echo=self.config.echo,
            future=True,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            pool_pre_ping=True,
            pool_reset_on_return="rollback",
            pool_use_lifo=self.config.pool_use_lifo,
            creator=creator,
        )

    def _build_engine(self) -> Engine:
        eng = self._build_engine_pyodbc()
        if eng is not None:
            return eng
        return self._build_engine_pytds_sql()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def connect(self) -> None:
        engine = self._build_engine()
        _listen_mssql_session(engine)
        def _test():
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        await asyncio.get_running_loop().run_in_executor(None, _test)
        self._engine = engine
        self.session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        workers = min(128, max(16, (os.cpu_count() or 4) * 4, self.config.pool_size + self.config.max_overflow + 16))
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mssql")

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[Session, None]:
        if not self.session_factory:
            raise RuntimeError("MSSQL session factory not initialized")
        s = self.session_factory()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    async def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        def _exec():
            with self.engine.begin() as conn:
                conn.execute(text(sql), params or {})
        await self._run(_exec)

    async def fetch_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        def _fetch():
            with self.engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params, dtype_backend=self.config.dtype_backend)
        df = await self._run(_fetch)
        return df.convert_dtypes()

    async def fetch_df_stream(self, query: str, params: Optional[Dict[str, Any]] = None, chunksize: int = 200000) -> Iterable[pd.DataFrame]:
        def _gen():
            with self.engine.connect() as conn:
                it = pd.read_sql(text(query), conn, params=params, dtype_backend=self.config.dtype_backend, chunksize=chunksize)
                for chunk in it:
                    yield chunk.convert_dtypes()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: _gen())

    async def execute_many(self, statements: List[tuple[str, Optional[Dict[str, Any]]]], parallel: int = 8) -> None:
        sem = asyncio.Semaphore(parallel)
        async def _task(sql: str, p: Optional[Dict[str, Any]]):
            async with sem:
                await self.execute(sql, p)
        await asyncio.gather(*(_task(s, p) for s, p in statements))

    async def fetch_df_many(self, queries: List[tuple[str, Optional[Dict[str, Any]]]], parallel: int = 8) -> List[pd.DataFrame]:
        sem = asyncio.Semaphore(parallel)
        async def _task(q: str, p: Optional[Dict[str, Any]]):
            async with sem:
                return await self.fetch_df(q, p)
        return await asyncio.gather(*(_task(q, p) for q, p in queries))

    async def dispose(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

class DatabaseOrchestrator:
    def __init__(self, base_dir: Path, remote: RemoteDBParams, config: DBConfig = DBConfig()):
        self.config = config
        self._hot = AsyncSQLiteManager(base_dir / "storage_data/hot_store.db", HotBase, config)
        self._cold = AsyncSQLiteManager(base_dir / "storage_data/cold_store.db", ColdBase, config)
        self._mssql = MSSQLManager(config, remote)
        self.managers: List[BaseDBManager] = [self._hot, self._cold, self._mssql]
        self._disposed = False
        self._drainer = None

    @property
    def hot(self) -> AsyncSQLiteManager:
        return self._hot

    @property
    def cold(self) -> AsyncSQLiteManager:
        return self._cold

    @property
    def mssql(self) -> MSSQLManager:
        return self._mssql

    async def initialize(self):
        await asyncio.gather(*(m.connect() for m in self.managers))
        from b_infrastructure.repositories.outbox import OutboxRepo
        await OutboxRepo(self._hot.engine).initialize()

    async def start_outbox_drainer(self, batch: int = 2000, interval: float = 0.2):
        if self._drainer:
            return
        from b_infrastructure.repositories.outbox import OutboxDrainer
        d = OutboxDrainer(self, batch=batch, interval=interval)
        await d.start()
        self._drainer = d

    async def stop_outbox_drainer(self):
        if self._drainer:
            await self._drainer.stop()
            self._drainer = None

    async def dispose_all(self):
        if self._disposed:
            return
        self._disposed = True
        await self.stop_outbox_drainer()
        await asyncio.gather(*(m.dispose() for m in self.managers))

    async def healthcheck(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        try:
            async with self.hot.engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            results["SQLiteHot"] = True
        except Exception:
            results["SQLiteHot"] = False
        try:
            async with self.cold.engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            results["SQLiteCold"] = True
        except Exception:
            results["SQLiteCold"] = False
        try:
            eng = self._mssql._engine
            if eng is not None:
                def _ping(engine: Engine) -> None:
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                await asyncio.get_running_loop().run_in_executor(None, _ping, eng)
                results["MSSQL"] = True
            else:
                results["MSSQL"] = False
        except Exception:
            results["MSSQL"] = False
        return results