from __future__ import annotations
import configparser
import asyncio
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Lock, Timer
from typing import Callable, List, Optional
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class Theme(str, Enum):
    DARK = "dark"
    LIGHT = "light"

@dataclass(frozen=True, slots=True)
class AppConfig:
    profile: str
    refresh_fast_ms: int
    refresh_slow_ms: int
    theme: Theme
    layout_path: str
    base_dir: Path
    mssql_host: Optional[str]
    mssql_port: int
    mssql_db: Optional[str]
    mssql_user: Optional[str]
    mssql_password: Optional[str]
    mssql_auth: str

class _ConfigReloader(FileSystemEventHandler):
    def __init__(self, cfg_file: Path, debounce_sec: float = 0.3):
        super().__init__()
        self.cfg_file = cfg_file
        self._config: Optional[AppConfig] = None
        self._lock = Lock()
        self._observer = Observer()
        self._observer.schedule(self, str(cfg_file.parent), recursive=False)
        self._observer.start()
        self._debounce_timer: Optional[Timer] = None
        self._debounce_sec = debounce_sec
        self._callbacks: List[Callable[[AppConfig], None]] = []
        self._load()

    def on_modified(self, event):
        self._maybe_reload(event)

    def on_created(self, event):
        self._maybe_reload(event)

    def on_moved(self, event):
        self._maybe_reload(event)

    def _maybe_reload(self, event):
        try:
            p = Path(str(getattr(event, "src_path", "")))
        except Exception:
            return
        if p == self.cfg_file:
            if self._debounce_timer and self._debounce_timer.is_alive():
                self._debounce_timer.cancel()
            self._debounce_timer = Timer(self._debounce_sec, self._load)
            self._debounce_timer.start()

    def _load(self):
        parser = configparser.ConfigParser(interpolation=None)
        if self.cfg_file.exists():
            parser.read(self.cfg_file, encoding="utf-8")
        else:
            parser["app"] = {
                "profile": "Equipment Realtime Visualization",
                "refresh_fast_ms": "100",
                "refresh_slow_ms": "1000",
            }
            parser["ui"] = {"theme": "dark", "layout_path": "e_ui/assets/layout/layout.json"}
            parser["db"] = {
                "mssql_host": "",
                "mssql_port": "1433",
                "mssql_db": "",
                "mssql_user": "",
                "mssql_password": "",
                "mssql_auth": "sql",
            }
            self.cfg_file.parent.mkdir(parents=True, exist_ok=True)
            with self.cfg_file.open("w", encoding="utf-8") as f:
                parser.write(f)

        def get_or(section: str, option: str, fallback: str) -> str:
            try:
                return parser.get(section, option, fallback=fallback).strip()
            except Exception:
                return fallback

        base = Path.cwd()
        profile = get_or("app", "profile", "Equipment Realtime Visualization")
        refresh_fast_ms = int(get_or("app", "refresh_fast_ms", "100"))
        refresh_slow_ms = int(get_or("app", "refresh_slow_ms", "1000"))
        theme_raw = get_or("ui", "theme", "dark").lower()
        layout_path = get_or("ui", "layout_path", "e_ui/assets/layout/layout.json")
        theme = Theme.DARK if theme_raw == "dark" else Theme.LIGHT

        mssql_host = os.getenv("MSSQL_HOST") or get_or("db", "mssql_host", "") or None
        mssql_port_raw = os.getenv("MSSQL_PORT") or get_or("db", "mssql_port", "1433")
        mssql_port = int(mssql_port_raw) if mssql_port_raw else 1433
        mssql_db = os.getenv("MSSQL_DB") or get_or("db", "mssql_db", "") or None
        mssql_user = os.getenv("MSSQL_USER") or get_or("db", "mssql_user", "") or None
        mssql_password = os.getenv("MSSQL_PASSWORD") or get_or("db", "mssql_password", "") or None
        mssql_auth = (os.getenv("MSSQL_AUTH") or get_or("db", "mssql_auth", "sql")).lower()

        cfg = AppConfig(
            profile=profile,
            refresh_fast_ms=refresh_fast_ms,
            refresh_slow_ms=refresh_slow_ms,
            theme=theme,
            layout_path=layout_path,
            base_dir=base,
            mssql_host=mssql_host.strip() if mssql_host else None,
            mssql_port=mssql_port,
            mssql_db=mssql_db.strip() if mssql_db else None,
            mssql_user=mssql_user.strip() if mssql_user else None,
            mssql_password=mssql_password.strip() if mssql_password else None,
            mssql_auth=mssql_auth.strip(),
        )
        with self._lock:
            self._config = cfg
            callbacks = list(self._callbacks)
        for fn in callbacks:
            try:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop and loop.is_running():
                    loop.call_soon_threadsafe(fn, cfg)
                else:
                    fn(cfg)
            except Exception:
                pass

    def get_config(self) -> AppConfig:
        with self._lock:
            assert self._config is not None
            return self._config

    def register_callback(self, func: Callable[[AppConfig], None]) -> None:
        with self._lock:
            self._callbacks.append(func)

    def shutdown(self) -> None:
        try:
            self._observer.stop()
            self._observer.join(timeout=1.0)
        except Exception:
            pass

_cfg_reloader: Optional[_ConfigReloader] = None

def init_config_system() -> None:
    global _cfg_reloader
    if _cfg_reloader is None:
        base = Path.cwd()
        cfg_file = base / "a_core/configs/app.ini"
        cfg_file.parent.mkdir(parents=True, exist_ok=True)
        _cfg_reloader = _ConfigReloader(cfg_file)

def get_config() -> AppConfig:
    if _cfg_reloader is None:
        init_config_system()
    assert _cfg_reloader is not None
    return _cfg_reloader.get_config()

def register_config_callback(func: Callable[[AppConfig], None]) -> None:
    if _cfg_reloader is None:
        init_config_system()
    assert _cfg_reloader is not None
    _cfg_reloader.register_callback(func)

def shutdown_config_system() -> None:
    global _cfg_reloader
    if _cfg_reloader is not None:
        _cfg_reloader.shutdown()
        _cfg_reloader = None