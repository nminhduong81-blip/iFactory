from __future__ import annotations
import sys, asyncio, logging
from typing import cast
from PySide6.QtWidgets import QApplication, QMessageBox
from qasync import QEventLoop
from a_core.configs.config import init_config_system, get_config, register_config_callback, AppConfig, shutdown_config_system
from e_ui.main_window import MainWindow
from e_ui.theme import apply_theme
from di import build_container

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def setup_app(app: QApplication) -> tuple[MainWindow, object]:
    try:
        init_config_system()
        cfg: AppConfig = get_config()
    except Exception as ex:
        QMessageBox.critical(None, "Startup Error", f"Config error:\n{ex}")
        sys.exit(1)
    register_config_callback(lambda new_cfg: apply_theme(app, new_cfg.theme.value))
    full_service, dbm = build_container()
    try:
        await dbm.initialize()
    except Exception as ex:
        QMessageBox.critical(None, "DB Init Error", f"Database init failed:\n{ex}")
        sys.exit(1)
    await full_service.initialize()
    apply_theme(app, cfg.theme.value)
    window = MainWindow(cfg, full_service)
    window.show()
    return window, dbm

def main():
    app = QApplication.instance() or QApplication(sys.argv)
    app = cast(QApplication, app)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    async def _shutdown(dbm):
        try:
            tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            try:
                await dbm.dispose_all()
            except Exception:
                pass
            try:
                shutdown_config_system()
            except Exception:
                pass
            loop.stop()
    with loop:
        try:
            window, db = loop.run_until_complete(setup_app(app))
            app.aboutToQuit.connect(lambda: asyncio.ensure_future(_shutdown(db)))
            loop.create_task(window.full_service.full_sync())
            loop.run_forever()
            sys.exit(0)
        except Exception as e:
            logger.exception("Unhandled error: %s", e)
            sys.exit(1)

if __name__ == "__main__":
    main()