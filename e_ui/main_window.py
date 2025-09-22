from __future__ import annotations
import getpass, os
from datetime import datetime, timedelta, timezone
import json
import asyncio
import time
from pathlib import Path
from dataclasses import replace, dataclass
from typing import cast, List, Dict, Any, Optional
from PySide6.QtCore import Qt, QTimer, QSettings
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import QApplication, QMainWindow, QTabWidget, QToolButton, QMenu, QWidget, QHBoxLayout, QLineEdit, QListWidget, QListWidgetItem, QVBoxLayout, QDialog, QLabel, QStatusBar, QStyleFactory, QWidgetAction, QComboBox, QSplitter
from qasync import asyncSlot
from a_core.configs.config import AppConfig, Theme
from d_application.services.full_service import FullLoaderService
from d_application.services.load_controller import LoadController
from e_ui.layout_view import LayoutView
from e_ui.dialogs import BaseTableDialog, StatusDialog, WipDialog, EipDialog, InputDialog
from e_ui.gantt import GanttStrip, StatusSummaryBar
from e_ui.theme import apply_theme, THEMES, apply_mica

def load_layout_codes(layout_path: str) -> list[str]:
    p = Path(layout_path)
    data = json.loads(p.read_text(encoding="utf-8"))
    codes: list[str] = []
    for floor in (data.get("floors") or {}).values():
        items = floor.get("items", []) or floor.get("devices", [])
        for item in items:
            code = str(item.get("id") or item.get("equip_code") or item.get("code") or "")
            if code:
                codes.append(code)
    return sorted(list(dict.fromkeys(codes)))

def _hex_to_rgba_str(s: str, alpha: float) -> str:
    s = s.strip()
    if s.startswith("#"):
        s = s[1:]
    if len(s) == 8:
        s = s[2:]
    if len(s) != 6:
        return "rgba(0,0,0,0.0)"
    r = int(s[0:2], 16)
    g = int(s[2:4], 16)
    b = int(s[4:6], 16)
    a = max(0.0, min(1.0, alpha))
    return f"rgba({r},{g},{b},{a:.3f})"

class CommandPalette(QDialog):
    def __init__(self, parent: QMainWindow, codes: list[str], handler) -> None:
        super().__init__(parent)
        self.setWindowFlags(Qt.WindowType.Dialog | Qt.WindowType.FramelessWindowHint)
        self.setModal(True)
        self.resize(440, 420)
        self.handler = handler
        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 8, 8, 8)
        self.search = QLineEdit(self)
        self.search.setPlaceholderText("Type equipment code or action… (e.g. 'status ABC123')")
        self.list_widget = QListWidget(self)
        self.list_widget.setAlternatingRowColors(True)
        for code in codes:
            self._add_item("status", "📊 Status", code)
            self._add_item("wip", "📦 WIP", code)
            self._add_item("eip", "🌐 EIP", code)
            self._add_item("input", "📥 Input", code)
        lay.addWidget(self.search)
        lay.addWidget(self.list_widget)
        self.search.textChanged.connect(self._filter)
        self.search.returnPressed.connect(self._activate_current)
        self.list_widget.itemActivated.connect(self._select)

    def _add_item(self, act: str, label: str, code: str):
        it = QListWidgetItem(f"{label} {code}", self.list_widget)
        it.setData(Qt.UserRole, (act, code))

    def showEvent(self, e):
        super().showEvent(e)
        self.search.setFocus()
        self._move_to_first()

    def keyPressEvent(self, e):
        if e.key() == Qt.Key_Escape:
            self.reject()
        else:
            super().keyPressEvent(e)

    def _filter(self, text: str):
        t = text.lower().strip()
        first_visible_row = -1
        for i in range(self.list_widget.count()):
            it = self.list_widget.item(i)
            is_vis = (t in it.text().lower()) if t else True
            it.setHidden(not is_vis)
            if is_vis and first_visible_row < 0:
                first_visible_row = i
        if first_visible_row >= 0:
            self.list_widget.setCurrentRow(first_visible_row)

    def _move_to_first(self):
        for i in range(self.list_widget.count()):
            it = self.list_widget.item(i)
            if not it.isHidden():
                self.list_widget.setCurrentRow(i)
                return

    def _activate_current(self):
        it = self.list_widget.currentItem()
        if it and not it.isHidden():
            self._select(it)

    def _select(self, item: QListWidgetItem):
        if item:
            data = item.data(Qt.UserRole) or (None, None)
            act, code = data
            if act and code:
                self.handler(act, code)
                self.accept()

class MiniToast(QLabel):
    def __init__(self, parent: QWidget, text: str, theme: str = "light"):
        super().__init__(parent)
        colors = THEMES.get(theme.lower(), THEMES["light"])
        accent = colors.get("primary", "#0F6CBD")
        bg = _hex_to_rgba_str(accent, 0.90) if theme.lower() == "light" else _hex_to_rgba_str(accent, 0.82)
        self.setText(text)
        self.setStyleSheet(f"background:{bg};color:white;padding:8px 12px;border-radius:10px;")
        self.setWindowFlags(Qt.WindowType.ToolTip)
        self.adjustSize()
        geo = parent.geometry()
        self.move(geo.center().x() - self.width() // 2, geo.bottom() - int(geo.height() * 0.12))
        self.show()
        QTimer.singleShot(2400, self.close)

DAYS_MAP = {
    "1d": 1,
    "1w": 7,
    "1m": 30,
    "3m": 90,
    "6m": 180,
}

@dataclass
class HistoryEntry:
    kind: str
    code: str
    dlg: BaseTableDialog
    task: Optional[asyncio.Task] = None
    token: int = 0

def _current_user() -> str:
    try:
        return getpass.getuser()
    except Exception:
        return os.environ.get("USERNAME") or os.environ.get("USER") or "Unknown"

class MainWindow(QMainWindow):
    SETTINGS_ORG = "MyCompany"
    SETTINGS_APP = "EquipMonitor"

    def __init__(self, cfg: AppConfig, full_service: FullLoaderService):
        super().__init__()
        self._settings = QSettings(self.SETTINGS_ORG, self.SETTINGS_APP)
        self.cfg = cfg
        self.full_service = full_service
        self.status_service = full_service.status_service
        self.input_service = full_service.input_service
        self.codes = load_layout_codes(cfg.layout_path)
        self.current_period_key = "1d"
        self._load_state()
        self.loader = LoadController(full_service, cfg.refresh_fast_ms or 30000, self.codes, quick_mode=True)
        self.loader.first_batch_ready.connect(self._handle_initial_data)
        self.loader.progress.connect(self._on_progress)
        user = _current_user()
        app = cfg.profile
        self.setWindowTitle(f"{app} | User: {user} | Design by IE")
        self.resize(1366, 860)
        self.tabs = QTabWidget(self)
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self._gantt_tasks: dict[str, Optional[asyncio.Task]] = {"Electrode": None, "Assembly": None}
        self.setCentralWidget(self.tabs)
        self._selected_code: dict[str, Optional[str]] = {"Electrode": None, "Assembly": None}
        self._last_gantt_ts: dict[str, float] = {"Electrode": 0.0, "Assembly": 0.0}
        self._gantt_interval_sec: float = 1.0
        self._gantt_sync_ts: dict[str, float] = {"Electrode": 0.0, "Assembly": 0.0}
        self._gantt_sync_interval_sec: float = 1.0
        self._refresh_lock = asyncio.Lock()
        self._suppress_autofit_until = 0.0
        self._view_refresh_ms = getattr(cfg, "view_refresh_ms", 1500)
        self.settings_btn = QToolButton(self)
        self.settings_btn.setObjectName("settingsBtn")
        self.settings_btn.setText("⚙️")
        self.settings_btn.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.settings_btn.setFixedHeight(26)
        self.settings_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.settings_btn.setMenu(self._build_settings_menu())
        cw = QWidget(self)
        cwl = QHBoxLayout(cw)
        cwl.setContentsMargins(0, 0, 0, 0)
        cwl.addWidget(self.settings_btn)
        self.tabs.setCornerWidget(cw, Qt.Corner.TopRightCorner)
        self._init_tabs()
        self.refresh_timer = QTimer(self)
        self.refresh_timer.setInterval(self._view_refresh_ms)
        self.refresh_timer.timeout.connect(self._refresh_device_views)
        self.refresh_timer.start()
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        palette_act = QAction("Command Palette", self)
        palette_act.setShortcut(QKeySequence("Ctrl+P"))
        palette_act.triggered.connect(self._open_palette)
        self.addAction(palette_act)
        self._prog_status = 0
        self._prog_input = 0
        self._history_entries: list[HistoryEntry] = []
        self._restore_geometry()
        self._disable_text_shadows()
        QTimer.singleShot(0, self._fit_all_views)
        QTimer.singleShot(50, self._fit_all_views)

    def _on_progress(self, msg: str):
        self.status.showMessage(msg, 2000)

    def _progress_cb(self, src: str, n: int):
        if src == "status":
            self._prog_status = n
        elif src == "input":
            self._prog_input = n
        self.status.showMessage(f"⏳ Status={self._prog_status}, Input={self._prog_input}", 1000)

    def _handle_initial_data(self, data: list):
        if not data:
            return
        statuses = data[0].get("status", [])
        inputs = data[1].get("input", [])
        if hasattr(self, "view_electrode"):
            self.view_electrode.apply_hot_data(statuses)
            self.view_electrode.apply_input_data(inputs)
        if hasattr(self, "view_assembly"):
            self.view_assembly.apply_hot_data(statuses)
            self.view_assembly.apply_input_data(inputs)
        self.status.showMessage(f"Quick sync: {len(statuses)} status, {len(inputs)} input", 5000)

    def _maybe_refresh_gantt_current_tab(self):
        idx = self.tabs.currentIndex()
        tab = "Electrode" if idx == 0 else "Assembly"
        code = self._selected_code.get(tab)
        if not code:
            return
        now = time.monotonic()
        if now - self._last_gantt_ts[tab] < self._gantt_interval_sec:
            return
        self._last_gantt_ts[tab] = now
        self._update_gantt(tab, code)

    @asyncSlot()
    async def _refresh_device_views(self):
        if not self.isVisible() or self.isMinimized():
            return
        if self._refresh_lock.locked():
            return
        async with self._refresh_lock:
            try:
                idx = self.tabs.currentIndex()
                if hasattr(self, "view_electrode") and idx == 0:
                    await self.view_electrode.refresh_devices()
                elif hasattr(self, "view_assembly") and idx == 1:
                    await self.view_assembly.refresh_devices()
                if getattr(self, "_refresh_toggle", False):
                    other = self.view_assembly if idx == 0 else self.view_electrode
                    if other:
                        await other.refresh_devices()
                self._refresh_toggle = not getattr(self, "_refresh_toggle", False)
                self._maybe_refresh_gantt_current_tab()
            except asyncio.CancelledError:
                return
            except Exception as ex:
                self.status.showMessage(f"Refresh error: {ex}", 2000)

    def showEvent(self, e):
        super().showEvent(e)
        self._fit_all_views()
        QTimer.singleShot(0, lambda: apply_mica(self, dark=self.cfg.theme.value.lower() == "dark", kind="tabbed"))
        QTimer.singleShot(200, self.loader.start)

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._fit_current_view(throttled=True)

    def _on_tab_changed(self, _idx: int):
        self._fit_current_view(throttled=True)
        self._suppress_autofit(1.0)

    def _make_tab(self, floor_name: str) -> QWidget:
        container = QWidget(self)
        v = QVBoxLayout(container)
        v.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Vertical, container)
        splitter.setChildrenCollapsible(False)
        lv = LayoutView(self.cfg.layout_path, floor_name, self.status_service, self.cfg.theme.value)
        bottom = QWidget(container)
        bl = QVBoxLayout(bottom)
        bl.setContentsMargins(0, 0, 0, 0)
        gantt = GanttStrip(bottom)
        summary = StatusSummaryBar(bottom)
        H = 40
        summary.setFixedHeight(H)
        gantt.setFixedHeight(H)
        bl.addWidget(summary)
        bl.addWidget(gantt)
        splitter.addWidget(lv)
        splitter.addWidget(bottom)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)
        v.addWidget(splitter)
        QTimer.singleShot(0, lambda: splitter.setSizes([int(self.height() * 1), max(gantt.minimumHeight() + summary.height(), int(self.height() * 0.05))]))
        if floor_name == "Electrode":
            self.view_electrode = lv
            self.gantt_electrode = gantt
            self.summary_electrode = summary
            lv.deviceSelected.connect(lambda code: self._on_device_selected("Electrode", code))
            lv.requestStatus.connect(lambda code: self._on_device_selected("Electrode", code))
            lv.requestWip.connect(lambda code: self._on_device_selected("Electrode", code))
            lv.requestEip.connect(lambda code: self._on_device_selected("Electrode", code))
            lv.requestInput.connect(lambda code: self._on_device_selected("Electrode", code))
        else:
            self.view_assembly = lv
            self.gantt_assembly = gantt
            self.summary_assembly = summary
            lv.deviceSelected.connect(lambda code: self._on_device_selected("Assembly", code))
            lv.requestStatus.connect(lambda code: self._on_device_selected("Assembly", code))
            lv.requestWip.connect(lambda code: self._on_device_selected("Assembly", code))
            lv.requestEip.connect(lambda code: self._on_device_selected("Assembly", code))
            lv.requestInput.connect(lambda code: self._on_device_selected("Assembly", code))
        default_code = next(iter(lv.devices.keys()), None)
        if default_code:
            self._selected_code[floor_name] = default_code
            QTimer.singleShot(0, lambda: self._update_gantt(floor_name, default_code))
        else:
            gantt.set_placeholder()
        return container

    def _init_tabs(self):
        self.tabs.blockSignals(True)
        c1 = self._make_tab("Electrode")
        c2 = self._make_tab("Assembly")
        self.tabs.addTab(c1, "Electrode")
        self.tabs.addTab(c2, "Assembly")
        self.view_electrode.syncScaleRequested.connect(self.view_assembly.apply_scale)
        self.view_assembly.syncScaleRequested.connect(self.view_electrode.apply_scale)
        self.view_electrode.syncScaleRequested.connect(lambda _: self._suppress_autofit(1.5))
        self.view_assembly.syncScaleRequested.connect(lambda _: self._suppress_autofit(1.5))
        self._wire_layout(self.view_electrode)
        self._wire_layout(self.view_assembly)
        self.tabs.setCurrentIndex(0)
        self.tabs.blockSignals(False)

    def _on_device_selected(self, tab: str, code: str):
        self._selected_code[tab] = code
        self._update_gantt(tab, code)

    def _today_range(self) -> tuple[datetime, datetime]:
        now = datetime.utcnow()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now

    def _parse_dt(self, v):
        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                try:
                    dt = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    return None
        else:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def _build_segments(self, rows, start: datetime, clip_end: datetime, fallback_status: Optional[str] = None, fallback_time: Optional[datetime] = None):
        events = []
        for r in rows:
            dt = self._parse_dt(r.get("event_time"))
            if dt is not None:
                events.append((dt, r.get("equip_status")))
        events.sort(key=lambda x: x[0])
        initial_status = None
        for t, s in events:
            if t < start:
                initial_status = s
            else:
                break
        if initial_status is None:
            initial_status = fallback_status
        segs = []
        cur_t = start
        cur_status = initial_status
        for t, s in events:
            if t < start:
                continue
            if t > clip_end:
                break
            if cur_t < t:
                segs.append((cur_t, t, cur_status))
            cur_status = s
            cur_t = t
        if cur_t < clip_end:
            segs.append((cur_t, clip_end, cur_status))
        if not segs and fallback_status:
            start_seg = max(start, fallback_time) if isinstance(fallback_time, datetime) else start
            start_seg = min(start_seg, clip_end)
            if start_seg < clip_end:
                segs.append((start_seg, clip_end, fallback_status))
        return segs

    def _get_tab_widgets(self, tab: str) -> tuple[LayoutView, GanttStrip, StatusSummaryBar]:
        if tab == "Electrode":
            return self.view_electrode, self.gantt_electrode, self.summary_electrode
        return self.view_assembly, self.gantt_assembly, self.summary_assembly

    def _cancel_gantt_task(self, tab: str):
        t = self._gantt_tasks.get(tab)
        if t and not t.done():
            t.cancel()
        self._gantt_tasks[tab] = None

    def _period_range_for_gantt(self) -> tuple[datetime, datetime]:
        now = datetime.utcnow()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return start, end

    def _update_gantt(self, tab: str, code: Optional[str]):
        lv, gw, sb = self._get_tab_widgets(tab)
        self._cancel_gantt_task(tab)
        if not code:
            gw.set_placeholder()
            return
        async def run():
            try:
                s, e_axis = self._period_range_for_gantt()
                e_clip = min(e_axis, datetime.utcnow())
                q_start = s - timedelta(days=1)
                q_end = e_clip
                rows = await self.status_service.query_period([code], q_start, q_end)
                dev = lv.devices.get(code)
                fb_status = getattr(dev, "current_status", None)
                fb_time = getattr(dev, "latest_time", None)
                segs = self._build_segments(rows, s, e_clip, fallback_status=fb_status, fallback_time=fb_time)
                gw.set_segments(code, segs, s, e_axis)
                sb.set_segments(segs, s, e_clip)
                now_ts = time.monotonic()
                if now_ts - self._gantt_sync_ts[tab] > self._gantt_sync_interval_sec:
                    self._gantt_sync_ts[tab] = now_ts
                    try:
                        await self.status_service.sync_period([code], s, e_clip, progress_cb=None, backfill=True)
                        new_rows = await self.status_service.query_period([code], q_start, q_end)
                        segs2 = self._build_segments(new_rows, s, e_clip, fallback_status=fb_status, fallback_time=fb_time)
                        if code == self._selected_code.get(tab):
                            gw.set_segments(code, segs2, s, e_axis)
                            sb.set_segments(segs2, s, e_clip)
                    except Exception:
                        pass
            except asyncio.CancelledError:
                return
            except Exception:
                gw.set_placeholder("No data")
        task = asyncio.create_task(run())
        self._gantt_tasks[tab] = task

    def _wire_layout(self, lv: LayoutView):
        lv.requestStatus.connect(self._open_status)
        lv.requestWip.connect(self._open_wip)
        lv.requestEip.connect(self._open_eip)
        lv.requestInput.connect(self._open_input)

    def _row_action(self, label: str, widget: QWidget) -> QWidgetAction:
        w = QWidget(self)
        h = QHBoxLayout(w)
        h.setContentsMargins(10, 6, 10, 6)
        h.addWidget(QLabel(label))
        h.addStretch(1)
        h.addWidget(widget)
        act = QWidgetAction(self)
        act.setDefaultWidget(w)
        return act

    def _build_settings_menu(self) -> QMenu:
        m = QMenu(self)
        m.setStyle(QStyleFactory.create("Fusion"))
        self.cmb_period = QComboBox(m)
        self.cmb_period.addItem("1 day", "1d")
        self.cmb_period.addItem("1 week", "1w")
        self.cmb_period.addItem("1 month", "1m")
        self.cmb_period.addItem("3 months", "3m")
        self.cmb_period.addItem("6 months", "6m")
        idx = max(0, self.cmb_period.findData(self.current_period_key))
        self.cmb_period.setCurrentIndex(idx)
        self.cmb_period.currentIndexChanged.connect(self._on_period_changed)
        self.cmb_theme = QComboBox(m)
        self.cmb_theme.addItem("Light", "light")
        self.cmb_theme.addItem("Dark", "dark")
        t_idx = 1 if self.cfg.theme == Theme.DARK else 0
        self.cmb_theme.setCurrentIndex(t_idx)
        self.cmb_theme.currentIndexChanged.connect(self._on_theme_changed)
        m.addAction(self._row_action("History", self.cmb_period))
        m.addSeparator()
        m.addAction(self._row_action("Theme", self.cmb_theme))
        return m

    def _on_period_changed(self):
        key = self.cmb_period.currentData()
        if key in DAYS_MAP and key != self.current_period_key:
            self.current_period_key = key
            self._save_state()
            MiniToast(self, f"History: {self.cmb_period.currentText()}", self.cfg.theme.value)
            for entry in list(self._history_entries):
                if isinstance(entry.dlg, BaseTableDialog):
                    entry.dlg.set_period_key(self.current_period_key)
                self._restart_history_refresh(entry)

    def _on_theme_changed(self):
        val = self.cmb_theme.currentData()
        if isinstance(val, str):
            new = Theme.DARK if val == "dark" else Theme.LIGHT
            if new != self.cfg.theme:
                self.cfg = replace(self.cfg, theme=new)
                app = cast(QApplication, QApplication.instance())
                if app:
                    apply_theme(app, self.cfg.theme.value)
                if hasattr(self, "view_electrode"):
                    self.view_electrode.set_theme(self.cfg.theme.value)
                if hasattr(self, "view_assembly"):
                    self.view_assembly.set_theme(self.cfg.theme.value)
                apply_mica(self, dark=self.cfg.theme.value.lower() == "dark", kind="tabbed")
                self._disable_text_shadows()
                self._fit_all_views()
                self._save_state()

    def _period_range(self) -> tuple[datetime, datetime]:
        now = datetime.utcnow()
        today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        key = self.current_period_key
        if key == "1d":
            start = today0
            end = now
        else:
            days = DAYS_MAP.get(key, 7)
            start = (today0 - timedelta(days=days))
            end = now
        return start, end

    def _register_history_dialog(self, kind: str, code: str, dlg: BaseTableDialog) -> HistoryEntry:
        entry = HistoryEntry(kind=kind, code=code, dlg=dlg)
        self._history_entries.append(entry)
        def _on_destroyed(_obj=None, e=entry):
            self._unregister_history_dialog(e)
        dlg.destroyed.connect(_on_destroyed)
        return entry

    def _unregister_history_dialog(self, entry: HistoryEntry):
        if entry.task and not entry.task.done():
            entry.task.cancel()
        try:
            self._history_entries.remove(entry)
        except ValueError:
            pass

    def _restart_history_refresh(self, entry: HistoryEntry):
        if not isinstance(entry.dlg, BaseTableDialog):
            return
        if not entry.dlg.isVisible():
            return
        if entry.task and not entry.task.done():
            entry.task.cancel()
        entry.token += 1
        tok = entry.token
        entry.task = asyncio.create_task(self._refresh_history(entry, tok))

    async def _refresh_history(self, entry: HistoryEntry, token: int, timeout_sec: int = 60):
        try:
            if not entry.dlg.isVisible():
                return
            s, e = self._period_range()
            if entry.kind in ("status", "wip", "eip"):
                rows = await self.status_service.query_period([entry.code], s, e)
                if token != entry.token or not entry.dlg.isVisible():
                    return
                if not rows:
                    entry.dlg.start_loading("Syncing status...")
                else:
                    entry.dlg.set_loading_text("Syncing status...")
                async def run_sync():
                    return await self.status_service.sync_period([entry.code], s, e, progress_cb=lambda src, n: entry.dlg.set_loading_text(f"Syncing {src}: {n} rows..."), backfill=True)
                try:
                    await asyncio.wait_for(run_sync(), timeout=timeout_sec)
                except asyncio.TimeoutError:
                    pass
                if token != entry.token or not entry.dlg.isVisible():
                    return
                new_rows = await self.status_service.query_period([entry.code], s, e)
                if token == entry.token and entry.dlg.isVisible():
                    entry.dlg.load_rows([[r.get("equip_code",""), r.get("equip_status",""), r.get("event_time","")] for r in new_rows])
                    entry.dlg.stop_loading()
            elif entry.kind == "input":
                rows = await self.input_service.query_period([entry.code], s, e)
                if token != entry.token or not entry.dlg.isVisible():
                    return
                if not rows:
                    entry.dlg.start_loading("Syncing input...")
                else:
                    entry.dlg.set_loading_text("Syncing input...")
                async def run_sync():
                    return await self.input_service.sync_period([entry.code], s, e, progress_cb=lambda src, n: entry.dlg.set_loading_text(f"Syncing {src}: {n} rows..."), backfill=True)
                try:
                    await asyncio.wait_for(run_sync(), timeout=timeout_sec)
                except asyncio.TimeoutError:
                    pass
                if token != entry.token or not entry.dlg.isVisible():
                    return
                new_rows = await self.input_service.query_period([entry.code], s, e)
                if token == entry.token and entry.dlg.isVisible():
                    entry.dlg.load_rows([[r.get("equip_code",""), r.get("material_batch",""), r.get("feeding_time","")] for r in new_rows])
                    entry.dlg.stop_loading()
        except asyncio.CancelledError:
            return
        except Exception as ex:
            if entry.dlg.isVisible():
                entry.dlg.set_loading_text(f"Error: {ex}")

    @asyncSlot()
    async def _open_status(self, code: str):
        s, e = self._period_range()
        rows = await self.status_service.query_period([code], s, e)
        dlg = StatusDialog(self, code, rows)
        dlg.set_period_key(self.current_period_key)
        dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
        dlg.show()
        entry = self._register_history_dialog("status", code, dlg)
        self._restart_history_refresh(entry)

    @asyncSlot()
    async def _open_wip(self, code: str):
        s, e = self._period_range()
        rows = await self.status_service.query_period([code], s, e)
        dlg = WipDialog(self, code, rows)
        dlg.set_period_key(self.current_period_key)
        dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
        dlg.show()
        entry = self._register_history_dialog("wip", code, dlg)
        self._restart_history_refresh(entry)

    @asyncSlot()
    async def _open_eip(self, code: str):
        s, e = self._period_range()
        rows = await self.status_service.query_period([code], s, e)
        dlg = EipDialog(self, code, rows)
        dlg.set_period_key(self.current_period_key)
        dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
        dlg.show()
        entry = self._register_history_dialog("eip", code, dlg)
        self._restart_history_refresh(entry)

    @asyncSlot()
    async def _open_input(self, code: str):
        s, e = self._period_range()
        rows = await self.input_service.query_period([code], s, e)
        dlg = InputDialog(self, code, rows)
        dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
        dlg.show()
        entry = self._register_history_dialog("input", code, dlg)
        self._restart_history_refresh(entry)

    def _disable_text_shadows(self):
        for w in self.findChildren(QWidget):
            w.setGraphicsEffect(None)

    def _suppress_autofit(self, seconds: float = 1.5):
        self._suppress_autofit_until = time.monotonic() + seconds

    def _fit_current_view(self, throttled: bool = False):
        if throttled and time.monotonic() < self._suppress_autofit_until:
            return
        idx = self.tabs.currentIndex()
        if idx == 0 and hasattr(self, "view_electrode"):
            self.view_electrode.fit_all()
        elif idx == 1 and hasattr(self, "view_assembly"):
            self.view_assembly.fit_all()

    def _fit_all_views(self):
        if hasattr(self, "view_electrode"):
            self.view_electrode.fit_all()
        if hasattr(self, "view_assembly"):
            self.view_assembly.fit_all()

    def _open_palette(self):
        dlg = CommandPalette(self, self.codes, self._execute_palette)
        dlg.exec()

    def _execute_palette(self, act: str, code: str):
        act = act.lower()
        if act == "status":
            asyncio.create_task(self._open_status(code))
        elif act == "wip":
            asyncio.create_task(self._open_wip(code))
        elif act == "eip":
            asyncio.create_task(self._open_eip(code))
        elif act == "input":
            asyncio.create_task(self._open_input(code))

    def _settings_key(self) -> str:
        return f"{self.cfg.profile}"

    def _save_state(self):
        s = self._settings
        s.beginGroup(self._settings_key())
        s.setValue("geometry", self.saveGeometry())
        s.setValue("theme", self.cfg.theme.value)
        s.endGroup()

    def _load_state(self):
        s = self._settings
        s.beginGroup(self._settings_key())
        s.endGroup()
        self.current_period_key = "1d"

    def _restore_geometry(self):
        s = self._settings
        s.beginGroup(self._settings_key())
        geo = s.value("geometry")
        s.endGroup()
        if geo is not None:
            self.restoreGeometry(geo)

    def closeEvent(self, event):
        try:
            self.loader.stop()
            self.refresh_timer.stop()
        except Exception:
            pass
        for entry in list(self._history_entries):
            self._unregister_history_dialog(entry)
        self._save_state()
        super().closeEvent(event)