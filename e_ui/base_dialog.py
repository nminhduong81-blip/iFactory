from __future__ import annotations
from typing import List, Optional, Any, Callable, Tuple
from datetime import datetime, timedelta
from PySide6.QtCore import (
    Qt, QModelIndex, QTimer, QSettings, QByteArray, QPoint, QEvent, 
    QThreadPool, QRegularExpression
)
from PySide6.QtGui import QAction, QKeySequence, QColor, QBrush
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QWidget, QLineEdit, QTableView, QMenu, 
    QMessageBox, QHBoxLayout, QComboBox, QCheckBox, QToolButton, QHeaderView, 
    QAbstractItemView, QApplication
)
from e_ui.theme import theme_bus, theme_colors, current_theme_name
from e_ui.table_models import TableDataModel, ExtendedSortFilterProxyModel
from e_ui.table_delegates import CellDelegate
from e_ui.loading_overlay import LoadingOverlay
from e_ui.chart_dialog import ColumnChartDialog
from e_ui.indexing import IndexSignals, IndexTask
from e_ui.utils import to_datetime

class BaseTableDialog(QDialog):
    SETTINGS_ORG = "MyCompany"
    SETTINGS_APP = "MyTablesApp"

    def __init__(self, parent: QWidget, title: str, headers: List[str]) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(1000, 640)
        self._headers = headers[:]
        self._settings = QSettings(self.SETTINGS_ORG, self.SETTINGS_APP)
        self._theme_name = current_theme_name()
        self._status_col: Optional[int] = None
        self._status_alpha_light = 56
        self._status_alpha_dark = 72
        self._highlight_threshold = 50000
        self._indexing_threshold = 30000
        self._page_size = 2000
        self._time_col_idx: Optional[int] = None
        self._all_rows: List[List[Any]] = []
        self._loaded_count = 0
        self._indexing_token = 0
        self._indexing_running = False
        self._pool = QThreadPool.globalInstance()
        self.model = TableDataModel(headers, parent=self)
        self.proxy = ExtendedSortFilterProxyModel(self)
        self.proxy.setSourceModel(self.model)
        self.proxy.setDynamicSortFilter(True)
        self._setup_ui()
        self._setup_actions()
        self._setup_connections()
        self._restore_settings()
        self._autosize_columns(initial=True)
        self._apply_filter()
        self._update_counts()

    def _setup_ui(self):
        root = QVBoxLayout(self)
        top = QHBoxLayout()
        self.cmb_column = QComboBox(self)
        self.cmb_column.addItem("All columns (*)", -1)
        for i, h in enumerate(self._headers):
            self.cmb_column.addItem(h, i)
        self.cmb_column.setToolTip("Choose a column to filter, or all columns")
        self.search = QLineEdit(self)
        self.search.setPlaceholderText("Search...")
        self.search.setClearButtonEnabled(True)
        self.chk_regex = QCheckBox("Regex", self)
        self.chk_case = QCheckBox("Aa", self)
        self.chk_case.setToolTip("Case sensitive")
        self.chk_exact = QCheckBox("Exact", self)
        self.chk_exact.setToolTip("Exact match")
        self.chk_no_accent = QCheckBox("No accents", self)
        self.chk_no_accent.setToolTip("Accent-insensitive (Vietnamese)")
        self.cmb_logic = QComboBox(self)
        self.cmb_logic.addItems(["OR", "AND"])
        self.cmb_logic.setToolTip("Combine tokens: OR or AND")
        btn_clear = QToolButton(self)
        btn_clear.setText("Clear")
        btn_clear.setToolTip("Clear filter")
        btn_clear.clicked.connect(self._clear_filter)
        top.addWidget(QLabel("Column:", self))
        top.addWidget(self.cmb_column, 0)
        top.addSpacing(8)
        top.addWidget(QLabel("Filter:", self))
        top.addWidget(self.search, 1)
        top.addWidget(self.cmb_logic)
        top.addWidget(self.chk_no_accent)
        top.addWidget(self.chk_regex)
        top.addWidget(self.chk_case)
        top.addWidget(self.chk_exact)
        top.addWidget(btn_clear)
        root.addLayout(top)
        self.table = QTableView(self)
        self.table.setModel(self.proxy)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectItems)
        self.table.setSelectionMode(QTableView.SelectionMode.ExtendedSelection)
        self.table.setWordWrap(False)
        self.table.setCornerButtonEnabled(True)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(True)
        self.table.verticalHeader().setDefaultSectionSize(22)
        hh = self.table.horizontalHeader()
        hh.setStretchLastSection(False)
        hh.setSectionsMovable(True)
        hh.setHighlightSections(False)
        hh.setContextMenuPolicy(Qt.CustomContextMenu)
        hh.customContextMenuRequested.connect(self._show_header_menu)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_table_menu)
        self.table.doubleClicked.connect(self.show_row_detail)
        self._delegate = CellDelegate(
            self.table,
            get_regex=self._current_regex,
            should_highlight_col=self._should_highlight_col,
            get_cell_bg=self._cell_background_for_index,
            highlight_brush=self._make_highlight_brush()
        )
        self.table.setItemDelegate(self._delegate)
        root.addWidget(self.table, 1)
        nav = QHBoxLayout()
        self.lbl_page = QLabel("0 rows", self)
        nav.addWidget(self.lbl_page, 0)
        nav.addStretch(1)
        self.btn_latest = QToolButton(self)
        self.btn_latest.setText("Latest")
        self.btn_latest.setToolTip("Show latest page")
        self.btn_latest.clicked.connect(self._on_latest)
        nav.addWidget(self.btn_latest)
        self.btn_load_more = QToolButton(self)
        self.btn_load_more.setText("Load more")
        self.btn_load_more.clicked.connect(self._on_load_more)
        nav.addWidget(self.btn_load_more)
        self.btn_autosize = QToolButton(self)
        self.btn_autosize.setText("Autosize")
        self.btn_autosize.setToolTip("Auto fit column width to contents")
        self.btn_autosize.clicked.connect(self._autosize_columns)
        nav.addWidget(self.btn_autosize)
        self.btn_chart = QToolButton(self)
        self.btn_chart.setText("Chart")
        self.btn_chart.setToolTip("Open column chart")
        self.btn_chart.clicked.connect(self._open_chart)
        nav.addWidget(self.btn_chart)
        root.addLayout(nav)
        self._default_header_state: QByteArray = hh.saveState()
        self._overlay = LoadingOverlay(self.table.viewport())
        self.table.viewport().installEventFilter(self)
        self._period_key: str = "1d"
        theme_bus.changed.connect(self._on_theme_changed)
        self.setWindowFlags(self.windowFlags() | Qt.Window | Qt.WindowMinMaxButtonsHint | Qt.WindowCloseButtonHint)
        self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)
        self.setSizeGripEnabled(True)

    def _setup_actions(self):
        act_copy = QAction("Copy", self)
        act_copy.setShortcut(QKeySequence.Copy)
        act_copy.triggered.connect(self.copy_selection_to_clipboard)
        self.table.addAction(act_copy)
        act_copy_nohdr = QAction("Copy (no headers)", self)
        act_copy_nohdr.setShortcut("Ctrl+Shift+C")
        act_copy_nohdr.triggered.connect(lambda: self.copy_selection_to_clipboard(with_headers=False))
        self.table.addAction(act_copy_nohdr)
        act_select_all = QAction("Select All", self)
        act_select_all.setShortcut(QKeySequence.SelectAll)
        act_select_all.triggered.connect(self.table.selectAll)
        self.table.addAction(act_select_all)
        act_focus_search = QAction("Focus Search", self)
        act_focus_search.setShortcut("Ctrl+F")
        act_focus_search.triggered.connect(self.search.setFocus)
        self.addAction(act_focus_search)
        act_clear = QAction("Clear Filter", self)
        act_clear.setShortcut(QKeySequence(Qt.Key_Escape))
        act_clear.triggered.connect(self._escape_action)
        self.addAction(act_clear)

    def _setup_connections(self):
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(300)
        self._filter_timer.timeout.connect(self._apply_filter)
        self.search.textChanged.connect(self._on_filter_input_changed)
        self.cmb_column.currentIndexChanged.connect(self._apply_filter)
        self.chk_regex.toggled.connect(self._apply_filter)
        self.chk_case.toggled.connect(self._apply_filter)
        self.chk_exact.toggled.connect(self._apply_filter)
        self.chk_no_accent.toggled.connect(self._apply_filter)
        self.cmb_logic.currentIndexChanged.connect(self._apply_filter)
        self.proxy.rowsInserted.connect(self._update_counts)
        self.proxy.rowsRemoved.connect(self._update_counts)
        self.proxy.modelReset.connect(self._update_counts)
        self.proxy.layoutChanged.connect(self._update_counts)
        self.table.selectionModel().selectionChanged.connect(self._update_counts)

    def set_period_key(self, key: str):
        self._period_key = key or "1d"

    def _period_range_for_key(self, key: str) -> tuple[datetime, datetime]:
        now = datetime.now()
        today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if key == "1d":
            start = today0
            end_axis = today0 + timedelta(days=1)
        elif key == "1w":
            start = today0 - timedelta(days=6)
            end_axis = today0 + timedelta(days=1)
        elif key == "1m":
            start = today0 - timedelta(days=29)
            end_axis = today0 + timedelta(days=1)
        elif key == "3m":
            start = today0 - timedelta(days=89)
            end_axis = today0 + timedelta(days=1)
        elif key == "6m":
            start = today0 - timedelta(days=179)
            end_axis = today0 + timedelta(days=1)
        else:
            start = today0 - timedelta(days=29)
            end_axis = today0 + timedelta(days=1)
        return start, end_axis

    def _rows_events(self) -> list[tuple[datetime, Optional[str]]]:
        try:
            h = self.model.headers()
            t_idx = h.index("event_time")
            s_idx = h.index("equip_status") if "equip_status" in h else -1
        except Exception:
            return []
        out = []
        for r in self._all_rows:
            t = r[t_idx] if t_idx < len(r) else None
            dt = to_datetime(t)
            if not dt:
                continue
            st = r[s_idx] if 0 <= s_idx < len(r) else None
            st = str(st) if st is not None else None
            out.append((dt, st))
        out.sort(key=lambda x: x[0])
        return out

    def _build_segments_in_range(self, events: list[tuple[datetime, Optional[str]]], 
                                 start: datetime, clip_end: datetime) -> list[tuple[datetime, datetime, Optional[str]]]:
        segs: list[tuple[datetime, datetime, Optional[str]]] = []
        if clip_end <= start:
            return segs
        initial_status: Optional[str] = None
        for t, s in events:
            if t < start:
                initial_status = s
            else:
                break
        cur_t = start
        cur_status = initial_status
        for t, s in events:
            if t < start:
                continue
            if t > clip_end:
                break
            if cur_t < t:
                segs.append((cur_t, t, cur_status))
            cur_t = t
            cur_status = s
        if cur_t < clip_end:
            segs.append((cur_t, clip_end, cur_status))
        return segs

    def _bucket_edges(self, key: str, start: datetime, end_axis: datetime, 
                      clip_end: datetime) -> tuple[list[str], list[tuple[datetime, datetime]], str, list[float]]:
        edges: list[tuple[datetime, datetime]] = []
        labels: list[str] = []
        bucket_totals: list[float] = []
        if key == "1d":
            unit = "min"
            day0 = start
            for i in range(24):
                a = day0 + timedelta(hours=i)
                b = day0 + timedelta(hours=i + 1)
                labels.append(f"{i:02d}h")
                eff_b = min(b, clip_end)
                if eff_b > a:
                    bucket_totals.append((eff_b - a).total_seconds() / 60.0)
                else:
                    bucket_totals.append(0.0)
                edges.append((a, b))
        else:
            unit = "h"
            day = start
            while day < end_axis:
                a = day
                b = day + timedelta(days=1)
                labels.append(a.strftime("%d/%m"))
                eff_b = min(b, clip_end)
                if eff_b > a:
                    bucket_totals.append((eff_b - a).total_seconds() / 3600.0)
                else:
                    bucket_totals.append(0.0)
                edges.append((a, b))
                day = b
        return labels, edges, unit, bucket_totals

    def _accumulate_by_bucket(self, segs: list[tuple[datetime, datetime, Optional[str]]], 
                              edges: list[tuple[datetime, datetime]], 
                              unit: str) -> tuple[dict[str, list[float]], list[str]]:
        data: dict[str, list[float]] = {}
        for _, _, st in segs:
            if st is None:
                continue
            if st not in data:
                data[st] = [0.0] * len(edges)
        for (s, e, st) in segs:
            if st is None:
                continue
            for i, (ba, bb) in enumerate(edges):
                a = max(s, ba)
                b = min(e, bb)
                if b <= a:
                    continue
                val = (b - a).total_seconds()
                if unit == "min":
                    val /= 60.0
                else:
                    val /= 3600.0
                data.setdefault(st, [0.0] * len(edges))
                data[st][i] += val
        ordered_statuses = list(data.keys())
        return data, ordered_statuses

    def _open_chart(self):
        h = self.model.headers()
        if "equip_status" not in h or "event_time" not in h:
            QMessageBox.information(self, "Chart", "Không có dữ liệu trạng thái để vẽ chart.")
            return
        if not self._all_rows:
            QMessageBox.information(self, "Chart", "Không có dữ liệu.")
            return
        s, e_axis = self._period_range_for_key(self._period_key)
        now = datetime.now()
        clip_end = min(e_axis, now)
        events = self._rows_events()
        segs = self._build_segments_in_range(events, s, clip_end)
        labels, edges, unit, bucket_totals = self._bucket_edges(self._period_key, s, e_axis, clip_end)
        data, ordered_statuses = self._accumulate_by_bucket(segs, edges, unit)
        if not data:
            QMessageBox.information(self, "Chart", "Không có dữ liệu trong khoảng được chọn.")
            return
        colors = theme_colors(self._theme_name)
        status_map = colors["status"]
        color_map = {st: QColor(str(status_map.get(st, status_map.get(None, "#80808040")))) for st in ordered_statuses}
        title = f"{self.windowTitle()} • {self._period_key.upper()} column chart"
        dlg = ColumnChartDialog(self, title, labels, data, ordered_statuses, unit, bucket_totals, color_map, theme=self._theme_name)
        dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
        dlg.resize(900, 520)
        dlg.exec()

    def set_page_size(self, n: int):
        self._page_size = max(100, int(n))

    def set_time_column_by_header(self, header_name: str):
        try:
            idx = self._headers.index(header_name)
        except ValueError:
            idx = None
        self._time_col_idx = idx

    def set_status_column(self, col: int):
        self._status_col = col
        self.table.viewport().update()

    def _on_theme_changed(self, name: str, colors: object):
        self._theme_name = name
        self._delegate.set_highlight_brush(self._make_highlight_brush())
        self.table.viewport().update()

    def _make_highlight_brush(self) -> QBrush:
        colors = theme_colors(self._theme_name)
        accent = str(colors["primary"])
        c = QColor(accent)
        c.setAlpha(56 if self._theme_name == "light" else 76)
        return QBrush(c)

    def start_loading(self, text: str = "Đang tải dữ liệu..."):
        self._overlay.start(text)

    def set_loading_text(self, text: str):
        self._overlay.set_text(text)

    def stop_loading(self):
        self._overlay.stop()

    def eventFilter(self, obj, ev):
        if obj is self.table.viewport() and ev.type() == QEvent.Resize:
            if self._overlay.isVisible():
                self._overlay.resize(self.table.viewport().size())
        return super().eventFilter(obj, ev)

    def clear_rows(self):
        self._all_rows = []
        self._loaded_count = 0
        self.model.clear()
        self._update_counts()
        self._update_nav()

    def _sorted_all_rows(self) -> List[List[Any]]:
        if self._time_col_idx is None:
            return self._all_rows[:]
        idx = self._time_col_idx
        def key_fn(r: List[Any]):
            dt = to_datetime(r[idx] if idx < len(r) else None)
            return dt or datetime.min
        return sorted(self._all_rows, key=key_fn, reverse=True)

    def _apply_pagination(self):
        if not self._all_rows:
            self.model.set_rows([])
            self._update_nav()
            return
        self._loaded_count = min(self._loaded_count or self._page_size, len(self._all_rows))
        sorted_rows = self._sorted_all_rows()
        view_rows = sorted_rows[: self._loaded_count]
        self.model.set_rows(view_rows)
        self._update_nav()
        self._start_indexing_if_needed()
        self._apply_filter()

    def load_rows(self, rows: List[List[Any]]):
        self._all_rows = rows[:]
        self._loaded_count = min(len(self._all_rows), self._page_size)
        self._apply_pagination()
        self._autosize_columns(initial=True)

    def _on_load_more(self):
        if not self._all_rows:
            return
        if self._loaded_count >= len(self._all_rows):
            return
        self._loaded_count = min(len(self._all_rows), self._loaded_count + self._page_size)
        self._apply_pagination()

    def _on_latest(self):
        if not self._all_rows:
            return
        self._loaded_count = min(self._page_size, len(self._all_rows))
        self._apply_pagination()

    def show_row_detail(self, index: QModelIndex):
        row = index.row()
        cols = range(self.proxy.columnCount())
        row_data = [self.proxy.data(self.proxy.index(row, c)) for c in cols]
        text = "\n".join(f"{self.model.headerData(i, Qt.Orientation.Horizontal)}: {row_data[i]}" for i in cols)
        QMessageBox.information(self, "Row Detail", text)

    def copy_selection_to_clipboard(self, with_headers: bool = True, sep: str = "\t"):
        sm = self.table.selectionModel()
        if not sm or not sm.hasSelection():
            return
        indexes = sm.selectedIndexes()
        rows = sorted({i.row() for i in indexes})
        hh = self.table.horizontalHeader()
        cols = sorted({i.column() for i in indexes}, key=lambda c: hh.visualIndex(c))
        sel_set = {(i.row(), i.column()) for i in indexes}
        lines = []
        if with_headers:
            headers = [self.model.headerData(c, Qt.Horizontal) for c in cols]
            lines.append(sep.join(str(h) for h in headers))
        for r in rows:
            vals = []
            for c in cols:
                vals.append("" if (r, c) not in sel_set else str(self.proxy.index(r, c).data() or ""))
            lines.append(sep.join(vals))
        QApplication.clipboard().setText("\n".join(lines))

    def _escape_action(self):
        if self.search.text():
            self._clear_filter()
        else:
            self.close()

    def _on_filter_input_changed(self, _):
        self._filter_timer.start()

    def _apply_filter(self):
        sorting = self.table.isSortingEnabled()
        self.table.setSortingEnabled(False)
        text = self.search.text()
        exact = self.chk_exact.isChecked()
        is_regex = self.chk_regex.isChecked()
        case_sensitive = self.chk_case.isChecked()
        ignore_accents = self.chk_no_accent.isChecked()
        logic_and = self.cmb_logic.currentText() == "AND"
        col = self.cmb_column.currentData()
        key_col = int(col) if isinstance(col, int) else -1
        self.proxy.setFilterParams(
            text=text,
            is_regex=is_regex,
            exact=exact,
            case_sensitive=case_sensitive,
            ignore_accents=ignore_accents,
            logic_and=logic_and,
            key_col=key_col,
        )
        self._maybe_index_for_filter()
        self._update_counts()
        self.table.viewport().update()
        self.table.setSortingEnabled(sorting)

    def _clear_filter(self):
        self.search.clear()
        self.chk_regex.setChecked(False)
        self.chk_case.setChecked(False)
        self.chk_exact.setChecked(False)
        self.chk_no_accent.setChecked(True)
        self.cmb_logic.setCurrentIndex(0)
        self.cmb_column.setCurrentIndex(0)
        self._apply_filter()

    def _maybe_index_for_filter(self):
        if self.model.rowCount() < self._indexing_threshold:
            return
        key_col = self.cmb_column.currentData()
        if not isinstance(key_col, int):
            key_col = -1
        if key_col < 0:
            cols = tuple(range(self.model.columnCount()))
        else:
            cols = (key_col,)
        case_sensitive = self.chk_case.isChecked()
        ignore_accents = self.chk_no_accent.isChecked()
        self._start_indexing(cols, case_sensitive, ignore_accents)

    def _start_indexing_if_needed(self):
        if self.model.rowCount() < self._indexing_threshold:
            return
        key_col = self.cmb_column.currentData()
        if not isinstance(key_col, int):
            key_col = -1
        if key_col < 0:
            cols = tuple(range(self.model.columnCount()))
        else:
            cols = (key_col,)
        case_sensitive = self.chk_case.isChecked()
        ignore_accents = self.chk_no_accent.isChecked()
        self._start_indexing(cols, case_sensitive, ignore_accents)

    def _start_indexing(self, cols: Tuple[int, ...], case_sensitive: bool, ignore_accents: bool):
        if self._indexing_running:
            return
        rows_snapshot = [r[:] for r in self.model._rows]
        token = self._indexing_token + 1
        self._indexing_token = token
        self._overlay.start("Indexing...")
        sig = IndexSignals()
        sig.finished.connect(self._on_index_done)
        sig.error.connect(self._on_index_error)
        task = IndexTask(rows_snapshot, cols, case_sensitive, ignore_accents, token, sig)
        self._pool.start(task)

    def _on_index_done(self, cache: object, cols_key: tuple, norm_key: tuple, token: int):
        if token != self._indexing_token:
            self._indexing_running = False
            self._overlay.stop()
            return
        cache_list = cache if isinstance(cache, list) else []
        self.proxy.set_prebuilt_cache(cache_list, tuple(cols_key), tuple(norm_key))
        self._indexing_running = False
        self._overlay.stop()

    def _on_index_error(self, msg: str, token: int):
        self._indexing_running = False
        self._overlay.stop()

    def _update_counts(self):
        total_all = len(self._all_rows)
        total = self.model.rowCount()
        visible = self.proxy.rowCount()
        selected = len(self.table.selectionModel().selectedRows())
        hh = self.table.horizontalHeader()
        vis_cols = sum(1 for i in range(hh.count()) if not hh.isSectionHidden(i))
        self.lbl_page.setText(f"Rows: {visible}/{total} of {total_all} | Selected: {selected} | Cols: {vis_cols}/{hh.count()}")

    def _update_nav(self):
        rem = max(0, len(self._all_rows) - self._loaded_count)
        self.btn_load_more.setEnabled(rem > 0)
        self.btn_load_more.setText(f"Load more (+{min(self._page_size, rem)})")

    def _should_highlight_col(self, col: int) -> bool:
        key_col = self.cmb_column.currentData()
        key_col = int(key_col) if isinstance(key_col, int) else -1
        if self.model.rowCount() > self._highlight_threshold and key_col < 0:
            return False
        return key_col == -1 or col == key_col

    def _current_regex(self) -> Optional[QRegularExpression]:
        return self.proxy.highlight_regex()

    def _cell_background_for_index(self, index: QModelIndex) -> Optional[QBrush]:
        if self._status_col is None:
            return None
        if index.column() != self._status_col:
            return None
        val = index.data()
        if not isinstance(val, str):
            val = str(val) if val is not None else ""
        colors = theme_colors(self._theme_name)
        status_map = colors["status"]
        hexc = status_map.get(val, status_map.get(None, "#00000000"))
        c = QColor(hexc)
        c.setAlpha(self._status_alpha_light if self._theme_name == "light" else self._status_alpha_dark)
        return QBrush(c)

    def _autosize_columns(self, initial: bool = False):
        hh = self.table.horizontalHeader()
        hh.setSectionResizeMode(QHeaderView.ResizeToContents)
        maxw = 600
        for c in range(self.model.columnCount()):
            w = min(self.table.columnWidth(c), maxw)
            self.table.setColumnWidth(c, max(w, 80))
        hh.setSectionResizeMode(QHeaderView.Interactive)
        if not initial:
            self._save_settings()

    def _show_table_menu(self, pos: QPoint):
        menu = QMenu(self)
        menu.addSeparator()
        act_copy = QAction("Copy (Ctrl+C)", self)
        act_copy.triggered.connect(self.copy_selection_to_clipboard)
        menu.addAction(act_copy)
        act_copy_nohdr = QAction("Copy (no headers) (Ctrl+Shift+C)", self)
        act_copy_nohdr.triggered.connect(lambda: self.copy_selection_to_clipboard(with_headers=False))
        menu.addAction(act_copy_nohdr)
        menu.addSeparator()
        act_auto = QAction("Autosize columns", self)
        act_auto.triggered.connect(self._autosize_columns)
        menu.addAction(act_auto)
        act_latest = QAction("Show Latest", self)
        act_latest.triggered.connect(self._on_latest)
        menu.addAction(act_latest)
        act_reset_cols = QAction("Reset columns", self)
        act_reset_cols.triggered.connect(self._reset_columns)
        menu.addAction(act_reset_cols)
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def _show_header_menu(self, pos: QPoint):
        menu = QMenu(self)
        hh = self.table.horizontalHeader()
        for visual in range(hh.count()):
            logical = hh.logicalIndex(visual)
            text = str(self.model.headerData(logical, Qt.Horizontal))
            act = QAction(text, self)
            act.setCheckable(True)
            act.setChecked(not hh.isSectionHidden(logical))
            act.toggled.connect(lambda checked, col=logical: hh.setSectionHidden(col, not checked))
            menu.addAction(act)
        menu.addSeparator()
        act_reset = QAction("Reset columns", self)
        act_reset.triggered.connect(self._reset_columns)
        menu.addAction(act_reset)
        menu.exec(self.table.horizontalHeader().viewport().mapToGlobal(pos))

    def _reset_columns(self):
        hh = self.table.horizontalHeader()
        hh.restoreState(self._default_header_state)
        self._autosize_columns()
        self._save_settings()

    def _current_column_order(self, visible_only: bool = True) -> List[int]:
        hh = self.table.horizontalHeader()
        order = []
        for visual in range(hh.count()):
            logical = hh.logicalIndex(visual)
            if visible_only and hh.isSectionHidden(logical):
                continue
            order.append(logical)
        return order

    def _settings_key(self) -> str:
        return f"BaseTableDialog/{self.windowTitle()}"

    def _save_settings(self):
        key = self._settings_key()
        self._settings.beginGroup(key)
        self._settings.setValue("geometry", self.saveGeometry())
        self._settings.setValue("header", self.table.horizontalHeader().saveState())
        self._settings.endGroup()

    def _restore_settings(self):
        key = self._settings_key()
        self._settings.beginGroup(key)
        geo = self._settings.value("geometry")
        hdr = self._settings.value("header")
        self._settings.endGroup()
        if isinstance(geo, QByteArray):
            self.restoreGeometry(geo)
        if isinstance(hdr, QByteArray):
            self.table.horizontalHeader().restoreState(hdr)

    def closeEvent(self, e):
        self._save_settings()
        super().closeEvent(e)