from __future__ import annotations
from typing import List, Optional, Any
from PySide6.QtCore import Qt, QSortFilterProxyModel, QModelIndex, QAbstractTableModel, QRegularExpression
from PySide6.QtWidgets import QWidget
from e_ui.utils import strip_accents

class TableDataModel(QAbstractTableModel):
    def __init__(self, headers: List[str], rows: Optional[List[List[Any]]] = None, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._headers = headers[:]
        self._rows: List[List[Any]] = rows[:] if rows else []

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._headers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid():
            return None
        if role in (Qt.DisplayRole, Qt.EditRole):
            r, c = index.row(), index.column()
            val = self._rows[r][c] if c < len(self._rows[r]) else ""
            return "" if val is None else str(val)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self._headers[section] if 0 <= section < len(self._headers) else ""
        return section + 1

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled

    def set_headers(self, headers: List[str]) -> None:
        self.beginResetModel()
        self._headers = headers[:]
        self.endResetModel()

    def set_rows(self, rows: List[List[Any]]) -> None:
        self.beginResetModel()
        self._rows = rows[:]
        self.endResetModel()

    def clear(self) -> None:
        self.set_rows([])

    def headers(self) -> List[str]:
        return self._headers[:]

class ExtendedSortFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._text: str = ""
        self._is_regex: bool = False
        self._exact: bool = False
        self._case_sensitive: bool = False
        self._ignore_accents: bool = False
        self._logic_and: bool = False
        self._filter_key_col: int = -1
        self._highlight_re: Optional[QRegularExpression] = None
        self._tokens: List[str] = []
        self._row_fields_cache: List[Optional[List[str]]] = []
        self._cache_cols_key: Optional[tuple[int, ...]] = None
        self._cache_norm_key: Optional[tuple[bool, bool]] = None
        self._bound_model = None

    def setSourceModel(self, sourceModel):
        super().setSourceModel(sourceModel)
        self._bind_source_signals()
        self._reset_cache()
        self.invalidateFilter()

    def _bind_source_signals(self):
        if self._bound_model is not None:
            try:
                self._bound_model.modelReset.disconnect(self._on_source_reset)
                self._bound_model.layoutChanged.disconnect(self._on_source_reset)
                self._bound_model.rowsInserted.disconnect(self._on_rows_changed)
                self._bound_model.rowsRemoved.disconnect(self._on_rows_changed)
                self._bound_model.dataChanged.disconnect(self._on_source_data_changed)
            except Exception:
                pass
        m = self.sourceModel()
        self._bound_model = m
        if m is not None:
            m.modelReset.connect(self._on_source_reset)
            m.layoutChanged.connect(self._on_source_reset)
            m.rowsInserted.connect(self._on_rows_changed)
            m.rowsRemoved.connect(self._on_rows_changed)
            m.dataChanged.connect(self._on_source_data_changed)

    def _on_source_reset(self):
        self._reset_cache()
        self.invalidateFilter()

    def _on_rows_changed(self, *args):
        self._resize_cache()
        self.invalidateFilter()

    def _on_source_data_changed(self, *args):
        self._reset_cache()
        self.invalidateFilter()

    def _resize_cache(self):
        m = self.sourceModel()
        if m is None:
            self._row_fields_cache = []
            return
        n = m.rowCount()
        cur = len(self._row_fields_cache)
        if n == cur:
            return
        if n > cur:
            self._row_fields_cache.extend([None] * (n - cur))
        else:
            self._row_fields_cache = self._row_fields_cache[:n]

    def _reset_cache(self):
        m = self.sourceModel()
        n = m.rowCount() if m else 0
        self._row_fields_cache = [None] * n
        self._cache_cols_key = None
        self._cache_norm_key = None

    def set_prebuilt_cache(self, cache: List[List[str]], cols_key: tuple[int, ...], norm_key: tuple[bool, bool]):
        m = self.sourceModel()
        if m is None:
            return
        if len(cache) != m.rowCount():
            return
        self._row_fields_cache = [fields[:] for fields in cache]
        self._cache_cols_key = cols_key
        self._cache_norm_key = norm_key
        self.invalidateFilter()

    def setFilterParams(self, text: str, is_regex: bool, exact: bool, case_sensitive: bool, ignore_accents: bool, logic_and: bool, key_col: int):
        self._text = text or ""
        self._is_regex = is_regex
        self._exact = exact
        self._case_sensitive = case_sensitive
        self._ignore_accents = ignore_accents
        self._logic_and = logic_and
        self._filter_key_col = int(key_col) if isinstance(key_col, int) else -1
        self._tokens = [t for t in self._text.split() if t]
        self._highlight_re = self._make_highlight_regex()
        self.setFilterRegularExpression(self._highlight_re or QRegularExpression())
        self._reset_cache()
        self.invalidateFilter()

    def highlight_regex(self) -> Optional[QRegularExpression]:
        return self._highlight_re

    def _norm(self, s: str) -> str:
        if s is None:
            s = ""
        s = s if self._case_sensitive else s.lower()
        if self._ignore_accents:
            s = strip_accents(s)
        return s

    def _make_highlight_regex(self) -> Optional[QRegularExpression]:
        if not self._text:
            return None
        if self._is_regex:
            pattern = self._text
            if self._exact and not pattern.startswith("^"):
                pattern = f"^{pattern}$"
            opts = QRegularExpression.CaseInsensitiveOption if not self._case_sensitive else QRegularExpression.NoPatternOption
            return QRegularExpression(pattern, opts)
        tokens = [t for t in self._text.split() if t]
        if not tokens:
            return None
        pat = "|".join(QRegularExpression.escape(t) for t in tokens)
        opts = QRegularExpression.CaseInsensitiveOption if not self._case_sensitive else QRegularExpression.NoPatternOption
        return QRegularExpression(pat, opts)

    def _selected_columns(self, model) -> List[int]:
        if self._filter_key_col is None or self._filter_key_col < 0:
            return list(range(model.columnCount()))
        return [self._filter_key_col]

    def _ensure_row_cache(self, row: int):
        m = self.sourceModel()
        if m is None:
            return
        cols = tuple(self._selected_columns(m))
        norm_key = (self._case_sensitive, self._ignore_accents)
        if self._cache_cols_key != cols or self._cache_norm_key != norm_key:
            self._row_fields_cache = [None] * m.rowCount()
            self._cache_cols_key = cols
            self._cache_norm_key = norm_key
        if 0 <= row < len(self._row_fields_cache) and self._row_fields_cache[row] is None:
            fields: List[str] = []
            for c in cols:
                idx = m.index(row, c)
                val = idx.data() or ""
                s = val if isinstance(val, str) else str(val)
                fields.append(self._norm(s))
            self._row_fields_cache[row] = fields

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        if not self._text:
            return True
        m = self.sourceModel()
        if m is None:
            return True
        if self._is_regex:
            rexp = self._highlight_re
            if rexp is None:
                return True
            if self._filter_key_col is None or self._filter_key_col < 0:
                for c in range(m.columnCount()):
                    idx = m.index(source_row, c)
                    val = idx.data() or ""
                    s = val if isinstance(val, str) else str(val)
                    if rexp.match(s).hasMatch():
                        return True
                return False
            idx = m.index(source_row, self._filter_key_col)
            val = idx.data() or ""
            s = val if isinstance(val, str) else str(val)
            return rexp.match(s).hasMatch()
        self._ensure_row_cache(source_row)
        fields = self._row_fields_cache[source_row] or []
        if not self._tokens:
            return True
        tokens_norm = [self._norm(t) for t in self._tokens]
        def token_in_fields(tok: str) -> bool:
            if self._exact:
                return any(tok == f for f in fields)
            return any(tok in f for f in fields)
        if self._logic_and:
            return all(token_in_fields(t) for t in tokens_norm)
        return any(token_in_fields(t) for t in tokens_norm)

    def lessThan(self, left: QModelIndex, right: QModelIndex) -> bool:
        lv = self.sourceModel().data(left, Qt.DisplayRole)
        rv = self.sourceModel().data(right, Qt.DisplayRole)
        try:
            lf = float(str(lv).replace(",", ""))
            rf = float(str(rv).replace(",", ""))
            return lf < rf
        except Exception:
            pass
        ls = str(lv) if lv is not None else ""
        rs = str(rv) if rv is not None else ""
        ls = self._norm(ls)
        rs = self._norm(rs)
        return ls < rs