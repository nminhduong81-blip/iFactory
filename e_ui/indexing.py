from __future__ import annotations
from typing import List, Any, Tuple
from PySide6.QtCore import QObject, Signal, QRunnable
from e_ui.utils import strip_accents

class IndexSignals(QObject):
    finished = Signal(object, tuple, tuple, int)
    error = Signal(str, int)

class IndexTask(QRunnable):
    def __init__(self, rows: List[List[Any]], cols: Tuple[int, ...], 
                 case_sensitive: bool, ignore_accents: bool, token: int, signals: IndexSignals):
        super().__init__()
        self.rows = rows
        self.cols = cols
        self.case_sensitive = case_sensitive
        self.ignore_accents = ignore_accents
        self.token = token
        self.signals = signals

    def run(self):
        try:
            out: List[List[str]] = []
            for r in self.rows:
                fields: List[str] = []
                for c in self.cols:
                    v = r[c] if c < len(r) else ""
                    s = v if isinstance(v, str) else ("" if v is None else str(v))
                    if not self.case_sensitive:
                        s = s.lower()
                    if self.ignore_accents:
                        s = strip_accents(s)
                    fields.append(s)
                out.append(fields)
            self.signals.finished.emit(out, self.cols, (self.case_sensitive, self.ignore_accents), self.token)
        except Exception as ex:
            self.signals.error.emit(str(ex), self.token)