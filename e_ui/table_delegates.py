from __future__ import annotations
from typing import Callable, Optional
from PySide6.QtCore import QModelIndex, QRegularExpression
from PySide6.QtGui import QPainter, QBrush
from PySide6.QtWidgets import QStyledItemDelegate

class CellDelegate(QStyledItemDelegate):
    def __init__(self, parent, get_regex: Callable[[], Optional[QRegularExpression]], 
                 should_highlight_col: Callable[[int], bool], 
                 get_cell_bg: Callable[[QModelIndex], Optional[QBrush]], 
                 highlight_brush: QBrush):
        super().__init__(parent)
        self._get_regex = get_regex
        self._should_highlight_col = should_highlight_col
        self._get_cell_bg = get_cell_bg
        self._highlight_brush = highlight_brush

    def set_highlight_brush(self, brush: QBrush):
        self._highlight_brush = brush

    def paint(self, painter: QPainter, option, index: QModelIndex):
        bg = self._get_cell_bg(index)
        if bg is not None:
            painter.save()
            painter.fillRect(option.rect, bg)
            painter.restore()
        regex = self._get_regex()
        text = index.data()
        if regex is not None and isinstance(text, str) and self._should_highlight_col(index.column()):
            if regex.match(text).hasMatch():
                painter.save()
                painter.fillRect(option.rect, self._highlight_brush)
                painter.restore()
        super().paint(painter, option, index)