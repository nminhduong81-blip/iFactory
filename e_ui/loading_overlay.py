from __future__ import annotations
from PySide6.QtCore import Qt, QTimer, QSize
from PySide6.QtGui import QPainter, QColor, QPen
from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel

class Spinner(QWidget):
    def __init__(self, parent=None, size: int = 36, line_count: int = 12, 
                 line_length: int = 7, line_width: int = 3):
        super().__init__(parent)
        self._size = size
        self._count = line_count
        self._len = line_length
        self._w = line_width
        self._step = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self._interval_ms = 80
        self._timer.start(self._interval_ms)
        self.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        self.setFixedSize(QSize(size, size))

    def _tick(self):
        self._step = (self._step + 1) % self._count
        self.update()

    def start(self):
        if not self._timer.isActive():
            self._timer.start(self._interval_ms)

    def stop(self):
        self._timer.stop()
        self._step = 0
        self.update()

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing, True)
        p.translate(self.width() / 2, self.height() / 2)
        color = self.palette().highlight().color()
        radius = min(self.width(), self.height()) // 2 - self._len - 2
        for i in range(self._count):
            alpha = int(255 * (i + 1) / self._count)
            c = QColor(color)
            c.setAlpha(alpha)
            pen = QPen(c, self._w, Qt.SolidLine, Qt.RoundCap)
            p.setPen(pen)
            p.drawLine(0, -radius, 0, -(radius + self._len))
            p.rotate(360 / self._count)
        p.end()

class LoadingOverlay(QWidget):
    def __init__(self, parent: QWidget):
        super().__init__(parent)
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setStyleSheet("background: rgba(0,0,0,80);")
        self.setVisible(False)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setAlignment(Qt.AlignCenter)
        self._box = QWidget(self)
        self._box.setAttribute(Qt.WA_StyledBackground, True)
        self._box.setStyleSheet("background: rgba(255,255,255,0.92); border-radius: 12px; padding: 16px 24px;")
        box_lay = QVBoxLayout(self._box)
        box_lay.setContentsMargins(20, 16, 20, 16)
        box_lay.setSpacing(10)
        self._spinner = Spinner(self._box, size=40, line_length=8, line_width=3)
        self._label = QLabel("Đang tải...", self._box)
        self._label.setAlignment(Qt.AlignCenter)
        box_lay.addWidget(self._spinner, 0, Qt.AlignHCenter)
        box_lay.addWidget(self._label, 0, Qt.AlignHCenter)
        lay.addWidget(self._box, 0, Qt.AlignCenter)

    def start(self, text: str = "Đang tải dữ liệu..."):
        self._label.setText(text)
        self._spinner.start()
        self.resize(self.parentWidget().size())
        self.show()

    def set_text(self, text: str):
        self._label.setText(text)

    def stop(self):
        self._spinner.stop()
        self.hide()