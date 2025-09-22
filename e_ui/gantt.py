from __future__ import annotations
from typing import List, Optional, Tuple
from datetime import datetime, timedelta
from bisect import bisect_right
from PySide6.QtCore import Qt, QRectF, QSize, QPointF, Signal
from PySide6.QtGui import QColor, QPainter, QPen, QBrush, QFont, QCursor
from PySide6.QtWidgets import QWidget, QToolTip
from e_ui.theme import theme_bus, theme_colors, current_theme_name

def _fmt_dur(seconds: float) -> str:
    secs = int(max(0, seconds) + 0.5)
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    if h > 0:
        return f"{h}h {m}m"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

class GanttStrip(QWidget):
    segmentSelected = Signal(object)

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._theme_name = current_theme_name()
        self._code: Optional[str] = None
        self._segments: List[Tuple[datetime, datetime, Optional[str]]] = []
        self._starts: List[datetime] = []
        self._start: Optional[datetime] = None
        self._end: Optional[datetime] = None
        self._placeholder: str = "No device selected"
        self._hover_x: Optional[float] = None
        self._hover_t: Optional[datetime] = None
        self._hover_seg: Optional[Tuple[datetime, datetime, Optional[str]]] = None
        self._selected_seg: Optional[Tuple[datetime, datetime, Optional[str]]] = None
        self._show_now_line: bool = True
        self._show_segment_labels: bool = True
        self._show_axis: bool = True
        self._margin_l, self._margin_r, self._margin_t, self._margin_b = 16, 16, 10, 34
        self._area = QRectF()
        self._bar_h = 0.0
        self._bar_y = 0.0
        self._ticks: List[datetime] = []
        self._geom_dirty = True
        self._cursor_pointing = False
        self._tooltip_last = ""
        theme_bus.changed.connect(self._on_theme_changed)
        self.setAttribute(Qt.WA_OpaquePaintEvent, True)
        self.setMouseTracking(True)
        self.setMinimumHeight(120)

    def sizeHint(self) -> QSize:
        return QSize(480, 160)

    def _on_theme_changed(self, name: str, _):
        self._theme_name = name
        self.update()

    def set_placeholder(self, text: Optional[str] = None):
        if text is not None:
            self._placeholder = text
        self._code = None
        self._segments = []
        self._starts = []
        self._start = None
        self._end = None
        self._hover_x = None
        self._hover_t = None
        self._hover_seg = None
        self._selected_seg = None
        self._geom_dirty = True
        self._tooltip_last = ""
        self.update()

    def set_segments(self, code: str, segments: List[Tuple[datetime, datetime, Optional[str]]], day_start: datetime, day_end: datetime):
        self._code = code
        self._segments = segments[:]
        self._segments.sort(key=lambda x: x[0])
        self._starts = [s for s, _, _ in self._segments]
        self._start = day_start
        self._end = day_end
        self._hover_x = None
        self._hover_t = None
        self._hover_seg = None
        self._selected_seg = None
        self._geom_dirty = True
        self._tooltip_last = ""
        self.update()

    def set_axis_visible(self, visible: bool):
        self._show_axis = bool(visible)
        if self._show_axis:
            self._margin_t, self._margin_b = 10, 34
        else:
            self._margin_t, self._margin_b = 8, 8
        self._geom_dirty = True
        self.update()

    def _time_to_x(self, t: datetime, left: float, right: float) -> float:
        if not self._start or not self._end:
            return left
        total = (self._end - self._start).total_seconds() or 1.0
        pos = (t - self._start).total_seconds()
        pos = max(0.0, min(total, pos))
        return left + (right - left) * (pos / total)

    def _x_to_time(self, x: float, left: float, right: float) -> Optional[datetime]:
        if not self._start or not self._end or right <= left:
            return None
        ratio = (x - left) / (right - left)
        ratio = max(0.0, min(1.0, ratio))
        delta = (self._end - self._start).total_seconds()
        return self._start + timedelta(seconds=delta * ratio)

    def _status_color(self, status: Optional[str]) -> QColor:
        colors = theme_colors(self._theme_name)
        hexc = colors["status"].get(status, colors["status"].get(None, "#00000000"))
        return QColor(str(hexc))

    def _find_segment_at(self, t: datetime) -> Optional[Tuple[datetime, datetime, Optional[str]]]:
        if not self._segments:
            return None
        i = bisect_right(self._starts, t) - 1
        if i >= 0:
            s, e, st = self._segments[i]
            if s <= t < e:
                return s, e, st
        return None

    def _hours_step(self, width_px: float) -> int:
        min_px = 64.0
        step = max(1, int(min_px / max(1.0, width_px / 24.0)))
        for cand in (1, 2, 3, 4, 6, 8, 12):
            if step <= cand:
                return cand
        return step

    def _aligned_hour_ticks(self, step_hours: int) -> List[datetime]:
        if not self._start or not self._end:
            return []
        start = self._start
        end = self._end
        first = start.replace(minute=0, second=0, microsecond=0)
        if first < start:
            first += timedelta(hours=1)
        ticks = []
        t = first
        step = timedelta(hours=step_hours)
        while t <= end:
            ticks.append(t)
            t += step
        return ticks

    @staticmethod
    def _luminance(c: QColor) -> float:
        r, g, b = c.redF(), c.greenF(), c.blueF()
        return 0.2126 * r + 0.7152 * g + 0.0722 * b

    def _contrast_text_on(self, bg: QColor, default: QColor) -> QColor:
        return QColor(Qt.black) if self._luminance(bg) > 0.5 else QColor(Qt.white)

    def _segment_rect(self, s: datetime, e: datetime, area: QRectF, bar_y: float, bar_h: float) -> QRectF:
        x1 = self._time_to_x(s, area.left(), area.right())
        x2 = self._time_to_x(e, area.left(), area.right())
        if x2 <= area.left() or x1 >= area.right():
            return QRectF()
        x1 = max(x1, area.left())
        x2 = min(x2, area.right())
        w = max(1.0, x2 - x1)
        return QRectF(x1, bar_y, w, bar_h)

    def _recalc_geometry(self):
        self._area = QRectF(self._margin_l, self._margin_t, self.width() - self._margin_l - self._margin_r, self.height() - self._margin_t - self._margin_b)
        self._bar_h = self._area.height()
        self._bar_y = self._area.top()
        self._ticks = self._aligned_hour_ticks(self._hours_step(self._area.width()))
        self._geom_dirty = False

    def _set_cursor_pointing(self, pointing: bool):
        if self._cursor_pointing != pointing:
            self._cursor_pointing = pointing
            self.setCursor(QCursor(Qt.PointingHandCursor) if pointing else QCursor(Qt.ArrowCursor))

    def mouseMoveEvent(self, event):
        if not self._start or not self._end:
            return super().mouseMoveEvent(event)
        if self._geom_dirty:
            self._recalc_geometry()
        x = event.position().x() if hasattr(event, "position") else float(event.pos().x())
        area = self._area
        if area.contains(QPointF(x, area.center().y())):
            t = self._x_to_time(x, area.left(), area.right())
            prev_seg = self._hover_seg
            self._hover_x = x
            self._hover_t = t
            seg = self._find_segment_at(t) if t else None
            self._hover_seg = seg
            self._set_cursor_pointing(bool(seg))
            if t:
                ts = t.strftime("%H:%M")
                if seg:
                    s, e, st = seg
                    dur = (e - s).total_seconds()
                    text = f"{ts}\nStatus: {st or 'Unknown'}\n{s.strftime('%H:%M')} → {e.strftime('%H:%M')}  ({_fmt_dur(dur)})"
                else:
                    text = f"{ts}\nNo data"
                if text != self._tooltip_last:
                    gp = event.globalPosition().toPoint() if hasattr(event, "globalPosition") else event.globalPos()
                    QToolTip.showText(gp, text, self)
                    self._tooltip_last = text
            if seg != prev_seg:
                self.update()
        else:
            self._hover_x = None
            self._hover_t = None
            self._hover_seg = None
            self._set_cursor_pointing(False)
            if self._tooltip_last:
                QToolTip.hideText()
                self._tooltip_last = ""
            self.update()
        super().mouseMoveEvent(event)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton and self._hover_seg:
            self._selected_seg = self._hover_seg
            self.segmentSelected.emit(self._selected_seg)
            self.update()
        super().mousePressEvent(event)

    def leaveEvent(self, e):
        self._hover_x = None
        self._hover_t = None
        self._hover_seg = None
        self._set_cursor_pointing(False)
        if self._tooltip_last:
            QToolTip.hideText()
            self._tooltip_last = ""
        self.update()
        super().leaveEvent(e)

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._geom_dirty = True

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing, True)
        colors = theme_colors(self._theme_name)
        bg = QColor(str(colors["surface"]))
        text_col = QColor(str(colors["text"]))
        grid_col = QColor(str(colors["text_alt"]))
        grid_col.setAlpha(90 if self._theme_name == "light" else 110)
        p.fillRect(self.rect(), bg)
        if self._geom_dirty:
            self._recalc_geometry()
        area = self._area
        p.setPen(QPen(grid_col, 1))
        p.drawRect(area)
        if not self._start or not self._end or not self._code:
            p.setPen(QPen(grid_col, 1))
            f = QFont(p.font())
            f.setPointSizeF(max(10.0, f.pointSizeF()))
            p.setFont(f)
            p.drawText(self.rect(), Qt.AlignCenter, self._placeholder)
            p.end()
            return
        if self._show_axis:
            step_ticks = self._ticks
            f_tick = QFont(p.font())
            f_tick.setPointSizeF(max(9.0, f_tick.pointSizeF() - 1))
            p.setFont(f_tick)
            for t in step_ticks:
                x = self._time_to_x(t, area.left(), area.right())
                is_major = t.hour in (0, 12)
                pen = QPen(grid_col, 1.5 if is_major else 1, Qt.DotLine)
                p.setPen(pen)
                p.drawLine(x, area.top(), x, area.bottom())
                p.setPen(QPen(text_col, 1))
                txt = t.strftime("%H:%M")
                p.drawText(QRectF(x - 24, self.height() - self._margin_b + 4, 48, self._margin_b - 6), Qt.AlignHCenter | Qt.AlignTop, txt)
        if self._show_now_line:
            try:
                now = datetime.now()
                if self._start <= now <= self._end:
                    x_now = self._time_to_x(now, area.left(), area.right())
                    p.setPen(QPen(QColor(str(colors["primary"])), 1.5))
                    p.drawLine(x_now, area.top(), x_now, area.bottom())
            except Exception:
                pass
        bar_h = self._bar_h
        bar_y = self._bar_y
        cache_color: dict[Optional[str], QColor] = {}
        def cstatus(st: Optional[str]) -> QColor:
            if st not in cache_color:
                cache_color[st] = self._status_color(st)
            return cache_color[st]
        for s, e, st in self._segments:
            rect = self._segment_rect(s, e, area, bar_y, bar_h)
            if rect.isNull():
                continue
            col = cstatus(st)
            if col.alpha() == 0:
                continue
            fill = QColor(col)
            fill.setAlpha(160 if self._theme_name == "light" else 180)
            p.fillRect(rect, QBrush(fill))
        def draw_outline(rect: QRectF, color: QColor, width: float, dash: Optional[Tuple[int, int]] = None):
            pen = QPen(color, width)
            if dash:
                pen.setStyle(Qt.CustomDashLine)
                pen.setDashPattern(list(dash))
            p.setPen(pen)
            p.setBrush(Qt.NoBrush)
            radius = min(6.0, rect.height() * 0.2)
            p.drawRoundedRect(rect.adjusted(0.5, 0.5, -0.5, -0.5), radius, radius)
        if self._selected_seg:
            s, e, st = self._selected_seg
            rect = self._segment_rect(s, e, area, bar_y, bar_h)
            if not rect.isNull():
                draw_outline(rect, QColor(str(colors["primary"])), 2.0)
        if self._hover_seg and self._hover_seg != self._selected_seg:
            s, e, st = self._hover_seg
            rect = self._segment_rect(s, e, area, bar_y, bar_h)
            if not rect.isNull():
                c = QColor(str(colors["primary"]))
                c.setAlpha(180)
                draw_outline(rect, c, 1.5, dash=(4, 3))
        label_cap = 200
        if self._show_segment_labels and len(self._segments) <= label_cap:
            f2 = QFont(p.font())
            f2.setPointSizeF(max(8.5, f2.pointSizeF() - 1.5))
            p.setFont(f2)
            for s, e, st in self._segments:
                rect = self._segment_rect(s, e, area, bar_y, bar_h)
                if rect.isNull() or rect.width() < 56:
                    continue
                col = cstatus(st)
                if col.alpha() == 0:
                    continue
                txt_color = self._contrast_text_on(col, text_col)
                p.setPen(QPen(txt_color, 1))
                label = (st or "").strip() or f"{s.strftime('%H:%M')} → {e.strftime('%H:%M')}"
                p.drawText(rect.adjusted(4, 0, -4, 0), Qt.AlignVCenter | Qt.AlignLeft, label)
        if self._hover_x is not None:
            p.setPen(QPen(QColor(str(colors["primary"])), 1))
            p.drawLine(self._hover_x, area.top(), self._hover_x, area.bottom())
        p.end()

class StatusSummaryBar(QWidget):
    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._theme_name = current_theme_name()
        self._segments: List[Tuple[datetime, datetime, Optional[str]]] = []
        self._start: Optional[datetime] = None
        self._end: Optional[datetime] = None
        self._rects: List[Tuple[QRectF, Optional[str], float]] = []
        self._tooltip_last = ""
        theme_bus.changed.connect(self._on_theme_changed)
        self.setAttribute(Qt.WA_OpaquePaintEvent, True)
        self.setMouseTracking(True)
        self.setMinimumHeight(44)

    def sizeHint(self) -> QSize:
        return QSize(400, 48)

    def _on_theme_changed(self, name: str, _):
        self._theme_name = name
        self.update()

    def set_segments(self, segments: List[Tuple[datetime, datetime, Optional[str]]], day_start: datetime, day_end: datetime):
        self._segments = segments[:]
        self._start = day_start
        self._end = day_end
        self._recalc_rects()
        self.update()

    def _status_color(self, status: Optional[str]) -> QColor:
        colors = theme_colors(self._theme_name)
        hexc = colors["status"].get(status, colors["status"].get(None, "#00000000"))
        return QColor(str(hexc))

    def _recalc_rects(self):
        self._rects.clear()
        if not self._start or not self._end or not self._segments:
            return
        total = (self._end - self._start).total_seconds()
        if total <= 0:
            return
        duration_map: dict[Optional[str], float] = {}
        for s, e, st in self._segments:
            a = max(s, self._start)
            b = min(e, self._end)
            if b > a:
                duration_map[st] = duration_map.get(st, 0.0) + (b - a).total_seconds()
        if not duration_map:
            return
        items = sorted(duration_map.items(), key=lambda kv: kv[1], reverse=True)
        start_x = 8.0
        end_x = max(start_x + 1.0, self.width() - 8.0)
        w = end_x - start_x
        h = max(1.0, self.height() - 16.0)
        for st, dur in items:
            percent = (dur / total) if total > 0 else 0.0
            rect_w = max(1.0, w * percent)
            r = QRectF(start_x, 8.0, rect_w, h)
            self._rects.append((r, st, percent))
            start_x += rect_w

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._recalc_rects()

    def mouseMoveEvent(self, event):
        if not self._rects:
            return super().mouseMoveEvent(event)
        pos = event.position() if hasattr(event, "position") else QPointF(event.pos())
        for r, st, pct in self._rects:
            if r.contains(pos):
                pct_txt = f"{pct*100:.1f}%"
                total = (self._end - self._start).total_seconds() if self._start and self._end else 0
                dur_txt = _fmt_dur(total * pct) if total else "-"
                text = f"Status: {st or 'Unknown'}\n{dur_txt}  ({pct_txt})"
                if text != self._tooltip_last:
                    gp = event.globalPosition().toPoint() if hasattr(event, "globalPosition") else event.globalPos()
                    QToolTip.showText(gp, text, self)
                    self._tooltip_last = text
                break
        super().mouseMoveEvent(event)

    def leaveEvent(self, e):
        if self._tooltip_last:
            QToolTip.hideText()
            self._tooltip_last = ""
        super().leaveEvent(e)

    def paintEvent(self, e):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing, True)
        colors = theme_colors(self._theme_name)
        bg = QColor(str(colors["surface"]))
        grid_col = QColor(str(colors["text_alt"]))
        grid_col.setAlpha(90 if self._theme_name == "light" else 110)
        p.fillRect(self.rect(), bg)
        area = QRectF(8, 8, self.width() - 16, self.height() - 16)
        p.setPen(QPen(grid_col, 1))
        p.drawRect(area)
        if not self._rects:
            p.end()
            return
        cache_color: dict[Optional[str], QColor] = {}
        def cstatus(st: Optional[str]) -> QColor:
            if st not in cache_color:
                cache_color[st] = self._status_color(st)
            return cache_color[st]
        for r, st, pct in self._rects:
            col = cstatus(st)
            if col.alpha() == 0:
                continue
            fill = QColor(col)
            fill.setAlpha(170 if self._theme_name == "light" else 190)
            p.fillRect(r, QBrush(fill))
        p.end()