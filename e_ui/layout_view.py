from __future__ import annotations
from typing import Dict, Optional, List, Any, cast, Tuple
from datetime import datetime, timezone
from pathlib import Path
import json
import asyncio
from PySide6.QtCore import Qt, QPointF, Signal, QRectF
from PySide6.QtGui import QBrush, QColor, QPen, QPainter
from PySide6.QtWidgets import QGraphicsView, QGraphicsScene, QGraphicsRectItem, QGraphicsItemGroup, QGraphicsItem, QMenu, QStyleFactory, QGraphicsColorizeEffect, QGraphicsSimpleTextItem
from PySide6.QtSvgWidgets import QGraphicsSvgItem
from qasync import asyncSlot
from e_ui.theme import THEMES, ThemeColors, ThemeStatusMap

def _normalize_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

def _fmt_relative(dt: Optional[datetime]) -> str:
    dt = _normalize_dt(dt)
    if not dt:
        return "-"
    try:
        now = datetime.now()
        s = int((now - dt).total_seconds())
        if s < 60:
            return f"{s}s ago"
        m = s // 60
        if m < 60:
            return f"{m}m ago"
        h = m // 60
        if h < 24:
            return f"{h}h {m % 60}m ago"
        d = h // 24
        return f"{d}d {h % 24}h ago"
    except Exception:
        return "-"

class DeviceGroup(QGraphicsItemGroup):
    def __init__(self, path: str, code: str, name: str) -> None:
        super().__init__()
        self.code = code
        self.name = name
        self.current_status: Optional[str] = None
        self.latest_time: Optional[datetime] = None
        self.svg_item = QGraphicsSvgItem(path)
        self.svg_item.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
        rect = self.svg_item.boundingRect()
        if rect.isEmpty():
            rect = QRectF(0, 0, 64, 64)
        padding = 10
        box_rect = rect.adjusted(-padding, -padding, padding, padding)
        self.box_item = QGraphicsRectItem(box_rect)
        self.box_item.setPen(Qt.NoPen)
        self.box_item.setBrush(Qt.NoBrush)
        self.border_item = QGraphicsRectItem(box_rect)
        self.border_item.setPen(Qt.NoPen)
        self.border_item.setBrush(Qt.NoBrush)
        self.badge_bg = QGraphicsRectItem()
        self.badge_bg.setPen(Qt.NoPen)
        self.badge_bg.setBrush(Qt.NoBrush)
        self.badge_bg.setVisible(False)
        self.text_item = QGraphicsSimpleTextItem("")
        self.text_item.setVisible(False)
        self.box_item.setZValue(0)
        self.svg_item.setZValue(1)
        self.badge_bg.setZValue(2)
        self.text_item.setZValue(3)
        self.border_item.setZValue(4)
        self.addToGroup(self.box_item)
        self.addToGroup(self.svg_item)
        self.addToGroup(self.badge_bg)
        self.addToGroup(self.text_item)
        self.addToGroup(self.border_item)
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptHoverEvents(True)
        self.current_theme = "light"
        self._status_color_func = None
        self._text_color = QColor("black")
        self._update_tooltip()

    def _lighter(self, c: QColor, factor: int = 160) -> QColor:
        return c.lighter(factor)

    def _update_tooltip(self):
        st = self.current_status or "Unknown"
        iso = self.latest_time.isoformat() if self.latest_time else "-"
        rel = _fmt_relative(self.latest_time)
        self.setToolTip(f"{self.code} • {self.name}\nStatus: {st}\nUpdated: {iso} ({rel})\nDouble-click: reset view\nRight-click: actions")

    def _update_border(self, hovered: bool = False):
        pen = QPen(Qt.NoPen)
        if self.isSelected():
            c = self._text_color if self._text_color.isValid() else QColor("white")
            pen = QPen(c, 2.2)
        elif hovered:
            fill = self.box_item.brush().color() if self.box_item.brush().style() != Qt.NoBrush else QColor(128, 128, 128, 80)
            pen = QPen(self._lighter(fill, 160), 2.0)
        self.border_item.setPen(pen)

    def hoverEnterEvent(self, event):
        self._update_border(hovered=True)
        super().hoverEnterEvent(event)

    def hoverLeaveEvent(self, event):
        self._update_border(hovered=False)
        super().hoverLeaveEvent(event)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            self._update_border(hovered=False)
        return super().itemChange(change, value)

    def update_status(self, status: Optional[str], color: Optional[QColor], latest_time: Optional[datetime]) -> None:
        norm_time = _normalize_dt(latest_time)
        changed = (status != self.current_status) or (norm_time != self.latest_time)
        if not changed:
            return
        self.current_status = status
        self.latest_time = norm_time
        if color and isinstance(color, QColor) and color.isValid():
            prev_brush = self.box_item.brush()
            prev = prev_brush.color() if prev_brush.style() != Qt.NoBrush else None
            if not prev or prev != color:
                self.box_item.setBrush(QBrush(color))
        else:
            if self.box_item.brush().style() != Qt.NoBrush:
                self.box_item.setBrush(Qt.NoBrush)
        self._update_border(hovered=False)
        self._update_tooltip()

    def set_theme(self, theme: str, status_color_func, text_color: Optional[QColor] = None) -> None:
        self.current_theme = theme
        self._status_color_func = status_color_func
        if text_color is not None:
            self._text_color = text_color
        color = status_color_func(self.current_status) if status_color_func else None
        self.update_status(self.current_status, color, self.latest_time)

    def update_input_count(self, count: Optional[int], text_color: Optional[QColor] = None) -> None:
        if text_color is not None:
            self._text_color = text_color
        if count and count > 0:
            self.text_item.setText(str(count))
            self.text_item.setBrush(QBrush(self._text_color))
            rect = self.svg_item.boundingRect()
            tb = self.text_item.boundingRect()
            pad_x, pad_y = 6, 2
            w = tb.width() + 2 * pad_x
            h = tb.height() + 2 * pad_y
            x = rect.center().x() - w / 2
            y = rect.top() - h - 8
            bg = QColor(0, 0, 0, 170) if self.current_theme == "dark" else QColor(255, 255, 255, 200)
            self.badge_bg.setRect(QRectF(x, y, w, h))
            self.badge_bg.setBrush(QBrush(bg))
            self.text_item.setPos(x + pad_x, y + pad_y - 1)
            self.text_item.setVisible(True)
            self.badge_bg.setVisible(True)
        else:
            self.text_item.setVisible(False)
            self.badge_bg.setVisible(False)

class LayoutView(QGraphicsView):
    requestStatus = Signal(str)
    requestWip = Signal(str)
    requestEip = Signal(str)
    requestInput = Signal(str)
    deviceSelected = Signal(str)
    syncScaleRequested = Signal(float)
    MIN_SCALE = 0.2
    MAX_SCALE = 5.0

    def __init__(self, layout_json: str, tab_filter: str, status_service, theme: str) -> None:
        super().__init__()
        self.service = status_service
        self.tab_filter = tab_filter
        self.theme = theme.lower()
        self._scene = QGraphicsScene(self)
        self.setScene(self._scene)
        self.devices: Dict[str, DeviceGroup] = {}
        self.bg_item: Optional[QGraphicsSvgItem] = None
        self._current_scale = 1.0
        self.setRenderHints(self.renderHints() | QPainter.Antialiasing | QPainter.SmoothPixmapTransform | QPainter.TextAntialiasing)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorViewCenter)
        self.setDragMode(QGraphicsView.NoDrag)
        self.setMouseTracking(True)
        self.setViewportUpdateMode(QGraphicsView.SmartViewportUpdate)
        self._load_layout(layout_json)

    def _status_color(self, v: Optional[str]) -> QColor:
        colors = cast(ThemeColors, THEMES.get(self.theme, THEMES["light"]))
        status_map = cast(ThemeStatusMap, colors["status"])
        return QColor(status_map.get(v, status_map.get(None, "#80808040")))

    def _text_color(self) -> QColor:
        colors = THEMES.get(self.theme, THEMES["light"])
        return QColor(colors["text"])

    def _to_datetime(self, v) -> Optional[datetime]:
        if isinstance(v, datetime):
            return _normalize_dt(v)
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
                return _normalize_dt(dt)
            except Exception:
                return None
        return None

    def _build_status_map(self, rows: List[Dict[str, Any]]) -> Dict[str, Tuple[Optional[str], Optional[datetime]]]:
        m: Dict[str, Tuple[Optional[str], Optional[datetime]]]= {}
        for r in rows:
            code = r.get("equip_code")
            if not code:
                continue
            status_code = r.get("equip_status")
            dt = self._to_datetime(r.get("as_of") or r.get("event_time"))
            m[code] = (status_code, dt)
        return m

    @asyncSlot()
    async def refresh_devices(self) -> None:
        try:
            codes = list(self.devices.keys())
            rows = await self.service.get_latest(codes)
            status_map = self._build_status_map(rows)
            for dev in self.devices.values():
                status_code, dt = status_map.get(dev.code, (None, None))
                dev.update_status(status_code, self._status_color(status_code), dt)
        except asyncio.CancelledError:
            return

    def apply_hot_data(self, status_rows: List[Dict[str, Any]]):
        status_map = self._build_status_map(status_rows)
        for dev in self.devices.values():
            status_code, dt = status_map.get(dev.code, (None, None))
            dev.update_status(status_code, self._status_color(status_code), dt)

    def apply_input_data(self, input_rows: List[Dict[str, Any]]):
        input_map: Dict[str, Optional[int]] = {}
        for r in input_rows:
            c = r.get("equip_code")
            if not c:
                continue
            mb = r.get("material_batch")
            input_map[c] = 1 if mb else None
        txt = self._text_color()
        for dev in self.devices.values():
            count = input_map.get(dev.code)
            dev.update_input_count(int(count) if count is not None else None, txt)

    def _load_layout(self, layout_json: str) -> None:
        p = Path(layout_json)
        base_dir = p.parent if p.parent.exists() else Path.cwd()
        colors = THEMES.get(self.theme, THEMES["light"])
        self.setBackgroundBrush(QBrush(QColor(colors["surface"])))
        data: dict = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        floor = data.get("floors", {}).get(self.tab_filter, {}) if data else {}
        self._load_background(base_dir, floor.get("background"))
        if self._scene.sceneRect().isEmpty():
            self._scene.setSceneRect(0, 0, 1600, 900)
        items = floor.get("devices") or floor.get("items") or []
        for it in items:
            code = str(it.get("code") or it.get("equip_code") or it.get("id") or "")
            name = str(it.get("name") or it.get("label") or code)
            img_decl = str(it.get("image") or f"{code}.svg")
            img_path = self._resolve_path(base_dir / "devices", img_decl, fallback=f"{code}.svg")
            dev = DeviceGroup(img_path, code, name)
            dev.set_theme(self.theme, self._status_color, self._text_color())
            x = float(it.get("x") or (it.get("pos") or {}).get("x") or 0)
            y = float(it.get("y") or (it.get("pos") or {}).get("y") or 0)
            dev.setPos(QPointF(x, y))
            self._scene.addItem(dev)
            self.devices[code] = dev
        self.fitInView(self._scene.sceneRect(), Qt.KeepAspectRatio)
        self._current_scale = self.transform().m11()

    def _load_background(self, base_dir: Path, bg_file: Optional[str]) -> None:
        if not bg_file:
            return
        path = self._resolve_path(base_dir, bg_file)
        if path:
            bg = QGraphicsSvgItem(path)
            bg.setZValue(0.0)
            bg.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
            self.bg_item = bg
            self._scene.addItem(bg)
            self._scene.setSceneRect(bg.boundingRect())

    def _resolve_path(self, base_dir: Path, rel_or_abs: str, fallback: Optional[str] = None) -> str:
        q = Path(rel_or_abs)
        candidates = [q, Path("e_ui/assets/layout") / q, Path("e_ui/assets/devices") / q, base_dir / q]
        if fallback:
            candidates.append(Path("e_ui/assets/devices") / fallback)
            candidates.append(base_dir / fallback)
        for c in candidates:
            if c.exists():
                return str(c.resolve())
        return ""

    def mouseMoveEvent(self, event):
        sp = self.mapToScene(event.pos())
        items = self._scene.items(QRectF(sp.x() - 1, sp.y() - 1, 2, 2))
        self.setCursor(Qt.PointingHandCursor if any(isinstance(it, DeviceGroup) for it in items) else Qt.ArrowCursor)
        super().mouseMoveEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self.reset_view()
        super().mouseDoubleClickEvent(event)

    def reset_view(self) -> None:
        self.fitInView(self._scene.sceneRect(), Qt.KeepAspectRatio)
        self._current_scale = self.transform().m11()

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            sp = self.mapToScene(event.pos())
            items = self._scene.items(QRectF(sp.x() - 2, sp.y() - 2, 4, 4))
            target: Optional[DeviceGroup] = next((it for it in items if isinstance(it, DeviceGroup)), None)
            if target:
                for dev in self.devices.values():
                    dev.setSelected(False)
                target.setSelected(True)
                self.deviceSelected.emit(target.code)
            else:
                self.setDragMode(QGraphicsView.ScrollHandDrag)
        elif event.button() == Qt.MiddleButton:
            self.setDragMode(QGraphicsView.ScrollHandDrag)
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() in (Qt.LeftButton, Qt.MiddleButton):
            self.setDragMode(QGraphicsView.NoDrag)
        super().mouseReleaseEvent(event)

    def contextMenuEvent(self, event):
        sp = self.mapToScene(event.pos())
        items = self._scene.items(QRectF(sp.x() - 2, sp.y() - 2, 4, 4))
        target: Optional[DeviceGroup] = next((it for it in items if isinstance(it, DeviceGroup)), None)
        if target:
            self._show_device_menu(target, event.globalPos())

    def _show_device_menu(self, target: DeviceGroup, global_pos):
        m = QMenu(self)
        m.setStyle(QStyleFactory.create("Fusion"))
        a1 = m.addAction("📊 Status")
        a2 = m.addAction("📦 WIP")
        a3 = m.addAction("🌐 EIP")
        a5 = m.addAction("📥 Input")
        act = m.exec(global_pos)
        if act == a1:
            self.requestStatus.emit(target.code)
        elif act == a2:
            self.requestWip.emit(target.code)
        elif act == a3:
            self.requestEip.emit(target.code)
        elif act == a5:
            self.requestInput.emit(target.code)

    def wheelEvent(self, event) -> None:
        delta = event.angleDelta().y()
        if delta == 0:
            return
        factor = 1.15 if delta > 0 else 1 / 1.15
        new_scale = self._current_scale * factor
        new_scale = max(self.MIN_SCALE, min(self.MAX_SCALE, new_scale))
        factor = new_scale / self._current_scale
        self.scale(factor, factor)
        self._current_scale = new_scale
        self.syncScaleRequested.emit(self._current_scale)

    def apply_scale(self, scale_value: float) -> None:
        scale_value = max(self.MIN_SCALE, min(self.MAX_SCALE, scale_value))
        self.resetTransform()
        self.scale(scale_value, scale_value)
        self._current_scale = scale_value

    def fit_all(self) -> None:
        self.fitInView(self._scene.sceneRect(), Qt.KeepAspectRatio)
        self._current_scale = self.transform().m11()

    def fit_selected(self) -> None:
        sel = [it for it in self.devices.values() if it.isSelected()]
        if not sel:
            self.fit_all()
            return
        rect = sel[0].mapToScene(sel[0].childrenBoundingRect()).boundingRect()
        for it in sel[1:]:
            rect = rect.united(it.mapToScene(it.childrenBoundingRect()).boundingRect())
        if rect.isValid() and not rect.isEmpty():
            self.fitInView(rect.adjusted(-20, -20, 20, 20), Qt.KeepAspectRatio)
            self._current_scale = self.transform().m11()

    def set_theme(self, theme: str) -> None:
        self.theme = theme.lower()
        colors = THEMES.get(self.theme, THEMES["light"])
        self.setBackgroundBrush(QBrush(QColor(colors["surface"])))
        for dev in self.devices.values():
            dev.set_theme(self.theme, self._status_color, self._text_color())
        if self.bg_item:
            if self.theme == "dark":
                eff = QGraphicsColorizeEffect()
                eff.setColor(QColor("white"))
                self.bg_item.setGraphicsEffect(eff)
            else:
                self.bg_item.setGraphicsEffect(None)
        self.fit_all()