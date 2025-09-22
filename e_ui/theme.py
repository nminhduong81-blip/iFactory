from __future__ import annotations
from typing import Dict, Union, Optional, cast
import sys
from PySide6.QtCore import QObject, Signal
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication, QStyleFactory

ThemeStatusMap = Dict[Optional[str], str]
ThemeColors = Dict[str, Union[str, ThemeStatusMap]]
ACCENT_DEFAULT = "#0F6CBD"

def _win_accent_hex() -> Optional[str]:
    if sys.platform != "win32":
        return None
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\DWM") as k:
            val, _ = winreg.QueryValueEx(k, "AccentColor")
            r = val & 0xFF
            g = (val >> 8) & 0xFF
            b = (val >> 16) & 0xFF
            return f"#{r:02x}{g:02x}{b:02x}"
    except Exception:
        return None

def _edge_colors(mode: str) -> ThemeColors:
    accent = _win_accent_hex() or ACCENT_DEFAULT
    if mode == "dark":
        return {
            "background": "#0f1115",
            "surface": "#1c1f24",
            "surface_alt": "transparent",
            "text": "#f3f4f6",
            "text_alt": "#a3aab2",
            "primary": accent,
            "menu_border": "#2a2f36",
            "hover": "#232830",
            "button": "#242a31",
            "status": {
                "1": "#15ff00",
                "2": "#e2e2e1",
                "3": "#ff0000",
                "4": "#0066ff",
                "5": "#fffb00",
                None: "#00000000"
            },
        }
    return {
        "background": "#f6f7f9",
        "surface": "#ffffff",
        "surface_alt": "#f7f8fa",
        "text": "#1a1d21",
        "text_alt": "#5f6670",
        "primary": accent,
        "menu_border": "#e5e7eb",
        "hover": "#eef3fb",
        "button": "#f3f4f6",
        "status": {
            "1": "#15ff00",
            "2": "#e2e2e1",
            "3": "#ff0000",
            "4": "#0066ff",
            "5": "#fffb00",
            None: "#00000000"
        },
    }

THEMES: Dict[str, ThemeColors] = {
    "dark": _edge_colors("dark"),
    "light": _edge_colors("light"),
}

def make_palette(colors: ThemeColors) -> QPalette:
    bg = cast(str, colors["background"])
    text = cast(str, colors["text"])
    surface = cast(str, colors["surface"])
    surface_alt = cast(str, colors["surface_alt"])
    button = cast(str, colors["button"])
    primary = cast(str, colors["primary"])
    text_alt = cast(str, colors["text_alt"])
    menu_border = cast(str, colors["menu_border"])
    p = QPalette()
    p.setColor(QPalette.ColorRole.Window, QColor(bg))
    p.setColor(QPalette.ColorRole.WindowText, QColor(text))
    p.setColor(QPalette.ColorRole.Base, QColor(surface))
    p.setColor(QPalette.ColorRole.AlternateBase, QColor(surface_alt))
    p.setColor(QPalette.ColorRole.ToolTipBase, QColor(surface))
    p.setColor(QPalette.ColorRole.ToolTipText, QColor(text))
    p.setColor(QPalette.ColorRole.Text, QColor(text))
    p.setColor(QPalette.ColorRole.Button, QColor(button))
    p.setColor(QPalette.ColorRole.ButtonText, QColor(text))
    p.setColor(QPalette.ColorRole.Highlight, QColor(primary))
    p.setColor(QPalette.ColorRole.HighlightedText, QColor(text))
    p.setColor(QPalette.ColorRole.PlaceholderText, QColor(text_alt))
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(text_alt))
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(text_alt))
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, QColor(text_alt))
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Base, QColor(surface_alt))
    return p

def _hex_to_rgba(s: str, alpha: float) -> str:
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

def make_stylesheet(colors: ThemeColors) -> str:
    text_alt = cast(str, colors["text_alt"])
    accent = cast(str, colors["primary"])
    accent_08 = _hex_to_rgba(accent, 0.08)
    accent_14 = _hex_to_rgba(accent, 0.14)
    accent_20 = _hex_to_rgba(accent, 0.20)
    accent_28 = _hex_to_rgba(accent, 0.28)
    return f"""
    * {{
        font-family: "Segoe UI Variable", "Segoe UI", "Inter", "Arial";
        font-size: 13px;
        selection-background-color: {accent_28};
    }}
    QMainWindow {{
        background: {cast(str, colors['background'])};
    }}
    QStatusBar {{
        background: {cast(str, colors['surface'])};
        color: {text_alt};
        border-top: 1px solid {cast(str, colors['menu_border'])};
    }}
    QTabBar::tab {{
        background: transparent;
        color: {text_alt};
        padding: 6px 12px;
        min-height: 26px;
        border: none;
        border-top-left-radius: 8px;
        border-top-right-radius: 8px;
        margin-right: 4px;
    }}
    QTabBar::tab:hover {{
        background: {cast(str, colors['hover'])};
        color: {cast(str, colors['text'])};
    }}
    QTabBar::tab:selected {{
        background: {cast(str, colors['surface'])};
        color: {cast(str, colors['text'])};
        margin-bottom: -2px;
        border-bottom: 2px solid {cast(str, colors['primary'])};
    }}
    QTabWidget::pane {{
        background: {cast(str, colors['surface'])};
        border: 1px solid {cast(str, colors['menu_border'])};
        border-top: 0px;
        top: -2px;
        border-radius: 8px;
    }}
    QPushButton {{
        border-radius: 8px;
        padding: 6px 14px;
        background: {cast(str, colors['button'])};
        color: {cast(str, colors['text'])};
        border: 1px solid {cast(str, colors['menu_border'])};
    }}
    QPushButton:hover {{
        background: {cast(str, colors['hover'])};
    }}
    QPushButton:focus {{
        outline: none;
        border: 1px solid {cast(str, colors['primary'])};
        background: {accent_08};
    }}
    QToolButton {{
        border: none;
        background: transparent;
        padding: 4px 8px;
        border-radius: 6px;
        color: {cast(str, colors['text'])};
    }}
    QToolButton:hover {{
        background: {accent_08};
    }}
    QToolButton:pressed {{
        background: {accent_14};
    }}
    QToolButton#settingsBtn {{
        min-height: 26px;
        min-width: 32px;
        font-size: 16px;
        padding: 0 8px;
        margin-top: 0px;
        border-radius: 6px;
    }}
    QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox, QDateTimeEdit {{
        color: {cast(str, colors['text'])};
        background: {cast(str, colors['surface'])};
        border: 1px solid {cast(str, colors['menu_border'])};
        padding: 6px 8px;
        border-radius: 8px;
    }}
    QLineEdit::placeholder {{
        color: {text_alt};
    }}
    QLineEdit:focus, QComboBox:focus, QSpinBox:focus, QDoubleSpinBox:focus, QDateTimeEdit:focus {{
        border: 1px solid {cast(str, colors['primary'])};
        background: {accent_08};
    }}
    QMenu {{
        background: {cast(str, colors['surface'])};
        color: {cast(str, colors['text'])};
        border: 1px solid {cast(str, colors['menu_border'])};
        border-radius: 8px;
        padding: 6px 2px;
    }}
    QMenu::item {{
        padding: 6px 12px;
        border-radius: 6px;
        color: {cast(str, colors['text'])};
    }}
    QMenu::item:selected {{
        background: {accent_14};
        color: {cast(str, colors['text'])};
    }}
    QMenu::item:disabled {{
        color: {text_alt};
        background: transparent;
    }}
    QGraphicsView {{
        background: {cast(str, colors['surface'])};
        border: 0;
    }}
    QHeaderView::section {{
        background: {cast(str, colors['surface'])};
        color: {cast(str, colors['text'])};
        border: none;
        border-bottom: 1px solid {cast(str, colors['menu_border'])};
        padding: 6px 8px;
    }}
    QTableView {{
        gridline-color: {cast(str, colors['menu_border'])};
        selection-background-color: {accent_20};
        selection-color: {cast(str, colors['text'])};
        alternate-background-color: {cast(str, colors['surface_alt'])};
    }}
    QScrollBar:vertical {{
        background: transparent;
        width: 10px;
        margin: 2px;
        border: none;
    }}
    QScrollBar::handle:vertical {{
        background: {_hex_to_rgba(text_alt, 0.35)};
        border-radius: 5px;
        min-height: 30px;
    }}
    QScrollBar::handle:vertical:hover {{
        background: {_hex_to_rgba(text_alt, 0.55)};
    }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
        height: 0px;
        border: none; background: none;
    }}
    QScrollBar:horizontal {{
        background: transparent;
        height: 10px;
        margin: 2px;
        border: none;
    }}
    QScrollBar::handle:horizontal {{
        background: {_hex_to_rgba(text_alt, 0.35)};
        border-radius: 5px;
        min-width: 30px;
    }}
    QScrollBar::handle:horizontal:hover {{
        background: {_hex_to_rgba(text_alt, 0.55)};
    }}
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
        width: 0px;
        border: none; background: none;
    }}
    QToolTip {{
        background: {cast(str, colors['surface'])};
        color: {cast(str, colors['text'])};
        border: 1px solid {cast(str, colors['menu_border'])};
        padding: 4px 8px;
        border-radius: 6px;
    }}
    """

class ThemeBus(QObject):
    changed = Signal(str, object)

theme_bus = ThemeBus()
_current_theme_name = "light"

def apply_theme(app: QApplication, theme: str) -> None:
    global _current_theme_name
    theme = (theme or "light").lower()
    colors = THEMES.get(theme, THEMES["light"])
    app.setStyle(QStyleFactory.create("Fusion"))
    app.setPalette(make_palette(colors))
    app.setStyleSheet(make_stylesheet(colors))
    _current_theme_name = theme
    theme_bus.changed.emit(theme, colors)

def apply_mica(widget, dark: bool = False, kind: str = "tabbed") -> bool:
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        from ctypes import wintypes
        hwnd = int(widget.winId())
        dwmapi = ctypes.WinDLL("dwmapi")
        DWMWA_USE_IMMERSIVE_DARK_MODE = 20
        val = ctypes.c_int(1 if dark else 0)
        dwmapi.DwmSetWindowAttribute(wintypes.HWND(hwnd), ctypes.c_uint(DWMWA_USE_IMMERSIVE_DARK_MODE), ctypes.byref(val), ctypes.sizeof(val))
        DWMWA_SYSTEMBACKDROP_TYPE = 38
        kinds = {"main": 2, "transient": 3, "tabbed": 4}
        backdrop = ctypes.c_int(kinds.get(kind, 4))
        res = dwmapi.DwmSetWindowAttribute(wintypes.HWND(hwnd), ctypes.c_uint(DWMWA_SYSTEMBACKDROP_TYPE), ctypes.byref(backdrop), ctypes.sizeof(backdrop))
        DWMWA_WINDOW_CORNER_PREFERENCE = 33
        DWMWCP_ROUND = 2
        corner = ctypes.c_int(DWMWCP_ROUND)
        dwmapi.DwmSetWindowAttribute(wintypes.HWND(hwnd), ctypes.c_uint(DWMWA_WINDOW_CORNER_PREFERENCE), ctypes.byref(corner), ctypes.sizeof(corner))
        return res == 0
    except Exception:
        return False

def current_theme_name() -> str:
    return _current_theme_name

def theme_colors(name: Optional[str] = None) -> ThemeColors:
    n = (name or _current_theme_name).lower()
    return THEMES.get(n, THEMES["light"])