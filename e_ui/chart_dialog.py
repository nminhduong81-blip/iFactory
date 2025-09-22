from __future__ import annotations
from typing import Dict, List, Optional
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QBrush, QPainter, QCursor
from PySide6.QtWidgets import QDialog, QVBoxLayout, QWidget, QToolTip
from PySide6.QtCharts import QChart, QChartView, QStackedBarSeries, QBarSet, QBarCategoryAxis, QValueAxis

class ColumnChartDialog(QDialog):
    def __init__(self, parent: QWidget, title: str, categories: List[str], 
                 data: Dict[str, List[float]], ordered_statuses: List[str], 
                 unit: str, bucket_totals: List[float], 
                 color_map: Dict[str, QColor], theme: str = "light"):
        super().__init__(parent)
        self.setWindowTitle(title)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 8, 8, 8)
        chart = QChart()
        chart.setTitle(title)
        series = QStackedBarSeries()
        for st in ordered_statuses:
            vals = data.get(st, [0.0] * len(categories))
            bs = QBarSet(st or "Unknown")
            bs.append(vals)
            col = color_map.get(st)
            if col is not None:
                bs.setColor(col)
                fill = QColor(col)
                fill.setAlpha(220 if theme == "light" else 200)
                bs.setBrush(QBrush(fill))
                bs.setBorderColor(col.darker(120))
            series.append(bs)
        chart.addSeries(series)
        axisX = QBarCategoryAxis()
        axisX.append(categories)
        axisX.setLabelsAngle(-90)
        chart.addAxis(axisX, Qt.AlignBottom)
        series.attachAxis(axisX)
        axisY = QValueAxis()
        axisY.setLabelFormat("%.0f" if unit == "min" else "%.1f")
        axisY.setTitleText("Minutes" if unit == "min" else "Hours")
        ymax = max(bucket_totals) if bucket_totals else (60.0 if unit == "min" else 24.0)
        if ymax <= 0:
            ymax = 1.0
        axisY.setRange(0, ymax)
        chart.addAxis(axisY, Qt.AlignLeft)
        series.attachAxis(axisY)
        chart.legend().setVisible(True)
        chart.legend().setAlignment(Qt.AlignBottom)
        view = QChartView(chart)
        view.setRenderHint(QPainter.Antialiasing, True)
        lay.addWidget(view)
        
        def on_hover(status: bool, index: int, barset: QBarSet):
            if not status:
                QToolTip.hideText()
                return
            st = barset.label()
            val = barset.at(index)
            total = bucket_totals[index] if 0 <= index < len(bucket_totals) else 0.0
            pct = (val / total * 100.0) if total > 0 else 0.0
            if unit == "min":
                txt_val = f"{val:.0f} min"
            else:
                minutes = int(round(val * 60))
                h = minutes // 60
                m = minutes % 60
                txt_val = f"{h}h {m}m" if h > 0 else f"{m}m"
            cat = categories[index] if 0 <= index < len(categories) else ""
            tip = f"{cat}\n{st}: {txt_val} ({pct:.1f}%)"
            QToolTip.showText(QCursor.pos(), tip, self)
        
        series.hovered.connect(on_hover)
        self.setWindowFlags(self.windowFlags() | Qt.Window | Qt.WindowMinMaxButtonsHint | Qt.WindowCloseButtonHint)
        self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)
        self.setSizeGripEnabled(True)