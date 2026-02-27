from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QSizePolicy
from PyQt6.QtGui import QColor
from PyQt6.QtCore import Qt
from PyQt6.QtCharts import QChart


class ChartLegend(QWidget):
    """Легенда с автоматическим переносом строк."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._layout = _WrapLayout(spacing=8)
        self.setLayout(self._layout)

    def rebuild(self, chart: QChart):
        # Очищаем старые элементы
        while self._layout.count():
            item = self._layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for series in chart.series():
            for marker in chart.legend().markers(series):
                color = marker.brush().color()
                label = marker.label()
                self._layout.addWidget(_LegendItem(label, color))


class _LegendItem(QWidget):
    def __init__(self, label: str, color: QColor, parent=None):
        super().__init__(parent)

        dot = QLabel("●")
        dot.setStyleSheet(f"color: {color.name()}; font-size: 14px;")
        dot.setFixedWidth(16)

        text = QLabel(label)
        text.setWordWrap(False)

        layout = QHBoxLayout()
        layout.setContentsMargins(4, 2, 8, 2)
        layout.setSpacing(4)
        layout.addWidget(dot)
        layout.addWidget(text)
        self.setLayout(layout)


class _WrapLayout(QVBoxLayout):
    """
    PyQt6 не имеет FlowLayout из коробки.
    Используем QWidget с resizeEvent для переноса элементов.
    """

    def __init__(self, spacing=4):
        super().__init__()
        self.setSpacing(spacing)
        self.setContentsMargins(0, 0, 0, 0)
        self.setAlignment(Qt.AlignmentFlag.AlignLeft)
