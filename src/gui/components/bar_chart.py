from PyQt6.QtWidgets import QWidget, QVBoxLayout, QToolTip
from PyQt6.QtCharts import (
    QChart,
    QChartView,
    QBarSeries,
    QBarSet,
    QBarCategoryAxis,
    QValueAxis,
)
from PyQt6.QtGui import QPainter, QCursor
from PyQt6.QtCore import Qt
from .chart_legend import ChartLegend

from ..utils.chart_data import ChartStore, get_latest


class BarChartWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._chart = QChart()
        self._chart.setTitle("Пропускная способность методов вставки (строк/сек)")
        self._chart.setAnimationOptions(QChart.AnimationOption.SeriesAnimations)
        self._chart.legend().setVisible(True)
        self._chart.legend().setAlignment(Qt.AlignmentFlag.AlignBottom)

        self._view = QChartView(self._chart)
        self._view.setRenderHint(QPainter.RenderHint.Antialiasing)

        self._legend = ChartLegend()

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._view)
        layout.addWidget(self._legend)
        self.setLayout(layout)

        # db_types нужен в слоте для формирования подписи
        self._db_types: list[str] = []

    def refresh(self, store: ChartStore):
        self._chart.removeAllSeries()
        for ax in self._chart.axes():
            self._chart.removeAxis(ax)

        latest = get_latest(store)
        if not latest:
            return

        self._db_types = list(latest.keys())

        all_methods: dict[str, QBarSet] = {}
        for db_type, methods in latest.items():
            for method_key, run in methods.items():
                label = self._method_label(method_key)
                if label not in all_methods:
                    all_methods[label] = QBarSet(label)

        for label, bar_set in all_methods.items():
            for db_type in self._db_types:
                methods = latest.get(db_type, {})
                value = next(
                    (
                        run.rps
                        for method_key, run in methods.items()
                        if self._method_label(method_key) == label
                    ),
                    0.0,
                )
                bar_set.append(value)

        series = QBarSeries()
        for bar_set in all_methods.values():
            series.append(bar_set)

        # Подключаем сигнал наведения
        series.hovered.connect(self._on_hovered)

        self._chart.addSeries(series)
        self._legend.rebuild(self._chart)

        axis_x = QBarCategoryAxis()
        axis_x.append(self._db_types)
        axis_x.setTitleText("СУБД")
        self._chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        series.attachAxis(axis_x)

        axis_y = QValueAxis()
        axis_y.setTitleText("Строк/сек")
        axis_y.setLabelFormat("%.0f")
        axis_y.setMin(0)
        self._chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        series.attachAxis(axis_y)

    def _on_hovered(self, status: bool, index: int, bar_set: QBarSet):
        """
        status  — True при наведении, False при уходе курсора
        index   — индекс категории на оси X (соответствует СУБД)
        bar_set — столбец, на который навели (его имя = метод)
        """
        if not status:
            QToolTip.hideText()
            return

        db_type = self._db_types[index] if index < len(self._db_types) else "?"
        method = bar_set.label()
        rps = bar_set.at(index)

        QToolTip.showText(
            QCursor.pos(),
            f"<b>{method}</b><br>" f"СУБД: {db_type}<br>" f"Строк/сек: {rps:,.0f}",
            self._view,
        )

    def clear(self):
        self._chart.removeAllSeries()
        self._db_types = []
        self._legend.rebuild(self._chart)

    @staticmethod
    def _method_label(method_key: str) -> str:
        if method_key.startswith("bulk_insert:"):
            batch = method_key.split(":")[1]
            return f"bulk_insert (batch={batch})"
        return method_key
