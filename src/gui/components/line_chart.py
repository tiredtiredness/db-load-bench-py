from PyQt6.QtWidgets import QWidget, QVBoxLayout
from PyQt6.QtCharts import QChart, QChartView, QLineSeries, QValueAxis
from PyQt6.QtGui import QPainter
from PyQt6.QtCore import Qt

from ..utils.chart_data import ChartStore, series_label
from .chart_legend import ChartLegend


class LineChartWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._chart = QChart()
        self._chart.setTitle("Масштабируемость методов")
        self._chart.setAnimationOptions(QChart.AnimationOption.SeriesAnimations)
        self._chart.legend().setVisible(False)

        view = QChartView(self._chart)
        view.setRenderHint(QPainter.RenderHint.Antialiasing)

        self._legend = ChartLegend()

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(view)
        layout.addWidget(self._legend)
        self.setLayout(layout)

    def refresh(self, store: ChartStore):
        self._chart.removeAllSeries()
        for ax in self._chart.axes():
            self._chart.removeAxis(ax)

        if not store:
            return

        axis_x = QValueAxis()
        axis_x.setTitleText("Количество строк")
        axis_x.setLabelFormat("%d")
        axis_x.setMin(0)

        axis_y = QValueAxis()
        axis_y.setTitleText("Время вставки (сек)")
        axis_y.setMin(0)

        self._chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)

        for db_type, methods in store.items():
            for method, runs in methods.items():
                if not runs:
                    continue

                if method == "bulk_insert":
                    by_batch: dict = {}
                    for run in runs:
                        by_batch.setdefault(run.batch_size, []).append(run)

                    for _, batch_runs in by_batch.items():
                        series = QLineSeries()
                        series.setName(series_label(db_type, method, batch_runs[0]))
                        series.append(0, 0)
                        for run in sorted(batch_runs, key=lambda r: r.rows):
                            series.append(run.rows, run.elapsed)
                        self._chart.addSeries(series)
                        series.attachAxis(axis_x)
                        series.attachAxis(axis_y)
                else:
                    series = QLineSeries()
                    series.setName(f"{db_type} / {method}")
                    series.append(0, 0)
                    for run in sorted(runs, key=lambda r: r.rows):
                        series.append(run.rows, run.elapsed)
                    self._chart.addSeries(series)
                    series.attachAxis(axis_x)
                    series.attachAxis(axis_y)

        self._legend.rebuild(self._chart)

    def clear(self):
        self._chart.removeAllSeries()
        self._legend.rebuild(self._chart)
