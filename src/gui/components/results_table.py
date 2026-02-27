from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
)
from PyQt6.QtCore import Qt

from ..utils.chart_data import ChartStore, series_label

HEADERS = [
    "СУБД / Метод",
    "Строк",
    "Время (сек)",
    "Строк/сек",
    "Частота (1/сек)",
    "Batch size",
]

# Индексы числовых колонок — сортируются как float, не как строка
NUMERIC_COLUMNS = {1, 2, 3, 4, 5}


class NumericItem(QTableWidgetItem):
    """QTableWidgetItem с корректной числовой сортировкой."""

    def __lt__(self, other: QTableWidgetItem) -> bool:
        try:
            return float(self.text()) < float(other.text())
        except ValueError:
            return self.text() < other.text()


class ResultsTableWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._table = QTableWidget()
        self._table.setColumnCount(len(HEADERS))
        self._table.setHorizontalHeaderLabels(HEADERS)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setAlternatingRowColors(True)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.verticalHeader().setVisible(False)

        # Включаем сортировку по клику на заголовок
        self._table.setSortingEnabled(True)

        # Состояние сортировки: col -> следующий порядок
        # None = дефолт (порядок вставки), затем Descending, Ascending, None...
        self._sort_state: dict[int, Qt.SortOrder | None] = {}
        self._current_sort_col: int | None = None

        header = self._table.horizontalHeader()
        header.setSectionsClickable(True)
        header.sectionClicked.connect(self._on_header_clicked)

        # Отключаем встроенную сортировку — управляем вручную для трёх состояний
        self._table.setSortingEnabled(False)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._table)
        self.setLayout(layout)

        self._store: ChartStore = {}

    def refresh(self, store: ChartStore):
        self._store = store
        self._fill_table()

    def _fill_table(self):
        # Запоминаем текущую сортировку чтобы восстановить после перезаполнения
        sort_col = self._current_sort_col
        sort_order = self._sort_state.get(sort_col) if sort_col is not None else None

        self._table.setRowCount(0)

        for db_type, methods in self._store.items():
            for method, runs in methods.items():
                for run in runs:
                    row = self._table.rowCount()
                    self._table.insertRow(row)

                    label = series_label(db_type, method, run)
                    batch_text = (
                        str(run.batch_size) if run.batch_size is not None else "—"
                    )
                    frequency = round(1 / run.elapsed, 3) if run.elapsed > 0 else 0

                    self._table.setItem(row, 0, self._cell(label, numeric=False))
                    self._table.setItem(row, 1, self._cell(str(run.rows), numeric=True))
                    self._table.setItem(
                        row, 2, self._cell(f"{run.elapsed:.3f}", numeric=True)
                    )
                    self._table.setItem(row, 3, self._cell(str(run.rps), numeric=True))
                    self._table.setItem(
                        row, 4, self._cell(str(frequency), numeric=True)
                    )
                    self._table.setItem(row, 5, self._cell(batch_text, numeric=True))

        # Восстанавливаем сортировку после перезаполнения
        if sort_col is not None and sort_order is not None:
            self._table.sortItems(sort_col, sort_order)
            self._update_indicator(sort_col, sort_order)

    def _on_header_clicked(self, col: int):
        """
        Три состояния по кругу:
        None (дефолт) → Descending → Ascending → None → ...
        """
        current = self._sort_state.get(col)

        if current is None:
            next_order = Qt.SortOrder.DescendingOrder
        elif current == Qt.SortOrder.DescendingOrder:
            next_order = Qt.SortOrder.AscendingOrder
        else:
            next_order = None  # сброс в порядок вставки

        # Сбрасываем состояние других колонок
        self._sort_state = {col: next_order}
        self._current_sort_col = col if next_order is not None else None

        if next_order is not None:
            self._table.sortItems(col, next_order)
            self._update_indicator(col, next_order)
        else:
            # Сброс — перезаполняем без сортировки
            self._table.horizontalHeader().setSortIndicatorShown(False)
            self._fill_table()

    def _update_indicator(self, col: int, order: Qt.SortOrder):
        header = self._table.horizontalHeader()
        header.setSortIndicatorShown(True)
        header.setSortIndicator(col, order)

    def clear(self):
        self._store = {}
        self._sort_state = {}
        self._current_sort_col = None
        self._table.setRowCount(0)
        self._table.horizontalHeader().setSortIndicatorShown(False)

    @staticmethod
    def _cell(text: str, numeric: bool = False) -> QTableWidgetItem:
        item = NumericItem(text) if numeric else QTableWidgetItem(text)
        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        return item
