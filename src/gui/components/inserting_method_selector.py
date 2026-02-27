from PyQt6.QtWidgets import QWidget, QFormLayout, QComboBox
from PyQt6.QtCore import pyqtSignal


INSERT_METHODS = {
    "Default Insert": "default_insert",
    "Bulk Insert": "bulk_insert",
    "File Insert": "file_insert",
}


class InsertingMethodSelector(QWidget):
    method_changed = pyqtSignal(str)
    log_message = pyqtSignal(str, str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.combo = QComboBox()
        self.combo.addItems(INSERT_METHODS.keys())
        self.combo.currentTextChanged.connect(self._on_changed)

        layout = QFormLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addRow("Метод вставки", self.combo)
        self.setLayout(layout)

    def _on_changed(self, label: str):
        self.method_changed.emit(INSERT_METHODS[label])
        self.log_message.emit(f"Выбран метод: {INSERT_METHODS[label]}", "INFO")

    def get_method(self) -> str:
        """Возвращает имя метода для вызова через getattr(db, method)."""
        return INSERT_METHODS[self.combo.currentText()]

    @staticmethod
    def register(label: str, method: str):
        """Регистрирует новый метод без изменения класса."""
        INSERT_METHODS[label] = method
