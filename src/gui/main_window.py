from PyQt6.QtWidgets import QWidget, QHBoxLayout, QVBoxLayout, QMainWindow, QPushButton
from .widgets import LogWidget, ConfigWidget, ResultsWidget
from .workers import InsertWorker


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("DB Load Bench")

        self.config_widget = ConfigWidget()
        self.results_widget = ResultsWidget()
        self.log_widget = LogWidget()
        self.run_btn = QPushButton("▶ Запустить")

        self.run_btn.clicked.connect(self._on_run_clicked)
        self.config_widget.log_message.connect(self.log_widget.log)

        container = QWidget()
        container.setMinimumSize(1000, 400)

        main_layout = QHBoxLayout(container)
        main_layout.addWidget(self.config_widget)

        right_layout = QVBoxLayout()
        right_layout.addWidget(self.results_widget)
        right_layout.addWidget(self.log_widget)
        right_layout.addWidget(self.run_btn)

        main_layout.addLayout(right_layout)

        self.setCentralWidget(container)

    def _on_run_clicked(self):
        config = self.config_widget.get_config()

        if not config["csv_file"]:
            self.log_widget.log("Не выбран CSV файл", "ERROR")
            return
        if not config["conn_params"]["database"]:
            self.log_widget.log("Не указана база данных", "ERROR")
            return

        self.worker = InsertWorker(config)
        self.worker.log_message.connect(self.log_widget.log)
        # self.worker.finished.connect(self.results_widget.update_results)
        self.worker.finished.connect(lambda: self.run_btn.setEnabled(True))
        self.worker.error.connect(lambda: self.run_btn.setEnabled(True))

        self.run_btn.setEnabled(False)
        self.worker.start()
