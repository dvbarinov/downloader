# main.py
import sys
import asyncio
import threading
from pathlib import Path
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QTabWidget, QTextEdit, QLabel, QProgressBar,
    QScrollArea, QFrame
)
from PySide6.QtCore import Qt, Signal, QObject, QTimer
from PySide6.QtGui import QFont
from downloader import download_files


class DownloaderSignals(QObject):
    """Сигналы для общения между потоками"""
    file_started = Signal(str)
    file_progress = Signal(str, int, int)
    file_finished = Signal(str, bool, str)


class DownloadManager:
    def __init__(self, signals: DownloaderSignals):
        self.signals = signals
        self.progress_bars = {}  # filename -> QProgressBar
        self.labels = {}         # filename -> QLabel (статус)

    def on_file_start(self, filename: str):
        self.signals.file_started.emit(filename)

    def on_file_progress(self, filename: str, done: int, total: int):
        self.signals.file_progress.emit(filename, done, total)

    def on_file_complete(self, filename: str, success: bool, error: str):
        self.signals.file_finished.emit(filename, success, error)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Файловый загрузчик")
        self.resize(800, 600)

        # Центральный виджет
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # Поле ввода и кнопка
        input_layout = QHBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Введите шаблон, например: https://example.com/data_{1..5}.csv")
        self.start_btn = QPushButton("Запустить загрузку")
        self.start_btn.clicked.connect(self.start_download)
        input_layout.addWidget(self.url_input)
        input_layout.addWidget(self.start_btn)
        layout.addLayout(input_layout)

        # Вкладки
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Вкладка "Загрузки"
        self.download_widget = QWidget()
        self.download_layout = QVBoxLayout(self.download_widget)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_area.setWidget(self.scroll_content)
        self.download_layout.addWidget(self.scroll_area)
        self.tabs.addTab(self.download_widget, "Загрузки")

        # Вкладка "Логи"
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.tabs.addTab(self.log_text, "Логи")

        # Сигналы и менеджер
        self.signals = DownloaderSignals()
        self.download_manager = DownloadManager(self.signals)
        self.setup_connections()

        # Флаг занятости
        self.is_downloading = False

    def setup_connections(self):
        self.signals.file_started.connect(self.add_file_entry)
        self.signals.file_progress.connect(self.update_progress)
        self.signals.file_finished.connect(self.mark_finished)

    def log(self, message: str):
        self.log_text.append(message)

    def add_file_entry(self, filename: str):
        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame_layout = QHBoxLayout(frame)

        label = QLabel(filename)
        label.setFixedWidth(200)
        progress = QProgressBar()
        progress.setRange(0, 100)
        progress.setValue(0)

        self.scroll_layout.addWidget(frame)
        frame_layout.addWidget(label)
        frame_layout.addWidget(progress)

        self.download_manager.progress_bars[filename] = progress
        self.download_manager.labels[filename] = label

        self.log(f"📥 Начата загрузка: {filename}")

    def update_progress(self, filename: str, done: int, total: int):
        if filename in self.download_manager.progress_bars:
            progress = self.download_manager.progress_bars[filename]
            if total > 0:
                percent = int((done / total) * 100)
                progress.setValue(percent)
            else:
                # Неизвестный размер — режим "бегущей точки"
                progress.setRange(0, 0)  # неопределённый прогресс

    def mark_finished(self, filename: str, success: bool, error: str):
        if filename in self.download_manager.labels:
            label = self.download_manager.labels[filename]
            if success:
                label.setStyleSheet("color: green; font-weight: bold;")
                self.log(f"✅ Успешно: {filename}")
            else:
                label.setStyleSheet("color: red; font-weight: bold;")
                self.log(f"❌ Ошибка: {filename} → {error}")

        if filename in self.download_manager.progress_bars:
            progress = self.download_manager.progress_bars[filename]
            progress.setRange(0, 100)
            progress.setValue(100 if success else 0)

    def start_download(self):
        if self.is_downloading:
            return
        template = self.url_input.text().strip()
        if not template:
            self.log("⚠️ Шаблон не задан!")
            return

        self.is_downloading = True
        self.start_btn.setEnabled(False)
        self.log(f"🚀 Запуск загрузки по шаблону: {template}")

        # Запуск в фоновом потоке
        thread = threading.Thread(
            target=self.run_async_download,
            args=(template,),
            daemon=True
        )
        thread.start()

    def run_async_download(self, template: str):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                download_files(
                    url_template=template,
                    output_dir="./downloads",
                    max_concurrent=10,
                    on_start=self.download_manager.on_file_start,
                    on_progress=self.download_manager.on_file_progress,
                    on_complete=self.download_manager.on_file_complete,
                )
            )
        finally:
            loop.close()
            # Вернуться в основной поток для обновления UI
            QTimer.singleShot(0, self.download_finished)

    def download_finished(self):
        self.is_downloading = False
        self.start_btn.setEnabled(True)
        self.log("🏁 Все загрузки завершены!")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())