import sys
import asyncio
import threading
from pathlib import Path

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QTabWidget, QTextEdit, QLabel, QProgressBar,
    QScrollArea, QFrame, QCheckBox
)
from PySide6.QtCore import Signal, QObject, QTimer
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
        self.labels = {}  # filename -> QLabel (статус)

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

        # Состояние отмены
        self._cancelled = False
        self._download_thread = None

        # Центральный виджет
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # Поле ввода и кнопки
        input_layout = QHBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("https://example.com/data_{1..5}.csv")
        self.start_btn = QPushButton("Запустить")
        self.cancel_btn = QPushButton("Отменить")
        self.cancel_btn.setEnabled(False)
        self.start_btn.clicked.connect(self.start_download)
        self.cancel_btn.clicked.connect(self.cancel_download)
        self.resume_checkbox = QCheckBox("Возобновлять загрузку")
        self.resume_checkbox.setChecked(True)
        self.clear_btn = QPushButton("Очистить частичные")
        self.clear_btn.clicked.connect(self.clear_partial_downloads)
        input_layout.addWidget(self.url_input)
        input_layout.addWidget(self.start_btn)
        input_layout.addWidget(self.cancel_btn)
        input_layout.addWidget(self.resume_checkbox)
        input_layout.addWidget(self.clear_btn)
        layout.addLayout(input_layout)

        # Вкладки (без изменений)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self.download_widget = QWidget()
        self.download_layout = QVBoxLayout(self.download_widget)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_area.setWidget(self.scroll_content)
        self.download_layout.addWidget(self.scroll_area)
        self.tabs.addTab(self.download_widget, "Загрузки")

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.tabs.addTab(self.log_text, "Логи")

        self.signals = DownloaderSignals()
        self.download_manager = DownloadManager(self.signals)
        self.setup_connections()

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
        frame_layout.addWidget(label)
        frame_layout.addWidget(progress)
        self.scroll_layout.addWidget(frame)
        self.download_manager.progress_bars[filename] = progress
        self.download_manager.labels[filename] = label
        self.log(f"📥 Начата загрузка: {filename}")

    def update_progress(self, filename: str, done: int, total: int):
        if filename in self.download_manager.progress_bars:
            pb = self.download_manager.progress_bars[filename]
            if total > 0:
                pb.setRange(0, 100)
                pb.setValue(int((done / total) * 100))
            else:
                pb.setRange(0, 0)

    def mark_finished(self, filename: str, success: bool, error: str):
        if filename in self.download_manager.labels:
            label = self.download_manager.labels[filename]
            color = "green" if success else "red"
            label.setStyleSheet(f"color: {color}; font-weight: bold;")
            status = "✅" if success else "❌"
            self.log(f"{status} {filename}: {error if not success else 'готово'}")

        if filename in self.download_manager.progress_bars:
            pb = self.download_manager.progress_bars[filename]
            pb.setRange(0, 100)
            pb.setValue(100 if success else 0)

    def start_download(self):
        if self._download_thread and self._download_thread.is_alive():
            return
        template = self.url_input.text().strip()
        if not template:
            self.log("⚠️ Шаблон не задан!")
            return

        self._cancelled = False
        self.start_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.log(f"🚀 Запуск загрузки: {template}")

        self._download_thread = threading.Thread(
            target=self.run_async_download,
            args=(template,),
            daemon=True
        )
        self._download_thread.start()

    def cancel_download(self):
        self._cancelled = True
        self.cancel_btn.setEnabled(False)
        self.log("🛑 Запрошена отмена загрузки...")

    def is_cancelled(self):
        return self._cancelled

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
                    check_cancelled=self.is_cancelled,
                    resume=self.resume_checkbox.isChecked(),
                )
            )
        except Exception as e:
            if "Отменено" in str(e):
                QTimer.singleShot(0, lambda: self.log("⏹️ Загрузка отменена."))
            else:
                QTimer.singleShot(0, lambda: self.log(f"💥 Критическая ошибка: {e}"))
        finally:
            loop.close()
            QTimer.singleShot(0, self.download_finished)

    def download_finished(self):
        self.start_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        if not self._cancelled:
            self.log("🏁 Все загрузки завершены!")

    def clear_partial_downloads(self):
        """Удаляет все файлы, у которых есть .meta — считаем их незавершёнными"""
        output_dir = Path("./downloads")
        if not output_dir.exists():
            self.log("📂 Папка загрузок пуста.")
            return

        deleted_files = []
        for meta_file in output_dir.glob(".*.meta"):
            if not meta_file.is_file():
                continue
            # Имя оригинального файла: убираем точку в начале и .meta в конце
            orig_name = meta_file.name[1:-5]  # например: ".file.csv.meta" → "file.csv"
            orig_path = output_dir / orig_name

            try:
                # Удаляем оригинальный файл и мета
                if orig_path.exists():
                    orig_path.unlink()
                meta_file.unlink()
                deleted_files.append(orig_name)
            except Exception as e:
                self.log(f"⚠️ Не удалось удалить {orig_name}: {e}")

        if deleted_files:
            msg = f"🗑️ Удалено частичных загрузок: {len(deleted_files)}\n• " + "\n• ".join(deleted_files)
            self.log(msg)
            # Очистим список в GUI (опционально — можно перезапустить вкладку)
            self.clear_download_list()
        else:
            self.log("✅ Нет частичных загрузок для удаления.")

        self.start_btn.setEnabled(True)

    def clear_download_list(self):
        """Очищает список файлов в GUI-вкладке 'Загрузки'"""
        while self.scroll_layout.count():
            child = self.scroll_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        self.download_manager.progress_bars.clear()
        self.download_manager.labels.clear()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
