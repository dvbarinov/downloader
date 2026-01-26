import asyncio
import aiohttp
import aiofiles
import re
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
import yaml
from tenacity import retry, stop_after_attempt, wait_fixed
#from tqdm.asyncio import tqdm  # tqdm поддерживает asyncio напрямую
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.progress import (
    Progress,
    TaskID,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    DownloadColumn,
    SpinnerColumn,
)
from rich.table import Table


console = Console()

# Глобальные списки для отслеживания состояния
completed_files = []
failed_files = []  # хранит кортежи (filename, error_message)
active_tasks: dict[int, str] = {}  # task_id -> filename


def setup_logging(config: Dict[str, Any]) -> None:
    log_level = getattr(logging, config.get("level", "INFO").upper())
    log_file = config.get("file", "download.log")
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            # logging.StreamHandler(sys.stdout) # лучше не выводить в консоль из-за прогрессбара
        ]
    )


def expand_wildcard_url(template: str) -> List[str]:
    """
    Преобразует 'https://ex.com/file_{1..3}.csv' →
    ['https://ex.com/file_1.csv', ..., 'https://ex.com/file_3.csv']
    """
    match = re.search(r'\{(\d+)\.\.(\d+)\}', template)
    if not match:
        raise ValueError("Шаблон должен содержать {start..end}, например {1..10}")

    start_str, end_str = match.groups()
    start, end = int(start_str), int(end_str)
    if start > end:
        raise ValueError("Начало диапазона не может быть больше конца")

    # Определяем ширину формата (для ведущих нулей)
    width = len(start_str) if start_str.startswith('0') and len(start_str) > 1 else 0
    urls = []
    for i in range(start, end + 1):
        repl = str(i).zfill(width) if width else str(i)
        url = template[:match.start()] + repl + template[match.end():]
        urls.append(url)
    return urls


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    reraise=True
)
async def fetch_content(session: aiohttp.ClientSession, url: str) -> bytes:
    async with session.get(url) as resp:
        if resp.status == 200:
            return await resp.read()
        else:
            raise aiohttp.ClientResponseError(
                request_info=resp.request_info,
                history=resp.history,
                status=resp.status,
                message=f"HTTP {resp.status}",
                headers=resp.headers
            )


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    chunk_size: int,
    retries_enabled: bool,
    max_attempts: int,
    delay: float,
    progress: Progress,
    task_id: TaskID,
    filename: str,
):
    """Скачивает один файл с ограничением параллелизма"""
    global completed_files, failed_files, active_tasks
    async with semaphore:  # ограничиваем одновременные запросы
        try:
            filepath = output_dir / filename

            if retries_enabled:
                # Патчим retry-декоратор под текущие настройки
                @retry(
                    stop=stop_after_attempt(max_attempts),
                    wait=wait_fixed(delay),
                    reraise=True
                )
                async def _download():
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=resp.status,
                                message=f"HTTP {resp.status}",
                                headers=resp.headers
                            )
                        # Получаем общий размер, если есть
                        total = resp.content_length or 1
                        if total is None or total == 0:
                            # Можно создать задачу без total → будет неопределённый прогресс
                            progress.start_task(task_id)
                            # Но BarColumn всё равно не заполнится — это нормально
                        else:
                            progress.update(task_id, total=total, refresh=True)

                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                await f.write(chunk)
                                progress.update(task_id, advance=len(chunk), refresh=True)
                await _download()
                completed_files.append(filename)
                logging.info(f"✅ Успешно: {url} → {filepath}")
            else:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        total = resp.content_length or 1
                        progress.update(task_id, total=total, refresh=True)

                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                await f.write(chunk)
                                progress.update(task_id, advance=len(chunk), refresh=True)
                        completed_files.append(filename)
                        logging.info(f"✅ Успешно: {url} → {filepath}")
                    else:
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=resp.status,
                            message=f"HTTP {resp.status}",
                            headers=resp.headers
                        )
        except Exception as e:
            error_msg = str(e)[:80]  # укоротим длинные ошибки
            failed_files.append((filename, error_msg))
            logging.error(f"❌ Ошибка при загрузке {url}: {e}")
        finally:
            # Удаляем из активных
            if task_id in active_tasks:
                del active_tasks[task_id]

    progress.update(task_id, visible=False)
    return True  # важно для as_completed


def make_status_display(progress: Progress) -> Table:
    """Создаёт таблицу с тремя секциями: активные, завершённые, ошибки"""
    table = Table.grid(expand=True)
    table.add_column(ratio=1)

    # Активные задачи — используем сам объект Progress
    if active_tasks:
        table.add_row(Panel(progress, title=f"📥 В процессе ({len(active_tasks)})", border_style="blue"))
    else:
        table.add_row(Text("📥 В процессе (0)", style="blue"))

    # Завершённые
    if completed_files:
        completed_text = Text("\n".join(f"• {f}" for f in sorted(completed_files[-20:])))  # последние 20
        table.add_row(Panel(completed_text, title=f"✅ Завершено ({len(completed_files)})", border_style="green"))
    else:
        table.add_row(Text("✅ Завершено (0)", style="green"))

    # Ошибки
    if failed_files:
        failed_text = Text("\n".join(f"• {f} → {err}" for f, err in failed_files[-10:]))  # последние 10
        table.add_row(Panel(failed_text, title=f"❌ Ошибки ({len(failed_files)})", border_style="red"))
    else:
        table.add_row(Text("❌ Ошибки (0)", style="red"))

    return table


async def download_all(config: Dict[str, Any]):
    """Основная функция загрузки"""
    global completed_files, failed_files, active_tasks
    completed_files.clear()
    failed_files.clear()
    active_tasks.clear()

    dl_cfg = config["download"]
    http_cfg = config["http"]
    urls = expand_wildcard_url(dl_cfg["url_template"])
    output_path = Path(dl_cfg["output_dir"])
    output_path.mkdir(parents=True, exist_ok=True)

    timeout = aiohttp.ClientTimeout(
        total=http_cfg["timeout"]["total"],
        connect=http_cfg["timeout"]["connect"]
    )

    semaphore = asyncio.Semaphore(dl_cfg.get("max_concurrent", 10))
    chunk_size = dl_cfg.get("chunk_size", 8192)

    retry_cfg = http_cfg.get("retries", {})
    retries_enabled = retry_cfg.get("enabled", False)
    max_attempts = retry_cfg.get("max_attempts", 3)
    delay = retry_cfg.get("delay", 1.0)

    # Настройка Rich Progress (только для активных задач)
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        #"{task.completed}-{task.total}",
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
        console=console,
        expand=True,
        auto_refresh=True
        # auto_refresh=False  # обновляем вручную через Live
    )

    # Инициализируем Live-рендер
    with Live(make_status_display(progress), refresh_per_second=5, console=console) as live:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks: List[asyncio.Task[Any]] = []
            for url in urls:
                filename = url.split('/')[-1]
                task_id = progress.add_task("download", filename=filename, start=False)
                active_tasks[task_id] = filename
                coro = download_file(
                    session, url, output_path, semaphore, chunk_size,
                    retries_enabled, max_attempts, delay,
                    progress, task_id, filename
                )
                tasks.append(asyncio.create_task(coro))
                # Обновляем отображение после добавления задачи
                live.update(make_status_display(progress))

            # Ждём завершения всех задач
            # ❗ ВАЖНО: не используем gather, а обрабатываем по одной
            for completed_task in asyncio.as_completed(tasks):
                await completed_task  # ждём завершения одной задачи
                live.update(make_status_display(progress))  # ← обновляем интерфейс


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "config.yaml"

    # if len(sys.argv) < 2:
    #     print("Использование: python download_wildcard.py 'https://example.com/data_{1..5}.csv'")
    #     sys.exit(1)

    config = load_config(config_path)
    setup_logging(config.get("logging", {}))
    try:
        asyncio.run(download_all(config))
        logging.info("✅ Все файлы загружены!")
    except KeyboardInterrupt:
        logging.warning("⚠️ Загрузка прервана пользователем (Ctrl+C)")
        print("\n\n🛑 Загрузка остановлена.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
