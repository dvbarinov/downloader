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
from tqdm.asyncio import tqdm  # tqdm поддерживает asyncio напрямую


def setup_logging(config: Dict[str, Any]) -> None:
    log_level = getattr(logging, config.get("level", "INFO").upper())
    log_file = config.get("file", "download.log")
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            #logging.StreamHandler(sys.stdout) # лучше не выводить в консоль из-за прогрессбара
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
        delay: float
):
    """Скачивает один файл с ограничением параллелизма"""
    async with semaphore:  # ограничиваем одновременные запросы
        try:
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
                        filename = url.split('/')[-1]
                        filepath = output_dir / filename
                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                await f.write(chunk)
                        return filepath

                filepath = await _download()
                logging.info(f"✅ Успешно: {url} → {filepath}")
            else:
                # Без повторов — простая загрузка
                async with session.get(url) as resp:
                    if resp.status == 200:
                        filename = url.split('/')[-1]
                        filepath = output_dir / filename
                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                await f.write(chunk)
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
            logging.error(f"❌ Ошибка при загрузке {url}: {e}")

    return True  # важно для as_completed


async def download_all(config: Dict[str, Any]):
    """Основная функция загрузки"""
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

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Создаём задачи
        tasks = [
            download_file(
                session, url, output_path, semaphore, chunk_size,
                retries_enabled, max_attempts, delay
            )
            for url in urls
        ]

        # Используем tqdm.as_completed для отслеживания прогресса
        for coro in tqdm.as_completed(tasks, total=len(tasks), desc="Загрузка файлов"):
            await coro  # дожидаемся завершения каждой задачи


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
    asyncio.run(download_all(config))

if __name__ == "__main__":
    main()