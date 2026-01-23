import asyncio
import aiohttp
import aiofiles
import re
from pathlib import Path
from typing import List, Callable


class DownloadCancelled(Exception):
    """Исключение для отмены загрузки"""
    pass


def expand_wildcard_url(template: str) -> List[str]:
    match = re.search(r'\{(\d+)\.\.(\d+)\}', template)
    if not match:
        raise ValueError("Шаблон должен содержать {start..end}")
    start_str, end_str = match.groups()
    start, end = int(start_str), int(end_str)
    if start > end:
        raise ValueError("Начало > конца")
    width = len(start_str) if start_str.startswith('0') and len(start_str) > 1 else 0
    return [
        template[:match.start()] + (str(i).zfill(width) if width else str(i)) + template[match.end()]
        for i in range(start, end + 1)
    ]


async def download_files(
    url_template: str,
    output_dir: str,
    max_concurrent: int = 10,
    chunk_size: int = 8192,
    on_start: Callable[[str], None] = None,
    on_progress: Callable[[str, int, int], None] = None,
    on_complete: Callable[[str, bool, str], None] = None,
    check_cancelled: Callable[[], bool] = None,  # НОВОЕ: функция проверки отмены
):
    urls = expand_wildcard_url(url_template)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for url in urls:
            if check_cancelled and check_cancelled():
                raise DownloadCancelled("Загрузка отменена пользователем")
            filename = url.split('/')[-1]
            if on_start:
                on_start(filename)
            task = _download_single(
                session, url, output_path, semaphore, chunk_size,
                filename, on_progress, on_complete, check_cancelled
            )
            tasks.append(task)
        await asyncio.gather(*tasks)


async def _download_single(
    session, url, output_path, semaphore, chunk_size,
    filename, on_progress, on_complete, check_cancelled
):
    filepath = output_path / filename
    try:
        async with semaphore:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}")
                total = resp.content_length or 0
                downloaded = 0
                async with aiofiles.open(filepath, 'wb') as f:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        if check_cancelled and check_cancelled():
                            raise DownloadCancelled("Отменено во время загрузки")
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(filename, downloaded, total)
                if on_complete:
                    on_complete(filename, True, "")
    except DownloadCancelled:
        # Удаляем частично загруженный файл
        try:
            filepath.unlink(missing_ok=True)
        except:
            pass
        if on_complete:
            on_complete(filename, False, "Отменено пользователем")
        raise
    except Exception as e:
        if on_complete:
            on_complete(filename, False, str(e))