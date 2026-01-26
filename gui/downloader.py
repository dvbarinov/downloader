import asyncio
import aiohttp
import aiofiles
import re
import json
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
        template[:match.start()] + (str(i).zfill(width) if width else str(i)) + template[match.end():]
        for i in range(start, end + 1)
    ]


def get_meta_path(filepath: Path) -> Path:
    return filepath.parent / f".{filepath.name}.meta"


async def download_files(
    url_template: str,
    output_dir: str,
    max_concurrent: int = 10,
    chunk_size: int = 8192,
    on_start: Callable[[str], None] = None,
    on_progress: Callable[[str, int, int], None] = None,
    on_complete: Callable[[str, bool, str], None] = None,
    check_cancelled: Callable[[], bool] = None,
    resume: bool = True,
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
                filename, on_progress, on_complete, check_cancelled, resume=resume
            )
            tasks.append(task)
        await asyncio.gather(*tasks)


async def _download_single(
    session, url, output_path, semaphore, chunk_size,
    filename, on_progress, on_complete, check_cancelled, resume=True
):
    filepath = output_path / filename
    meta_path = get_meta_path(filepath)
    server_size = None
    local_size = 0

    try:
        # Шаг 1: Получаем размер файла на сервере
        async with session.head(url) as head_resp:
            if head_resp.status != 200:
                #raise Exception(f"HEAD failed: {head_resp.status}")
                accepts_ranges = False  # без размера — не можем возобновить
            else:
                server_size = head_resp.content_length
                accepts_ranges = 'bytes' in head_resp.headers.get('Accept-Ranges', '').lower()

        if server_size is None:
            accepts_ranges = False  # без размера — не можем возобновить

        # Шаг 2: Проверяем локальный файл
        if filepath.exists():
            local_size = filepath.stat().st_size
            if resume and accepts_ranges and local_size < server_size:
                # Возобновляем
                headers = {'Range': f'bytes={local_size}-'}
                mode = 'ab'
                downloaded = local_size
                if on_progress:
                    on_progress(filename, downloaded, server_size)
            elif local_size == server_size:
                # Уже загружен
                if on_complete:
                    on_complete(filename, True, "Уже загружен")
                return
            else:
                # Невозможно возобновить — перезаписываем
                accepts_ranges = False
                local_size = 0
                mode = 'wb'
                downloaded = 0
        else:
            # Новый файл
            mode = 'wb'
            downloaded = 0

        # Сохраняем метаданные
        meta_data = {'server_size': server_size, 'url': url}
        async with aiofiles.open(meta_path, 'w') as mf:
            await mf.write(json.dumps(meta_data))

        # Шаг 3: Загружаем
        headers = {'Range': f'bytes={downloaded}-'} if (accepts_ranges and downloaded > 0) else {}
        async with semaphore:
            async with session.get(url, headers=headers) as resp:
                expected_status = 206 if (headers and accepts_ranges) else 200
                if resp.status != expected_status:
                    raise Exception(f"HTTP {resp.status} (ожидался {expected_status})")

                total = server_size or resp.content_length or 0
                async with aiofiles.open(filepath, mode) as f:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        if check_cancelled and check_cancelled():
                            raise DownloadCancelled("Отменено во время загрузки")
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(filename, downloaded, total or downloaded)

                if on_complete:
                    on_complete(filename, True, "")

    except DownloadCancelled:
        # Не удаляем файл при отмене — чтобы можно было возобновить!
        if on_complete:
            on_complete(filename, False, "Отменено (можно возобновить)")
        raise
    except Exception as e:
        # При ошибке удаляем метафайл, чтобы не мешать следующей попытке
        try:
            meta_path.unlink(missing_ok=True)
        except:
            pass
        if on_complete:
            on_complete(filename, False, str(e))
