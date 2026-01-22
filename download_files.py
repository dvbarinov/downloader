# download_wildcard.py
import asyncio
import aiohttp
import aiofiles
import re
from pathlib import Path
from typing import List, Optional


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
        if width:
            repl = str(i).zfill(width)
        else:
            repl = str(i)
        url = template[:match.start()] + repl + template[match.end():]
        urls.append(url)
    return urls


async def download_file(session: aiohttp.ClientSession, url: str, output_dir: Path, semaphore: asyncio.Semaphore):
    """Скачивает один файл с ограничением параллелизма"""
    async with semaphore:  # ограничиваем одновременные запросы
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    filename = url.split('/')[-1]
                    filepath = output_dir / filename
                    async with aiofiles.open(filepath, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
                    print(f"✅ {url} → {filepath}")
                else:
                    print(f"❌ Ошибка {resp.status} при загрузке {url}")
        except Exception as e:
            print(f"⚠️  Не удалось загрузить {url}: {e}")


async def download_all(template: str, output_dir: str = "./downloads", max_concurrent: int = 10):
    """Основная функция загрузки"""
    urls = expand_wildcard_url(template)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Ограничиваем количество одновременных соединений
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            download_file(session, url, output_path, semaphore)
            for url in urls
        ]
        await asyncio.gather(*tasks)


# Пример использования
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Использование: python download_wildcard.py 'https://example.com/data_{1..5}.csv'")
        sys.exit(1)

    template = sys.argv[1]
    asyncio.run(download_all(template, output_dir="./downloads", max_concurrent=10))