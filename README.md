# File Downloader
A powerful CLI file downloader with wildcarded names

## 🚀 Releases
The stable version is **[v1.0.0](https://github.com/dvbarinov/downloader/releases/tag/v1.0.0)**.

### ✅ Key Features

- **Wildcard URL templates**  
  Download batches using intuitive pattern.

- **Asynchronous & concurrent downloads**  
  Uses `aiohttp` + `asyncio` for maximum I/O efficiency (no threading overhead).

- **YAML configuration support**  
  Define timeouts, retries, concurrency, and output paths in `config.yaml`.

- **Smart retry logic**  
  Automatic retries on failure (configurable attempts and delay).

- **Rich real-time UI**  
  - Individual progress bars for each file  
  - Grouped status panels: **📥 In Progress**, **✅ Completed**, **❌ Failed**  

- **Graceful shutdown**  
  Handles `Ctrl+C` without corrupting files or breaking the terminal.

- **Logging & diagnostics**  
  Detailed logs saved to `download.log` for post-mortem analysis.

## Tecnologies
- Python

## Launch
```bash
# Uses config.yaml by default
python download_files.py

# Или указать свой конфиг
python download_files.py my_config.yaml
```

## 🔗 Integration
Works perfectly with **[File Generator](https://github.com/dvbarinov/generator)** for end-to-end testing:
1. Generate 1000 test files with realistic size distribution
2. Serve them via HTTP
3. Download and validate with Smart Downloader
