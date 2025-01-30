# RAGFlow Uploader

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Testing: Pytest](https://img.shields.io/badge/testing-pytest-yellow)](https://docs.pytest.org/)
[![Linting: Ruff](https://img.shields.io/badge/linting-ruff-red)](https://github.com/astral-sh/ruff)
[![Type Checking: MyPy](https://img.shields.io/badge/type%20checking-mypy-blue)](https://mypy.readthedocs.io/)

A robust file uploader and processor for RAGFlow with advanced progress tracking. This tool provides an efficient, asynchronous solution for managing file uploads to RAGFlow datasets with real-time progress monitoring and error handling.

## 🚀 Features

- **Asynchronous Architecture**
  - High-performance file processing using `aiofiles`
  - Concurrent upload handling
  - Non-blocking progress tracking

- **Advanced File Management**
  - Automatic file type detection using `python-magic`
  - Real-time file system monitoring with `watchdog`
  - Robust error handling and retry mechanisms

- **Progress Tracking**
  - Real-time upload progress with `tqdm`
  - System resource monitoring via `psutil`
  - Human-readable file sizes and timestamps

- **Developer Experience**
  - Type hints throughout codebase
  - Comprehensive test coverage
  - Modern tooling integration (Black, Ruff, MyPy)

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- [UV](https://github.com/astral-sh/uv) (recommended) or Poetry
- RAGFlow API Key

### Quick Start with UV (Recommended)
```bash
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate  # On Windows
source .venv/bin/activate  # On Unix

# Install dependencies with UV
uv pip install -r requirements.txt

# Install development dependencies
uv pip install -r requirements-dev.txt
```

### Alternative: Using Poetry
```bash
# Install dependencies and create virtual environment
poetry install

# Activate the virtual environment
poetry shell
```

## 🔧 Configuration

Create a `.env` file in the project root:
```ini
RAGFLOW_API_KEY=your_api_key_here
UPLOAD_BATCH_SIZE=100
MAX_RETRIES=3
WATCH_DIRECTORY=./uploads
LOG_LEVEL=INFO
```

## 📚 Usage

### Command Line Interface
```bash
# Start the uploader with default settings
ragflow-uploader

# Specify custom watch directory
ragflow-uploader --watch-dir ./my_documents

# Enable verbose logging
ragflow-uploader --verbose
```

### Python API
```python
import asyncio
from ragflow_uploader import RAGFlowUploader

async def main():
    uploader = RAGFlowUploader(
        api_key="your_api_key",
        watch_directory="./uploads"
    )
    
    # Start monitoring and uploading
    await uploader.start()
    
    # Upload a specific file
    await uploader.upload_file("path/to/document.pdf")

if __name__ == "__main__":
    asyncio.run(main())
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov --cov-report=html

# Run specific test categories
pytest tests/test_uploader.py -v
pytest tests/test_monitoring.py -v
```

## 🔍 Code Quality

```bash
# Format code
black .

# Sort imports
isort .

# Lint code
ruff check .

# Type checking
mypy .
```

## 📦 Project Structure

```
ragflow-uploader/
├── ragflow_uploader/
│   ├── __init__.py
│   ├── uploader.py      # Core upload functionality
│   ├── monitor.py       # File system monitoring
│   ├── progress.py      # Progress tracking
│   └── utils.py         # Utility functions
├── tests/
│   ├── test_uploader.py
│   ├── test_monitor.py
│   └── test_progress.py
├── pyproject.toml       # Project configuration
├── requirements.txt     # Production dependencies
└── README.md
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed
4. Run tests and linting
   ```bash
   pytest
   black .
   isort .
   ruff check .
   mypy .
   ```
5. Commit changes (`git commit -m 'Add amazing feature'`)
6. Push to branch (`git push origin feature/amazing-feature`)
7. Open Pull Request

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.

## 📫 Contact

Project Maintainer - [your.email@example.com](mailto:your.email@example.com)

## 📚 API Documentation

For detailed API documentation, see [api.md](api.md).
