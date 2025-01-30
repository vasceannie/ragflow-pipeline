import os
import hashlib
import requests
import time
import fnmatch
import mimetypes
import magic  # for better file type detection
from typing import List, Set, Dict, Optional, Generator, Union, Tuple
from pathlib import Path
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import asyncio
from dataclasses import dataclass, field
import json
from functools import partial
from datetime import datetime, timedelta
import humanize  # for human-readable sizes and times
import psutil  # for system resource monitoring
from collections import deque

# Configure logging with file output
log_file = 'ragflow_uploader.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

@dataclass
class ProcessingMetrics:
    """Real-time processing metrics"""
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    disk_io_read: int = 0
    disk_io_write: int = 0
    network_sent: int = 0
    network_recv: int = 0
    
    def to_dict(self) -> dict:
        return {
            'cpu_percent': f"{self.cpu_percent:.1f}%",
            'memory_percent': f"{self.memory_percent:.1f}%",
            'disk_io_read': humanize.naturalsize(self.disk_io_read),
            'disk_io_write': humanize.naturalsize(self.disk_io_write),
            'network_sent': humanize.naturalsize(self.network_sent),
            'network_recv': humanize.naturalsize(self.network_recv)
        }

@dataclass
class FileStats:
    """Enhanced statistics for file processing with historical data"""
    total_size: int = 0
    processed_size: int = 0
    successful_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    retry_count: int = 0
    start_time: Optional[datetime] = None
    last_error: Optional[str] = None
    error_files: List[str] = field(default_factory=list)
    current_file: Optional[str] = None
    
    # Historical data for graphs
    speed_history: deque = field(default_factory=lambda: deque(maxlen=100))
    success_rate_history: deque = field(default_factory=lambda: deque(maxlen=100))
    metrics_history: deque = field(default_factory=lambda: deque(maxlen=100))
    
    # File type statistics
    extension_stats: Dict[str, int] = field(default_factory=dict)
    mime_type_stats: Dict[str, int] = field(default_factory=dict)
    size_distribution: Dict[str, int] = field(default_factory=dict)
    
    def __post_init__(self):
        self.error_files = []
        self.speed_history = deque(maxlen=100)
        self.success_rate_history = deque(maxlen=100)
        self.metrics_history = deque(maxlen=100)
        self.extension_stats = {}
        self.mime_type_stats = {}
        self.size_distribution = {
            '0-1MB': 0,
            '1-10MB': 0,
            '10-100MB': 0,
            '100MB-1GB': 0,
            '>1GB': 0
        }
        self.current_metrics = ProcessingMetrics()
        self._last_update = time.time()
        self._last_processed_size = 0
    
    def update_metrics(self):
        """Update system metrics"""
        self.current_metrics.cpu_percent = psutil.cpu_percent()
        self.current_metrics.memory_percent = psutil.virtual_memory().percent
        
        disk_io = psutil.disk_io_counters()
        self.current_metrics.disk_io_read = disk_io.read_bytes
        self.current_metrics.disk_io_write = disk_io.write_bytes
        
        net_io = psutil.net_io_counters()
        self.current_metrics.network_sent = net_io.bytes_sent
        self.current_metrics.network_recv = net_io.bytes_recv
        
        self.metrics_history.append(self.current_metrics)
    
    def update_file_stats(self, file_path: Path, mime_type: str):
        """Update file statistics"""
        ext = file_path.suffix.lower()
        size = file_path.stat().st_size
        
        # Update extension stats
        self.extension_stats[ext] = self.extension_stats.get(ext, 0) + 1
        
        # Update MIME type stats
        self.mime_type_stats[mime_type] = self.mime_type_stats.get(mime_type, 0) + 1
        
        # Update size distribution
        size_mb = size / (1024 * 1024)
        if size_mb <= 1:
            self.size_distribution['0-1MB'] += 1
        elif size_mb <= 10:
            self.size_distribution['1-10MB'] += 1
        elif size_mb <= 100:
            self.size_distribution['10-100MB'] += 1
        elif size_mb <= 1024:
            self.size_distribution['100MB-1GB'] += 1
        else:
            self.size_distribution['>1GB'] += 1
    
    @property
    def elapsed_time(self) -> float:
        if not self.start_time:
            return 0
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def processing_speed(self) -> float:
        now = time.time()
        time_diff = now - self._last_update
        size_diff = self.processed_size - self._last_processed_size
        
        if time_diff > 0:
            speed = size_diff / (1024 * 1024 * time_diff)  # MB/s
            self.speed_history.append(speed)
            self._last_update = now
            self._last_processed_size = self.processed_size
            return speed
        return 0
    
    @property
    def average_speed(self) -> float:
        """Calculate moving average speed"""
        if not self.speed_history:
            return 0
        return sum(self.speed_history) / len(self.speed_history)
    
    @property
    def success_rate(self) -> float:
        total = self.successful_count + self.failed_count
        if total == 0:
            return 100.0
        rate = (self.successful_count / total) * 100
        self.success_rate_history.append(rate)
        return rate
    
    @property
    def estimated_time_remaining(self) -> timedelta:
        if not self.average_speed or not self.total_size:
            return timedelta(0)
        remaining_size = self.total_size - self.processed_size
        remaining_seconds = remaining_size / (self.average_speed * 1024 * 1024)
        return timedelta(seconds=int(remaining_seconds))
    
    def format_progress(self) -> str:
        """Format progress for display"""
        return (
            f"\nProgress Report:"
            f"\n{'='*50}"
            f"\nFiles:"
            f"\n - Processing: {self.current_file or 'None'}"
            f"\n - Successful: {self.successful_count}"
            f"\n - Failed: {self.failed_count}"
            f"\n - Skipped: {self.skipped_count}"
            f"\n - Success Rate: {self.success_rate:.1f}%"
            f"\n"
            f"\nSize:"
            f"\n - Total: {humanize.naturalsize(self.total_size)}"
            f"\n - Processed: {humanize.naturalsize(self.processed_size)}"
            f"\n - Remaining: {humanize.naturalsize(self.total_size - self.processed_size)}"
            f"\n"
            f"\nSpeed:"
            f"\n - Current: {self.processing_speed:.1f} MB/s"
            f"\n - Average: {self.average_speed:.1f} MB/s"
            f"\n - Time Elapsed: {humanize.naturaldelta(self.elapsed_time)}"
            f"\n - Time Remaining: {humanize.naturaldelta(self.estimated_time_remaining)}"
            f"\n"
            f"\nSystem Metrics:"
            f"\n - CPU Usage: {self.current_metrics.cpu_percent:.1f}%"
            f"\n - Memory Usage: {self.current_metrics.memory_percent:.1f}%"
            f"\n - Disk Read: {humanize.naturalsize(self.current_metrics.disk_io_read)}"
            f"\n - Disk Write: {humanize.naturalsize(self.current_metrics.disk_io_write)}"
            f"\n - Network Sent: {humanize.naturalsize(self.current_metrics.network_sent)}"
            f"\n - Network Received: {humanize.naturalsize(self.current_metrics.network_recv)}"
            f"\n{'='*50}"
        )
    
    def to_dict(self) -> dict:
        """Convert stats to JSON-serializable dict with enhanced metrics"""
        return {
            'files': {
                'total': self.successful_count + self.failed_count + self.skipped_count,
                'successful': self.successful_count,
                'failed': self.failed_count,
                'skipped': self.skipped_count,
                'success_rate': f"{self.success_rate:.1f}%",
                'error_files': self.error_files,
                'current_file': self.current_file
            },
            'size': {
                'total': humanize.naturalsize(self.total_size),
                'processed': humanize.naturalsize(self.processed_size),
                'remaining': humanize.naturalsize(self.total_size - self.processed_size)
            },
            'speed': {
                'current': f"{self.processing_speed:.1f} MB/s",
                'average': f"{self.average_speed:.1f} MB/s",
                'history': list(self.speed_history)
            },
            'time': {
                'elapsed': humanize.naturaldelta(self.elapsed_time),
                'remaining': humanize.naturaldelta(self.estimated_time_remaining)
            },
            'metrics': self.current_metrics.to_dict(),
            'file_stats': {
                'extensions': self.extension_stats,
                'mime_types': self.mime_type_stats,
                'size_distribution': self.size_distribution
            }
        }

class ProgressDisplay:
    """Handles progress display and updates"""
    
    def __init__(self, stats: FileStats, refresh_rate: float = 1.0):
        self.stats = stats
        self.refresh_rate = refresh_rate
        self._last_display = 0
    
    def update(self, force: bool = False):
        """Update progress display if needed"""
        now = time.time()
        if force or (now - self._last_display) >= self.refresh_rate:
            self.stats.update_metrics()
            print(self.stats.format_progress())
            self._last_display = now

class FileProcessor:
    """Enhanced file filtering and stats collection"""
    
    def __init__(
        self,
        allowed_extensions: Optional[Set[str]] = None,
        exclude_dirs: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        content_patterns: Optional[List[str]] = None,
        mime_types: Optional[List[str]] = None,
        modified_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        max_depth: Optional[int] = None,
        include_hidden: bool = False,
        min_text_ratio: Optional[float] = None,
        encoding: str = 'utf-8',
        backup_dir: Optional[str] = None
    ):
        self.allowed_extensions = allowed_extensions
        self.exclude_dirs = exclude_dirs or []
        self.exclude_patterns = exclude_patterns or []
        self.min_size = min_size
        self.max_size = max_size
        self.content_patterns = content_patterns
        self.mime_types = set(mime_types) if mime_types else None
        self.modified_after = modified_after
        self.modified_before = modified_before
        self.max_depth = max_depth
        self.include_hidden = include_hidden
        self.min_text_ratio = min_text_ratio
        self.encoding = encoding
        self.backup_dir = backup_dir
        self.stats = FileStats()
        self.magic = magic.Magic(mime=True)
        
    def is_hidden(self, path: Path) -> bool:
        """Check if file or any parent directory is hidden"""
        parts = path.parts
        return any(part.startswith('.') for part in parts)
    
    def get_relative_depth(self, path: Path, root: Path) -> int:
        """Calculate relative directory depth"""
        return len(path.relative_to(root).parts)
    
    def is_text_file(self, file_path: Path) -> bool:
        """Check if file is text and meets minimum text ratio"""
        try:
            mime_type = self.magic.from_file(str(file_path))
            if not mime_type.startswith('text/'):
                return False

            if self.min_text_ratio:
                with open(file_path, 'rb') as f:
                    content = f.read()
                    text_chars = sum(32 <= c <= 126 or c in {9, 10, 13} for c in content)
                    ratio = text_chars / len(content)
                    return ratio >= self.min_text_ratio
            return True
        except Exception:
            return False
    
    def create_backup(self, file_path: Path) -> Optional[Path]:
        """Create backup of file before processing"""
        if not self.backup_dir:
            return None
            
        try:
            backup_path = Path(self.backup_dir) / file_path.name
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.copy2(file_path, backup_path)
            return backup_path
        except Exception as e:
            logging.warning(f"Failed to create backup of {file_path}: {e}")
            return None
        
    def should_process_file(self, file_path: Path, root_dir: Path = None) -> bool:
        """Enhanced file filtering"""
        try:
            # Hidden file check
            if not self.include_hidden and self.is_hidden(file_path):
                return False

            # Depth check
            if root_dir and self.max_depth is not None and self.get_relative_depth(file_path, root_dir) > self.max_depth:
                return False

            # Basic checks
            if self.allowed_extensions and file_path.suffix.lower() not in self.allowed_extensions:
                return False

            if any(fnmatch.fnmatch(file_path.name, pattern) for pattern in self.exclude_patterns):
                return False

            # Size check
            file_size = file_path.stat().st_size
            if self.min_size and file_size < self.min_size:
                return False
            if self.max_size and file_size > self.max_size:
                return False

            # Modification time check
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            if self.modified_after and mtime < self.modified_after:
                return False
            if self.modified_before and mtime > self.modified_before:
                return False

            # MIME type check
            if self.mime_types:
                mime_type = self.magic.from_file(str(file_path))
                if not any(mime_type.startswith(m) for m in self.mime_types):
                    return False

            # Text ratio check
            if self.min_text_ratio and not self.is_text_file(file_path):
                return False

            # Content pattern check
            if self.content_patterns:
                try:
                    with open(file_path, 'r', encoding=self.encoding, errors='ignore') as f:
                        content = f.read()
                        if all(
                            pattern not in content
                            for pattern in self.content_patterns
                        ):
                            return False
                except UnicodeDecodeError:
                    return False

            return True
        except Exception as e:
            logging.warning(f"Error checking file {file_path}: {str(e)}")
            return False

class RetryPolicy:
    """Configurable retry policy for failed operations"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: float = 0.1
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and jitter"""
        import random
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        jitter_range = delay * self.jitter
        return delay + random.uniform(-jitter_range, jitter_range)

class DirectoryWatcher(FileSystemEventHandler):
    """Watches directory for changes and triggers processing"""
    
    def __init__(self, uploader, dataset_name: str, processor: FileProcessor):
        self.uploader = uploader
        self.dataset_name = dataset_name
        self.processor = processor
        self.processing_queue = asyncio.Queue()
        self.dataset_id = None
        
    async def start(self, path: str):
        """Start watching directory"""
        self.dataset_id = self.uploader.create_dataset(self.dataset_name)
        
        # Start observer
        observer = Observer()
        observer.schedule(self, path, recursive=True)
        observer.start()
        
        try:
            while True:
                # Process any files in queue
                try:
                    file_path = await asyncio.wait_for(self.processing_queue.get(), timeout=1.0)
                    await self.process_file(file_path)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logging.error(f"Error processing file: {str(e)}")
        finally:
            observer.stop()
            observer.join()
            
    async def process_file(self, file_path: Path):
        """Process a single file"""
        if not self.processor.should_process_file(file_path):
            return
            
        try:
            await self.uploader.upload_and_process_file(
                self.dataset_id,
                file_path,
                update_stats=self.processor.stats
            )
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            
    def on_created(self, event):
        if not event.is_directory:
            self.processing_queue.put_nowait(Path(event.src_path))
            
    def on_modified(self, event):
        if not event.is_directory:
            self.processing_queue.put_nowait(Path(event.src_path))

class RAGFlowUploader:
    """Pipeline for uploading local directories to RAGFlow with progress tracking and duplicate handling"""
    
    # Default supported file extensions
    DEFAULT_SUPPORTED_EXTENSIONS = {
        '.pdf', '.docx', '.txt', '.md', '.csv',
        '.json', '.xml', '.html', '.htm'
    }
    
    def __init__(
        self,
        api_key: str,
        base_url: str = "http://localhost",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        max_workers: int = 4,
        hash_algorithm: str = 'sha256',
        retry_policy: RetryPolicy = None
    ):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
        self.hash_algorithm = getattr(hashlib, hash_algorithm)
        self.retry_policy = retry_policy
        
    def _make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> dict:
        """Make HTTP request with retries and error handling"""
        url = f"{self.base_url}/api/v1/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries):
            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.retry_delay * (attempt + 1))

    def create_dataset(self, name: str) -> str:
        """Create or get existing dataset"""
        self.logger.info(f"Creating/retrieving dataset: {name}")
        
        try:
            result = self._make_request(
                "POST",
                "datasets",
                headers=self.headers,
                json={"name": name}
            )
            dataset_id = result['data']['id']
            self.logger.info(f"Created new dataset: {name} (ID: {dataset_id})")
            return dataset_id
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:  # Handle duplicate dataset
                datasets = self._make_request("GET", "datasets", headers=self.headers)
                dataset_id = next(d['id'] for d in datasets['data'] if d['name'] == name)
                self.logger.info(f"Using existing dataset: {name} (ID: {dataset_id})")
                return dataset_id
            raise

    def get_existing_documents(self, dataset_id: str) -> Dict[str, Dict]:
        """Get map of existing documents with metadata for smarter duplicate checking"""
        self.logger.info(f"Fetching existing documents for dataset: {dataset_id}")

        result = self._make_request(
            "GET",
            f"datasets/{dataset_id}/documents",
            headers=self.headers
        )

        docs = {
            doc['name']: {
                'id': doc['id'],
                'size': doc.get('size'),
                'update_time': doc.get('update_time'),
                'hash': doc.get('hash'),  # Store hash if available
            }
            for doc in result['data'].get('docs', [])
        }
        self.logger.info(f"Found {len(docs)} existing documents")
        return docs

    def calculate_file_hash(self, file_path: Path, chunk_size: int = 8192) -> str:
        """Calculate file hash for duplicate detection"""
        hash_obj = self.hash_algorithm()
        
        total_size = file_path.stat().st_size
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Hashing {file_path.name}") as pbar:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_obj.update(chunk)
                    pbar.update(len(chunk))
                    
        return hash_obj.hexdigest()

    def should_upload_file(
        self,
        file_path: Path,
        existing_docs: Dict[str, Dict],
        force_hash_check: bool = False
    ) -> bool:
        """Determine if file should be uploaded based on metadata and optionally hash"""
        if file_path.name not in existing_docs:
            return True
            
        existing = existing_docs[file_path.name]
        file_stat = file_path.stat()
        
        # Quick checks first
        if file_stat.st_size != existing.get('size'):
            return True
            
        file_mtime = int(file_stat.st_mtime * 1000)
        if abs(file_mtime - existing.get('update_time', 0)) > 1000:
            return True
            
        # Optional hash check
        if force_hash_check or existing.get('hash'):
            file_hash = self.calculate_file_hash(file_path)
            return file_hash != existing.get('hash')
            
        return False

    async def upload_and_process_file(
        self,
        dataset_id: str,
        file_path: Path,
        pbar: Optional[tqdm] = None,
        update_stats: Optional[FileStats] = None
    ) -> Optional[str]:
        """Upload file with progress tracking and stats updates"""
        try:
            if update_stats:
                update_stats.total_size += file_path.stat().st_size
                
            result = self._make_request(
                "POST",
                f"datasets/{dataset_id}/documents",
                headers={'Authorization': self.headers['Authorization']},
                files={'file': (file_path.name, open(file_path, 'rb'))}
            )
            
            doc_id = result['data'][0]['id']
            
            if pbar:
                pbar.update(1)
                pbar.set_postfix(
                    file=file_path.name,
                    status="OK",
                    speed=f"{update_stats.processing_speed:.1f}MB/s" if update_stats else ""
                )
                
            if update_stats:
                update_stats.successful_count += 1
                update_stats.processed_size += file_path.stat().st_size
                
            return doc_id
            
        except Exception as e:
            self.logger.error(f"Error uploading {file_path}: {str(e)}")
            if pbar:
                pbar.set_postfix(file=file_path.name, status=f"Failed: {str(e)}")
            if update_stats:
                update_stats.failed_count += 1
            return None

    async def _process_files_batch(
        self,
        dataset_id: str,
        files: List[Path],
        pbar: tqdm,
        processor: FileProcessor
    ) -> List[str]:
        """Process a batch of files concurrently"""
        successful_uploads = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                (asyncio.get_event_loop().run_in_executor(
                    executor,
                    self.upload_and_process_file,
                    dataset_id,
                    file_path,
                    pbar,
                    processor.stats
                ), file_path)
                for file_path in files
            ]
            
            for future, file_path in futures:
                try:
                    if doc_id := await future:
                        successful_uploads.append(doc_id)
                except Exception as e:
                    self.logger.error(f"Failed to process {file_path}: {str(e)}")
                    processor.stats.failed_count += 1
                    
        return successful_uploads

    async def _start_document_processing(self, dataset_id: str, doc_ids: List[str]):
        """Start processing uploaded documents"""
        if not doc_ids:
            return
            
        self.logger.info(f"Starting document processing for {len(doc_ids)} files")
        try:
            self._make_request(
                "POST",
                f"datasets/{dataset_id}/chunks",
                headers=self.headers,
                json={"document_ids": doc_ids}
            )
            self.logger.info("Document processing started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start document processing: {str(e)}")

    def _collect_files(
        self,
        root_dir: str,
        processor: FileProcessor,
        existing_docs: Dict[str, Dict],
        force_hash_check: bool
    ) -> List[Path]:
        """Collect files to process with filtering"""
        files_to_process = []
        
        for root, dirs, files in os.walk(root_dir):
            dirs[:] = [d for d in dirs if d not in (processor.exclude_dirs or [])]
            
            for file in files:
                file_path = Path(root) / file
                
                if not processor.should_process_file(file_path):
                    processor.stats.skipped_count += 1
                    continue
                    
                if self.should_upload_file(file_path, existing_docs, force_hash_check):
                    files_to_process.append(file_path)
                else:
                    processor.stats.skipped_count += 1
                    
        return files_to_process

    def _log_final_stats(self, processor: FileProcessor, files_count: int):
        """Log final processing statistics"""
        self.logger.info(
            f"\nProcessing complete:"
            f"\n - Total files: {files_count}"
            f"\n - Successful: {processor.stats.successful_count}"
            f"\n - Failed: {processor.stats.failed_count}"
            f"\n - Skipped: {processor.stats.skipped_count}"
            f"\n - Total size: {processor.stats.total_size / (1024*1024):.1f}MB"
            f"\n - Average speed: {processor.stats.processing_speed:.1f}MB/s"
            f"\n - Time taken: {processor.stats.elapsed_time:.1f}s"
        )

    async def process_directory(
        self,
        dataset_name: str,
        root_dir: str,
        processor: Optional[FileProcessor] = None,
        watch: bool = False,
        force_hash_check: bool = False
    ):
        """Process a directory recursively, with optional watching"""
        processor = processor or FileProcessor(allowed_extensions=self.DEFAULT_SUPPORTED_EXTENSIONS)
        processor.stats.start_time = datetime.now()
        
        self.logger.info(f"Starting directory processing: {root_dir}")
        
        dataset_id = self.create_dataset(dataset_name)
        if not dataset_id:
            self.logger.error("Failed to create/get dataset")
            return

        existing_docs = self.get_existing_documents(dataset_id)
        files_to_process = self._collect_files(root_dir, processor, existing_docs, force_hash_check)

        if not files_to_process:
            self.logger.info("No new files to process")
            if watch:
                watcher = DirectoryWatcher(self, dataset_name, processor)
                await watcher.start(root_dir)
            return

        progress = ProgressDisplay(processor.stats)
        with tqdm(
            total=len(files_to_process),
            desc="Uploading files",
            unit='file',
            postfix={'speed': '0MB/s'}
        ) as pbar:
            successful_uploads = await self._process_files_batch(dataset_id, files_to_process, pbar, processor)
            progress.update(force=True)

        self._log_final_stats(processor, len(files_to_process))
        await self._start_document_processing(dataset_id, successful_uploads)
        
        if watch:
            watcher = DirectoryWatcher(self, dataset_name, processor)
            await watcher.start(root_dir)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='RAGFlow Directory Pipeline - Upload and process local files'
    )
    parser.add_argument('directory', help='Directory to process')
    parser.add_argument(
        '--dataset',
        default='auto_upload',
        help='Dataset name (default: auto_upload)'
    )
    parser.add_argument(
        '--extensions',
        nargs='*',
        help='Allowed file extensions (default: pdf,docx,txt,md,csv,json,xml,html)'
    )
    parser.add_argument(
        '--exclude-dirs',
        nargs='*',
        help='Directory names to exclude'
    )
    parser.add_argument(
        '--exclude-patterns',
        nargs='*',
        help='Glob patterns to exclude (e.g. "*.tmp" "backup_*")'
    )
    parser.add_argument(
        '--min-size',
        type=int,
        help='Minimum file size in bytes'
    )
    parser.add_argument(
        '--max-size',
        type=int,
        help='Maximum file size in bytes'
    )
    parser.add_argument(
        '--content-patterns',
        nargs='*',
        help='Only process files containing these patterns'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of concurrent uploads (default: 4)'
    )
    parser.add_argument(
        '--base-url',
        default='http://localhost',
        help='RAGFlow API base URL (default: http://localhost)'
    )
    parser.add_argument(
        '--watch',
        action='store_true',
        help='Watch directory for changes'
    )
    parser.add_argument(
        '--force-hash',
        action='store_true',
        help='Force hash checking for all files'
    )
    parser.add_argument(
        '--hash-algorithm',
        default='sha256',
        choices=['md5', 'sha1', 'sha256', 'sha512'],
        help='Hash algorithm for duplicate detection (default: sha256)'
    )
    parser.add_argument(
        '--mime-types',
        nargs='*',
        help='Only process files of these MIME types (e.g. "text/plain" "application/pdf")'
    )
    parser.add_argument(
        '--modified-after',
        type=lambda s: datetime.fromisoformat(s),
        help='Only process files modified after this date (ISO format)'
    )
    parser.add_argument(
        '--modified-before',
        type=lambda s: datetime.fromisoformat(s),
        help='Only process files modified before this date (ISO format)'
    )
    parser.add_argument(
        '--max-depth',
        type=int,
        help='Maximum directory depth to process'
    )
    parser.add_argument(
        '--include-hidden',
        action='store_true',
        help='Include hidden files and directories'
    )
    parser.add_argument(
        '--min-text-ratio',
        type=float,
        help='Minimum ratio of text characters for text files'
    )
    parser.add_argument(
        '--encoding',
        default='utf-8',
        help='Encoding for text file operations (default: utf-8)'
    )
    parser.add_argument(
        '--backup-dir',
        help='Directory to store file backups before processing'
    )
    parser.add_argument(
        '--retry-policy',
        type=json.loads,
        help='JSON retry policy configuration'
    )
    parser.add_argument(
        '--stats-file',
        help='File to save processing statistics'
    )
    parser.add_argument(
        '--progress-refresh',
        type=float,
        default=1.0,
        help='Progress display refresh rate in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable detailed progress display'
    )

    args = parser.parse_args()

    # Get API key from environment
    api_key = os.getenv('RAGFLOW_API_KEY')
    if not api_key:
        raise ValueError("Missing RAGFLOW_API_KEY environment variable")

    # Convert extensions to set with dots
    extensions = (
        {f".{ext.lstrip('.')}" for ext in args.extensions}
        if args.extensions
        else None
    )

    # Create file processor with enhanced options
    processor = FileProcessor(
        allowed_extensions=extensions,
        exclude_dirs=args.exclude_dirs,
        exclude_patterns=args.exclude_patterns,
        min_size=args.min_size,
        max_size=args.max_size,
        content_patterns=args.content_patterns,
        mime_types=args.mime_types,
        modified_after=args.modified_after,
        modified_before=args.modified_before,
        max_depth=args.max_depth,
        include_hidden=args.include_hidden,
        min_text_ratio=args.min_text_ratio,
        encoding=args.encoding,
        backup_dir=args.backup_dir
    )

    # Configure retry policy
    retry_policy = RetryPolicy(**(args.retry_policy or {}))

    # Create uploader with retry policy
    uploader = RAGFlowUploader(
        api_key=api_key,
        base_url=args.base_url,
        max_workers=args.max_workers,
        hash_algorithm=args.hash_algorithm,
        retry_policy=retry_policy
    )

    # Run async process_directory
    try:
        asyncio.run(uploader.process_directory(
            dataset_name=args.dataset,
            root_dir=args.directory,
            processor=processor,
            watch=args.watch,
            force_hash_check=args.force_hash
        ))
    finally:
        # Save final stats if requested
        if args.stats_file:
            with open(args.stats_file, 'w') as f:
                json.dump(processor.stats.to_dict(), f, indent=2) 