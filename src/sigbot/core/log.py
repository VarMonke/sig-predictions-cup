from __future__ import annotations

import asyncio
import contextlib
import logging
import queue
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from typing import Any


class _UtcFormatter(logging.Formatter):
    converter = time.gmtime

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        ct = self.converter(record.created)
        return f"{time.strftime('%Y-%m-%dT%H:%M:%S', ct)}.{int(record.msecs):03d}Z"


_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def setup_logging(log_dir: Path, level: str = "INFO", filename: str = "sigbot.core.log") -> QueueListener:
    """Route all logging through a queue so the event loop never blocks on file I/O.

    Returns the started QueueListener; call ``.stop()`` on shutdown to flush.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = _UtcFormatter(_FORMAT)

    file_handler = RotatingFileHandler(
        log_dir / filename,
        maxBytes=50 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)

    log_queue: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(QueueHandler(log_queue))
    root.setLevel(level)
    for noisy in ("aiohttp.access", "asyncio", "discord"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))

    listener = QueueListener(log_queue, file_handler, console_handler, respect_handler_level=True)
    listener.start()
    return listener


class RecorderLogHandler(logging.Handler):
    """Streams log records into the database ``logs`` table (for the Grafana log view): INFO and up from
    our own code, WARNING and up from libraries. It runs in the QueueListener thread, so every row hops
    onto the event loop, where the Recorder lives; rows logged after the loop has closed are dropped."""

    def __init__(
        self,
        write: Callable[[str, tuple[Any, ...]], None],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(logging.INFO)
        self._write = write
        self._loop = loop

    def emit(self, record: logging.LogRecord) -> None:
        ours = record.name == "sigbot" or record.name.startswith("sigbot.")
        if record.levelno < (self.level if ours else logging.WARNING):
            return
        row = (
            datetime.fromtimestamp(record.created, UTC),
            record.levelname,
            record.name,
            self.format(record)[:4000],
        )
        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._write, "logs", row)


def stream_logs(listener: QueueListener, handler: logging.Handler) -> None:
    listener.handlers = (*listener.handlers, handler)
