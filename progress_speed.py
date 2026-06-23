import time
from collections import deque
from typing import Optional


class ProgressStream:
    """Handles a single stream of progress (either upload or download)."""

    def __init__(self, total_size: Optional[int] = None, window_seconds: float = 3.0):
        self.total_size = total_size
        self.processed = 0
        self.window_seconds = window_seconds
        self.history = deque()
        self.last_print_time = 0.0

    def update(self, bytes_count: int):
        self.processed += bytes_count
        now = time.monotonic()
        self.history.append((now, self.processed))

        # Prune elements in history older than the sliding window limit
        while self.history and (now - self.history[0][0]) > self.window_seconds:
            self.history.popleft()

    def get_recent_speed(self) -> float:
        """Returns the recent average speed in bytes per second."""
        if len(self.history) < 2:
            return 0.0
        time_diff = self.history[-1][0] - self.history[0][0]
        if time_diff <= 0:
            return 0.0
        return (self.history[-1][1] - self.history[0][1]) / time_diff


class ProgressSpeedManager:
    """Manages both Upload and Download trackers simultaneously."""

    def __init__(self, download_size: Optional[int] = None, upload_size: Optional[int] = None):
        self.download = ProgressStream(download_size) if download_size is not None else None
        self.upload = ProgressStream(upload_size) if upload_size is not None else None
        self.last_print_time = 0.0

    def display(self, force: bool = False):
        """Prints a throttled progress bar to the terminal to avoid CPU overhead."""
        now = time.monotonic()
        # Throttles printing to a maximum of once every 0.3 seconds
        if not force and (now - self.last_print_time) < 0.3:
            return
        self.last_print_time = now

        output = "\r"

        # Helper to format tracker output
        def format_stream(tracker, label):
            speed = tracker.get_recent_speed() / (1024 * 1024)
            mb = tracker.processed / (1024 * 1024)
            if tracker.total_size and tracker.total_size > 0:
                pct = (tracker.processed / tracker.total_size) * 100
                total = tracker.total_size / (1024 * 1024)
                return f"{label}: {pct:5.1f}% | {mb:7.1f}/{total:7.1f} MB | {speed:6.1f} MB/s"
            return f"{label}: {mb:7.1f} MB | {speed:6.1f} MB/s"

        parts = []
        if self.download:
            parts.append(format_stream(self.download, "DL"))
        if self.upload:
            parts.append(format_stream(self.upload, "UL"))

        print(f"\r\033[K{' | '.join(parts)}", end="", flush=True)