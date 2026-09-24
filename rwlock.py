from __future__ import annotations

import threading
from contextlib import contextmanager


class FairRWLock:
    """FIFO ticket RW lock that batches only adjacent pending readers."""

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._next_ticket = 0
        self._granted_to = 0
        self._active_writer = False
        self._active_readers = 0
        self._pending_readers: dict[int, bool] = {}

    def snapshot(self) -> tuple[int, int, int, bool]:
        with self._cond:
            return (
                len(self._pending_readers),
                self._active_readers,
                self._next_ticket - self._granted_to,
                self._active_writer,
            )

    def acquire_read(self) -> None:
        with self._cond:
            ticket = self._next_ticket
            self._next_ticket += 1
            self._pending_readers[ticket] = False
            self._pump()
            while not self._pending_readers[ticket]:
                self._cond.wait()
                self._pump()
            del self._pending_readers[ticket]

    def release_read(self) -> None:
        with self._cond:
            if self._active_readers <= 0:
                raise RuntimeError("read lock released without an active reader")
            self._active_readers -= 1
            self._pump()

    def acquire_write(self) -> None:
        with self._cond:
            ticket = self._next_ticket
            self._next_ticket += 1
            self._pump()
            while (
                ticket != self._granted_to
                or self._active_readers
                or self._active_writer
            ):
                self._cond.wait()
                self._pump()
            self._granted_to += 1
            self._active_writer = True

    def release_write(self) -> None:
        with self._cond:
            if not self._active_writer:
                raise RuntimeError("write lock released without an active writer")
            self._active_writer = False
            self._pump()

    def _pump(self) -> None:
        while self._granted_to in self._pending_readers:
            if self._active_writer:
                return
            self._pending_readers[self._granted_to] = True
            self._granted_to += 1
            self._active_readers += 1
        self._cond.notify_all()

    @contextmanager
    def read_lock(self):
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self):
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()
