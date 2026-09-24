from __future__ import annotations

import threading
from contextlib import contextmanager


class CounterRWLock:
    """Buggy reader-counter implementation kept solely for reproduction."""

    def __init__(
        self,
        wait_hook=None,
        before_writer_published=None,
        on_writer_published=None,
    ) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer_present = False
        self.waiting_writers = 0
        self._wait_hook = wait_hook
        self._before_writer_published = before_writer_published
        self._on_writer_published = on_writer_published

    def acquire_read(self) -> None:
        with self._cond:
            while self._writer_present:
                self._cond.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._cond:
            if self._readers <= 0:
                raise RuntimeError("read lock released without an active reader")
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self) -> None:
        self._cond.acquire()
        published = None
        before_published = self._before_writer_published
        try:
            self.waiting_writers += 1
            while self._writer_present or self._readers > 0:
                if self._wait_hook is None:
                    self._cond.wait()
                    continue
                self._cond.release()
                try:
                    self._wait_hook()
                finally:
                    self._cond.acquire()

            self.waiting_writers -= 1
            published = self._on_writer_published
        finally:
            self._cond.release()

        if before_published is not None:
            before_published()

        # Defect: admission was decided before the writer state was published.
        self._writer_present = True
        if published is not None:
            self._on_writer_published()

    def release_write(self) -> None:
        with self._cond:
            if not self._writer_present:
                raise RuntimeError("write lock released without an active writer")
            self._writer_present = False
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
