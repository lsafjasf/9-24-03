"""修复后的读写锁（写者优先，标准库实现）。

协议（所有状态变更都在同一把监视器锁内完成）：
  - 读者进入条件：无活动写者 且 无等待写者（写者优先，防止写者饥饿）。
  - 写者进入条件：无活动写者 且 读者数为 0；
    检查与置位 _writer_active 在同一次持锁内原子完成，无窗口。
  - 释放（读/写）都在锁内修改状态并 notify_all。

读并发不受影响：没有写者等待时，读者互不阻塞，可任意并发进入。
"""

import threading
from contextlib import contextmanager


class ReadWriteLock:
    def __init__(self):
        self._cond = threading.Condition()
        self._readers = 0
        self._writer_active = False
        self._writers_waiting = 0

    def _breakpoint(self, name):
        """观测钩子：生产中为空操作，测试可替换以确定性观察内部状态。"""

    # ---- 读者 ----

    def acquire_read(self):
        with self._cond:
            # 写者优先：有写者在等时，新读者排队，写者不会被饿死
            while self._writer_active or self._writers_waiting > 0:
                self._breakpoint("reader_waiting")
                self._cond.wait()
            self._readers += 1

    def release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    # ---- 写者 ----

    def acquire_write(self):
        with self._cond:
            self._writers_waiting += 1
            try:
                while self._writer_active or self._readers > 0:
                    self._breakpoint("writer_waiting")
                    self._cond.wait()
            finally:
                self._writers_waiting -= 1
            # 检查与置位在同一次持锁内完成：不存在双写者窗口
            self._writer_active = True

    def release_write(self):
        with self._cond:
            self._writer_active = False
            self._cond.notify_all()

    # ---- 上下文管理器 ----

    @contextmanager
    def read_locked(self):
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_locked(self):
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()
