"""有缺陷的读写锁实现（仅用于复现问题，作为修复前的对照）。

缺陷 1（写者饥饿）：写者等待期间新读者仍可进入。只要读请求
    接续不断，读者计数永远回不到 0，写者被无限期推迟。

缺陷 2（双写者窗口）：获取写锁时"检查"在监视器锁内完成，
    但"置位"在锁外完成；释放写锁时同样在锁外清除标志。
    检查与置位之间存在窗口，两个写者可同时认为自己持有写锁。
"""

import threading


class BuggyReadWriteLock:
    def __init__(self):
        self._cond = threading.Condition()
        self._readers = 0
        self._writer_active = False

    def _breakpoint(self, name):
        """观测钩子：生产中为空操作。

        测试可替换实例属性以确定性地控制线程调度，从而稳定复现竞争，
        而不是靠 sleep 碰运气。
        """

    # ---- 读者 ----

    def acquire_read(self):
        with self._cond:
            # 缺陷 1：只看是否有活动写者，不看是否有写者在等
            while self._writer_active:
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
            while self._readers > 0 or self._writer_active:
                self._breakpoint("writer_waiting")
                self._cond.wait()
        # 缺陷 2：检查在锁内完成，置位却在锁外完成，中间存在窗口
        self._breakpoint("before_commit_writer")
        self._writer_active = True

    def release_write(self):
        # 缺陷 2：标志在锁外清除，与 acquire 的检查/置位不构成原子协议
        self._writer_active = False
        with self._cond:
            self._cond.notify_all()
