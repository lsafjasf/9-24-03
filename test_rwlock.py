"""读写锁缺陷复现 + 修复回归测试。

复现不靠时序运气：
  - 写者饥饿：用事件做"读者接力"编排，保证读者计数在写者等待期间
    永远 >= 1，写者必然拿不到锁。
  - 双写者窗口：用 Barrier 把两个写者精确停在"检查已通过、标志未置位"
    的窗口里，再同时放行，必然双双进入临界区。

运行：python3 -m unittest test_rwlock -v
"""

import threading
import time
import unittest

from rwlock_buggy import BuggyReadWriteLock
from rwlock_fixed import ReadWriteLock


def _hook_event(lock, watch_name, event):
    """让 lock 的观测钩子在命中 watch_name 时置位 event。"""
    def hook(name):
        if name == watch_name:
            event.set()
    lock._breakpoint = hook


class BuggyReproductionTests(unittest.TestCase):
    """复现缺陷：这些测试断言缺陷确实存在（修复前的行为）。"""

    def test_writer_starvation(self):
        """读者接力：读者计数永不归零，写者被无限期推迟。"""
        lock = BuggyReadWriteLock()
        writer_parked = threading.Event()
        writer_acquired = threading.Event()
        _hook_event(lock, "writer_waiting", writer_parked)

        def writer():
            lock.acquire_write()
            writer_acquired.set()
            lock.release_write()

        lock.acquire_read()  # 主线程先持读锁，readers = 1
        w = threading.Thread(target=writer)
        w.start()
        # 确定性等待：写者已进入等待循环（正在 cond.wait 或即将 wait）
        self.assertTrue(writer_parked.wait(timeout=2))

        # 读者接力 5 轮：先加新读者再放旧读者，readers 永远 >= 1
        for _ in range(5):
            lock.acquire_read()
            lock.release_read()

        # 缺陷：尽管读锁曾"空闲"过（接力间隙），写者仍拿不到锁
        self.assertFalse(writer_acquired.wait(timeout=0.3),
                         "缺陷未复现：写者不应在读者接力期间拿到锁")

        # 收尾：真正放空读者，写者才能进入
        lock.release_read()
        self.assertTrue(writer_acquired.wait(timeout=2))
        w.join(timeout=2)

    def test_double_writer_window(self):
        """检查与置位之间的窗口：两个写者同时进入临界区。"""
        lock = BuggyReadWriteLock()
        barrier = threading.Barrier(2)

        def hook(name):
            if name == "before_commit_writer":
                # 把两个写者都钉在窗口里，再同时放行
                barrier.wait(timeout=5)
        lock._breakpoint = hook

        guard = threading.Lock()
        active = 0
        max_active = 0

        def writer():
            nonlocal active, max_active
            lock.acquire_write()
            with guard:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)  # 持有临界区，放大可观察的重叠
            with guard:
                active -= 1
            lock.release_write()

        threads = [threading.Thread(target=writer) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # 缺陷：两个写者同时处于临界区
        self.assertEqual(max_active, 2,
                         "缺陷未复现：应观察到两个写者同时在临界区内")


class FixedRegressionTests(unittest.TestCase):
    """回归测试：修复后的锁必须满足互斥、写者不饿死、读并发不受影响。"""

    def test_writer_not_starved(self):
        """同样的读者接力场景：写者一旦等待，新读者被挡住，写者及时进入。"""
        lock = ReadWriteLock()
        writer_parked = threading.Event()
        writer_acquired = threading.Event()
        reader2_acquired = threading.Event()
        _hook_event(lock, "writer_waiting", writer_parked)

        def writer():
            lock.acquire_write()
            writer_acquired.set()
            lock.release_write()

        def reader2():
            lock.acquire_read()
            reader2_acquired.set()
            lock.release_read()

        lock.acquire_read()  # readers = 1
        w = threading.Thread(target=writer)
        w.start()
        self.assertTrue(writer_parked.wait(timeout=2))

        # 写者等待期间，新读者必须排队（写者优先）
        r2 = threading.Thread(target=reader2)
        r2.start()
        self.assertFalse(reader2_acquired.wait(timeout=0.3),
                         "写者等待期间新读者不应插队")

        # 主线程释放读锁后，写者必须先于排队读者进入
        lock.release_read()
        self.assertTrue(writer_acquired.wait(timeout=2),
                        "写者被饿死：读者放空后写者仍未进入")
        w.join(timeout=2)

        # 写者完成后，排队的读者被唤醒
        self.assertTrue(reader2_acquired.wait(timeout=2))
        r2.join(timeout=2)

    def test_write_mutual_exclusion(self):
        """写者等待期间，第二个写者绝不能进入；释放后才放行。"""
        lock = ReadWriteLock()
        w1_holding = threading.Event()
        w1_release = threading.Event()
        w2_acquired = threading.Event()

        def writer1():
            lock.acquire_write()
            w1_holding.set()
            w1_release.wait(timeout=5)
            lock.release_write()

        def writer2():
            w1_holding.wait(timeout=5)
            lock.acquire_write()
            w2_acquired.set()
            lock.release_write()

        t1 = threading.Thread(target=writer1)
        t2 = threading.Thread(target=writer2)
        t1.start()
        t2.start()
        self.assertTrue(w1_holding.wait(timeout=2))
        time.sleep(0.2)  # 给 w2 充分时间尝试进入
        self.assertFalse(w2_acquired.is_set(),
                         "写锁互斥被破坏：w1 持锁期间 w2 进入了")
        w1_release.set()
        self.assertTrue(w2_acquired.wait(timeout=2))
        t1.join(timeout=2)
        t2.join(timeout=2)

    def test_read_concurrency_preserved(self):
        """无写者时，多个读者可同时持有读锁（读并发不受影响）。"""
        lock = ReadWriteLock()
        n_readers = 4
        barrier = threading.Barrier(n_readers)
        errors = []

        def reader():
            try:
                lock.acquire_read()
                # 若读锁被互斥化，barrier 永远凑不齐，超时即失败
                barrier.wait(timeout=3)
                lock.release_read()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(n_readers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        self.assertEqual(errors, [], f"读并发被破坏: {errors}")

    def test_stress_invariants(self):
        """压力回归：任何时刻至多一个写者，且写者持锁时无读者。"""
        lock = ReadWriteLock()
        guard = threading.Lock()
        state = {"readers": 0, "writer": False}
        violations = []
        stop = time.monotonic() + 1.5

        def check():
            if state["writer"] and (state["readers"] > 0):
                violations.append("writer overlaps readers")
            if violations:
                return

        def reader():
            while time.monotonic() < stop and not violations:
                lock.acquire_read()
                with guard:
                    state["readers"] += 1
                    check()
                with guard:
                    state["readers"] -= 1
                lock.release_read()

        def writer():
            while time.monotonic() < stop and not violations:
                lock.acquire_write()
                with guard:
                    if state["writer"]:
                        violations.append("two writers")
                    state["writer"] = True
                    check()
                with guard:
                    state["writer"] = False
                lock.release_write()

        threads = ([threading.Thread(target=reader) for _ in range(6)]
                   + [threading.Thread(target=writer) for _ in range(3)])
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertEqual(violations, [])

    def test_context_managers(self):
        lock = ReadWriteLock()
        with lock.read_locked():
            self.assertEqual(lock._readers, 1)
        self.assertEqual(lock._readers, 0)
        with lock.write_locked():
            self.assertTrue(lock._writer_active)
        self.assertFalse(lock._writer_active)


if __name__ == "__main__":
    unittest.main(verbosity=2)
