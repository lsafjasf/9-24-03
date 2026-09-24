"""Deterministic repro and regression tests for the RW lock defects.

Defect 1 (writer starvation): legacy_rwlock.CounterRWLock lets readers in
whenever no writer is *active*, ignoring *queued* writers. A reader relay
that never lets the reader count reach zero blocks a writer forever.

Defect 2 (double-writer window): rwlock.FairRWLock.acquire_write waited on
`ticket != granted_to or active_readers` but never checked `_active_writer`.
While a writer holds the lock, the next writer's ticket is already current
and the reader count is zero, so it walks straight into the critical
section -> two writers at once, deterministically.

Run: python3 -m unittest test_rwlock -v
"""

from __future__ import annotations

import threading
import time
import unittest

from legacy_rwlock import CounterRWLock
from rwlock import FairRWLock

TIMEOUT = 5.0
SHORT = 0.3


def wait_until(predicate, timeout=TIMEOUT, interval=0.001):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class WriterStarvationReproTests(unittest.TestCase):
    """Defect 1: a reader relay starves a queued writer on CounterRWLock."""

    ROUNDS = 32

    def test_counter_rwlock_writer_starvation_repro(self):
        lock = CounterRWLock()
        writer_acquired = threading.Event()
        writer_done = threading.Event()

        def writer():
            lock.acquire_write()
            writer_acquired.set()
            lock.release_write()
            writer_done.set()

        lock.acquire_read()  # first reader held by the main thread
        writer_thread = threading.Thread(target=writer, daemon=True)
        writer_thread.start()
        # The writer has queued (it bumps waiting_writers before blocking).
        self.assertTrue(wait_until(lambda: lock.waiting_writers == 1))

        # Reader relay: reader i+1 acquires *before* reader i releases, so
        # the reader count provably never reaches zero. The writer's wait
        # condition can therefore never become true -> deterministic block.
        entered = [threading.Event() for _ in range(self.ROUNDS)]
        release_me = [threading.Event() for _ in range(self.ROUNDS)]

        def reader(i):
            lock.acquire_read()
            entered[i].set()
            release_me[i].wait(TIMEOUT)
            lock.release_read()

        readers = []
        for i in range(self.ROUNDS):
            t = threading.Thread(target=reader, args=(i,), daemon=True)
            t.start()
            readers.append(t)
            self.assertTrue(entered[i].wait(TIMEOUT))  # got in despite queued writer
            if i == 0:
                lock.release_read()  # main thread leaves the relay
            else:
                release_me[i - 1].set()
                readers[i - 1].join(TIMEOUT)

        # 32 readers came and went while a writer was queued; it never ran.
        self.assertFalse(writer_acquired.is_set())
        self.assertTrue(writer_thread.is_alive())
        self.assertEqual(lock.waiting_writers, 1)

        # Drain the last reader: the writer must finally get in. This proves
        # the lock is not deadlocked -- the protocol itself starved it.
        release_me[-1].set()
        readers[-1].join(TIMEOUT)
        self.assertTrue(writer_done.wait(TIMEOUT))
        writer_thread.join(TIMEOUT)


class DoubleWriterWindowTests(unittest.TestCase):
    """Defect 2: two writers inside the critical section at once."""

    def test_fair_rwlock_double_writer_window_repro(self):
        lock = FairRWLock()
        w2_entered = threading.Event()
        w2_done = threading.Event()
        overlap_seen = []

        lock.acquire_write()  # W1 (main thread) holds the write lock

        def w2():
            lock.acquire_write()
            w2_entered.set()
            # If W1 still holds the lock here, mutual exclusion is broken.
            overlap_seen.append(lock.snapshot()[3])  # _active_writer flag
            lock.release_write()
            w2_done.set()

        t = threading.Thread(target=w2, daemon=True)
        t.start()
        try:
            entered_while_w1_holds = w2_entered.wait(SHORT)
            self.assertFalse(
                entered_while_w1_holds,
                "second writer entered the critical section while the first "
                "writer still held the lock (double-writer window)",
            )
        finally:
            # On the buggy lock W2's release clears the writer flag under
            # W1's feet; tolerate that so the assertion above is the failure.
            try:
                lock.release_write()
            except RuntimeError:
                pass
        self.assertTrue(w2_done.wait(TIMEOUT))
        t.join(TIMEOUT)
        self.assertEqual(overlap_seen, [True])  # flag set exactly once, by W2

    def test_fair_rwlock_writer_waits_for_active_writer(self):
        """Regression: W2 must not start until W1 releases."""
        lock = FairRWLock()
        order = []
        w2_may_enter = threading.Event()

        def w1():
            with lock.write_lock():
                order.append("w1-acquire")
                self.assertTrue(w2_may_enter.wait(TIMEOUT))
                order.append("w1-release")

        def w2():
            with lock.write_lock():
                order.append("w2")

        t1 = threading.Thread(target=w1)
        t2 = threading.Thread(target=w2)
        t1.start()
        self.assertTrue(wait_until(lambda: order == ["w1-acquire"]))
        t2.start()
        # Give W2 every chance to misbehave while W1 holds the lock.
        time.sleep(SHORT)
        self.assertEqual(order, ["w1-acquire"])
        w2_may_enter.set()
        t1.join(TIMEOUT)
        t2.join(TIMEOUT)
        self.assertEqual(order, ["w1-acquire", "w1-release", "w2"])


class FairnessRegressionTests(unittest.TestCase):
    """The fixed lock must not starve writers behind a reader stream."""

    def test_writer_not_starved_by_reader_stream(self):
        lock = FairRWLock()
        order = []
        r1_holding = threading.Event()
        r1_release = threading.Event()

        def r1():
            with lock.read_lock():
                order.append("r1")
                r1_holding.set()
                self.assertTrue(r1_release.wait(TIMEOUT))

        def writer():
            with lock.write_lock():
                order.append("W")

        def r2():
            with lock.read_lock():
                order.append("r2")

        t_r1 = threading.Thread(target=r1)
        t_w = threading.Thread(target=writer)
        t_r2 = threading.Thread(target=r2)

        t_r1.start()
        self.assertTrue(r1_holding.wait(TIMEOUT))
        t_w.start()
        # Wait until the writer is actually queued (ticket taken).
        self.assertTrue(wait_until(lambda: lock.snapshot()[2] >= 1))
        t_r2.start()
        # Wait until R2 has also queued behind the writer.
        self.assertTrue(wait_until(lambda: lock.snapshot()[0] >= 1))
        r1_release.set()

        t_r1.join(TIMEOUT)
        t_w.join(TIMEOUT)
        t_r2.join(TIMEOUT)
        # FIFO: the writer queued before R2, so it must run before R2 even
        # though R2 is a reader and readers batch together.
        self.assertEqual(order, ["r1", "W", "r2"])

    def test_reader_relay_cannot_starve_writer(self):
        """Mirror of the legacy repro: on the fair lock the relay stalls and
        the writer gets in as soon as existing readers drain."""
        lock = FairRWLock()
        writer_done = threading.Event()

        def writer():
            with lock.write_lock():
                writer_done.set()

        lock.acquire_read()
        w = threading.Thread(target=writer)
        w.start()
        self.assertTrue(wait_until(lambda: lock.snapshot()[2] >= 1))

        # A new reader now queues *behind* the writer.
        r2_entered = threading.Event()

        def r2():
            with lock.read_lock():
                r2_entered.set()

        t2 = threading.Thread(target=r2)
        t2.start()
        self.assertTrue(wait_until(lambda: lock.snapshot()[0] >= 1))
        self.assertFalse(r2_entered.wait(SHORT))  # blocked behind the writer

        lock.release_read()  # last active reader drains
        self.assertTrue(writer_done.wait(TIMEOUT))
        self.assertTrue(r2_entered.wait(TIMEOUT))
        w.join(TIMEOUT)
        t2.join(TIMEOUT)


class ReadConcurrencyTests(unittest.TestCase):
    """The fix must not serialize readers."""

    def test_readers_run_concurrently(self):
        lock = FairRWLock()
        barrier = threading.Barrier(4, timeout=TIMEOUT)
        inside = []
        inside_lock = threading.Lock()
        max_concurrent = 0

        def reader():
            nonlocal max_concurrent
            with lock.read_lock():
                with inside_lock:
                    inside.append(1)
                    max_concurrent = max(max_concurrent, len(inside))
                barrier.wait()  # only passes if all 4 are inside together
                with inside_lock:
                    inside.pop()

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(TIMEOUT)
        self.assertEqual(max_concurrent, 4)

    def test_queued_writer_does_not_block_already_granted_readers(self):
        lock = FairRWLock()
        barrier = threading.Barrier(3, timeout=TIMEOUT)

        def reader():
            with lock.read_lock():
                barrier.wait()

        threads = [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(TIMEOUT)  # would time out if readers were serialized


class MutualExclusionStressTests(unittest.TestCase):
    """Invariant: never (writers > 1) and never (writer && readers)."""

    def test_mixed_workload_never_overlaps(self):
        lock = FairRWLock()
        monitor = threading.Lock()
        active_readers = 0
        active_writers = 0
        violations = []

        def observe_read():
            nonlocal active_readers
            with monitor:
                active_readers += 1
                if active_writers:
                    violations.append("reader overlapped writer")
            yield
            with monitor:
                active_readers -= 1

        def observe_write():
            nonlocal active_writers
            with monitor:
                active_writers += 1
                if active_writers > 1 or active_readers:
                    violations.append("writer overlapped")
            yield
            with monitor:
                active_writers -= 1

        def reader():
            for _ in range(100):
                lock.acquire_read()
                obs = observe_read()
                next(obs)
                time.sleep(0)  # widen any overlap window
                next(obs, None)
                lock.release_read()

        def writer():
            for _ in range(25):
                lock.acquire_write()
                obs = observe_write()
                next(obs)
                time.sleep(0)
                next(obs, None)
                lock.release_write()

        threads = [threading.Thread(target=reader) for _ in range(6)]
        threads += [threading.Thread(target=writer) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(TIMEOUT * 4)
            self.assertFalse(t.is_alive(), "lock deadlocked")
        self.assertEqual(violations, [])


class ErrorPathTests(unittest.TestCase):
    def test_release_without_acquire_raises(self):
        for cls in (CounterRWLock, FairRWLock):
            lock = cls()
            with self.assertRaises(RuntimeError):
                lock.release_read()
            with self.assertRaises(RuntimeError):
                lock.release_write()


if __name__ == "__main__":
    unittest.main()
