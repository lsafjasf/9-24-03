import threading
import unittest

from legacy_rwlock import CounterRWLock


def wait_for(predicate, message, timeout=2.0):
    deadline = threading.TIMEOUT_MAX if timeout is None else timeout
    waited = 0.0
    step = 0.001
    while waited < deadline:
        if predicate():
            return
        threading.Event().wait(step)
        waited += step
    raise AssertionError(message)


class CounterRWLockDefectTest(unittest.TestCase):
    def test_continuous_readers_starve_waiting_writer(self):
        lock = CounterRWLock()
        writer_entered = threading.Event()

        lock.acquire_read()
        writer = threading.Thread(
            target=lambda: (lock.acquire_write(), writer_entered.set()),
            daemon=True,
        )
        writer.start()
        wait_for(lambda: lock.waiting_writers == 1, "writer did not queue")

        generations = 5
        for _ in range(generations):
            next_reader = threading.Thread(target=lock.acquire_read, daemon=True)
            next_reader.start()
            next_reader.join(1.0)
            self.assertFalse(next_reader.is_alive(), "old protocol blocked a fresh reader")
            lock.release_read()
            self.assertEqual(lock.waiting_writers, 1)

        threading.Event().wait(0.05)
        self.assertFalse(
            writer_entered.is_set(),
            "writer entered even though readers continuously replaced each other",
        )

        lock.release_read()
        self.assertTrue(writer_entered.wait(1.0), "writer did not enter after readers drained")

    def test_two_writers_can_enter_during_release_window(self):
        wake_writers = threading.Event()
        admission_barrier = threading.Barrier(2)
        overlap_barrier = threading.Barrier(3)
        lock = CounterRWLock(
            wait_hook=wake_writers.wait,
            before_writer_published=admission_barrier.wait,
        )

        reader = threading.Thread(target=lock.acquire_read, daemon=True)
        reader.start()
        reader.join(1.0)

        def writer():
            lock.acquire_write()
            overlap_barrier.wait()

        first_writer = threading.Thread(target=writer, daemon=True)
        second_writer = threading.Thread(target=writer, daemon=True)
        first_writer.start()
        second_writer.start()
        wait_for(lambda: lock.waiting_writers == 2, "both writers did not queue")

        lock.release_read()
        wake_writers.set()

        try:
            overlap_barrier.wait(timeout=2.0)
        except threading.BrokenBarrierError:
            self.fail("the second writer was excluded; release window was not reproduced")


if __name__ == "__main__":
    unittest.main(verbosity=2)
