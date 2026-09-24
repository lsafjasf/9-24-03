# 读写锁协议说明

## 修复前的协议

### 旧实现 `legacy_rwlock.CounterRWLock`（读者计数器协议）

- 读者加锁：只要当前没有**活跃**写者（`_writer_present == False`）就进入，`_readers += 1`；不看是否有写者正在排队。
- 写者加锁：等到 `_writer_present == False` 且 `_readers == 0` 才置位 `_writer_present`。
- 缺陷 1（写者饥饿）：写者的等待条件要求读者计数归零，而新读者只检查"有没有活跃写者"。只要读者流不断（前一个读者释放前下一个读者已进入），`_readers` 永远大于 0，写者的等待条件永远为假，被无限期饿死。
- 缺陷 2（双写者窗口）：`acquire_write` 在条件锁内完成"准入判定"（等待循环退出），却在**释放锁之后**才发布 `_writer_present = True`。释放与发布之间的窗口里，第二个写者看到 `_writer_present == False` 且 `_readers == 0`，同样通过准入判定，于是两个写者同时进入临界区。`test_repro.py` 用 `wait_hook` / `before_writer_published` 钩子把这个窗口确定性地撑开复现。

### 修复后 `rwlock.FairRWLock`（FIFO 票号 + 相邻读者批量放行）

- 每个加锁请求（读或写）取一个全局递增的票号 `_next_ticket`；`_granted_to` 指向当前可被准入的票号。
- 读者：登记 `_pending_readers[ticket]` 后等待自己的票被 `_pump` 授予。
- 写者：等待 `ticket == _granted_to` **且** `_active_readers == 0` **且** `not _active_writer`，三者同时满足才进入，然后 `_granted_to += 1`、`_active_writer = True`。
- `_pump`：在没有活跃写者时，把队首连续的读者票一次性批量授予（每授予一张读者票立即 `_active_readers += 1`），遇到写者票即停。

## 关键修复点

1. **写者等待条件补上 `_active_writer`**：准入判定（等待条件）与状态发布（`_active_writer = True`）在同一把条件锁的同一个临界区内原子完成，不存在"判定通过但状态未发布"的窗口，彻底消除双写者。
2. **读者计数在授予时（`_pump` 内）增加**，而不是读者线程被唤醒之后：写者看到 `_active_readers == 0` 时，不存在"已授予但还没计数"的读者，读写不会重叠。
3. **FIFO 票号消除写者饥饿**：写者取票后，排在它后面的读者票号更大，`_pump` 遇到写者票即停，新读者无法插队；写者只需等排在自己前面的有限个持有者释放，等待时间有界。
4. **读并发不受影响**：相邻的读者票仍被 `_pump` 批量授予，读者之间不串行化（`test_readers_run_concurrently` 用 4 路 barrier 验证）。

## 为什么不会引入死锁

- **单锁**：所有状态变更与等待都在同一把 `Condition` 锁内，不存在多锁顺序问题。
- **等待必有唤醒**：每次状态变更（授予、释放读、释放写）都会调用 `_pump`，`_pump` 末尾 `notify_all`，所有等待者都能被唤醒重检条件。
- **队首永远可推进**：`_granted_to` 指向的队首票，要么是读者票（无活跃写者时立即授予），要么是写者票（等当前持有者释放后即可授予）。持有者只释放、不再获取，因此不存在循环等待。
- **前提**：同一线程不得递归/重入加锁（读者持锁时再取读锁、或任何重入写锁），这与绝大多数 RW 锁（如 `pthread_rwlock` 默认行为）一致。

## 运行命令

```bash
# 缺陷复现（针对保留的遗留实现 legacy_rwlock.CounterRWLock）
python3 -m unittest test_repro -v

# 修复后实现的回归测试（互斥、防饥饿、读并发、压力不变量、错误路径）
python3 -m unittest test_rwlock -v
```
