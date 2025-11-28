package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockManager 管理页面级别的锁
 *
 * 支持两种锁类型：
 * - SHARED (S锁/读锁): 多个事务可以同时持有
 * - EXCLUSIVE (X锁/写锁): 只有一个事务可以持有
 *
 * 锁兼容性矩阵：
 *           | SHARED | EXCLUSIVE |
 * SHARED    |   Y    |     N     |
 * EXCLUSIVE |   N    |     N     |
 *
 * 使用简单的超时机制检测死锁
 */
public class LockManager {

    /** 锁类型枚举 */
    public enum LockType {
        SHARED,
        EXCLUSIVE
    }

    /** 单个页面的锁状态 */
    private static class PageLockState {
        // 持有共享锁的事务集合
        Set<TransactionId> sharedLockHolders = new HashSet<>();
        // 持有排他锁的事务（最多一个）
        TransactionId exclusiveLockHolder = null;
        // 等待获取锁的事务（用于死锁检测）
        Set<TransactionId> waitingTransactions = new HashSet<>();
    }

    /** 页面锁表：PageId -> PageLockState */
    private final Map<PageId, PageLockState> lockTable;

    /** 事务持有的页面：TransactionId -> Set<PageId>，用于快速释放 */
    private final Map<TransactionId, Set<PageId>> transactionLocks;

    public LockManager() {
        this.lockTable = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
    }

    /**
     * 获取指定页面的锁
     *
     * @param tid 事务ID
     * @param pid 页面ID
     * @param lockType 锁类型
     * @param timeoutMs 超时时间（毫秒），用于死锁检测
     * @return 是否成功获取锁
     */
    public boolean acquireLock(TransactionId tid, PageId pid, LockType lockType, long timeoutMs) {
        long startTime = System.currentTimeMillis();

        while (true) {
            synchronized (this) {
                PageLockState state = lockTable.computeIfAbsent(pid, k -> new PageLockState());

                if (lockType == LockType.SHARED) {
                    // 请求共享锁
                    if (tryAcquireSharedLock(tid, pid, state)) {
                        state.waitingTransactions.remove(tid);
                        recordLock(tid, pid);
                        return true;
                    }
                } else {
                    // 请求排他锁
                    if (tryAcquireExclusiveLock(tid, pid, state)) {
                        state.waitingTransactions.remove(tid);
                        recordLock(tid, pid);
                        return true;
                    }
                }

                // 加入等待队列
                state.waitingTransactions.add(tid);

                // 检测死锁：简单超时策略
                if (System.currentTimeMillis() - startTime > timeoutMs) {
                    state.waitingTransactions.remove(tid);
                    return false; // 超时，可能死锁
                }
            }

            // 等待一段时间后重试
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    /**
     * 尝试获取共享锁
     * 条件：没有其他事务持有排他锁（或者排他锁持有者就是自己）
     */
    private boolean tryAcquireSharedLock(TransactionId tid, PageId pid, PageLockState state) {
        // 如果已经持有排他锁，直接成功（排他锁包含共享锁）
        if (tid.equals(state.exclusiveLockHolder)) {
            return true;
        }

        // 如果已经持有共享锁，直接成功
        if (state.sharedLockHolders.contains(tid)) {
            return true;
        }

        // 没有排他锁持有者，可以获取共享锁
        if (state.exclusiveLockHolder == null) {
            state.sharedLockHolders.add(tid);
            return true;
        }

        return false;
    }

    /**
     * 尝试获取排他锁
     * 条件：没有其他事务持有任何锁
     */
    private boolean tryAcquireExclusiveLock(TransactionId tid, PageId pid, PageLockState state) {
        // 如果已经持有排他锁，直接成功
        if (tid.equals(state.exclusiveLockHolder)) {
            return true;
        }

        // 检查是否可以升级：自己持有共享锁且是唯一持有者
        boolean canUpgrade = state.sharedLockHolders.size() == 1
                && state.sharedLockHolders.contains(tid);

        // 没有排他锁，且（没有共享锁 或 可以升级）
        if (state.exclusiveLockHolder == null &&
                (state.sharedLockHolders.isEmpty() || canUpgrade)) {

            // 移除共享锁（如果有的话，升级场景）
            state.sharedLockHolders.remove(tid);
            state.exclusiveLockHolder = tid;
            return true;
        }

        return false;
    }

    /**
     * 记录事务持有的锁
     */
    private void recordLock(TransactionId tid, PageId pid) {
        transactionLocks.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }

    /**
     * 释放指定页面的锁
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        PageLockState state = lockTable.get(pid);
        if (state == null) {
            return;
        }

        // 释放排他锁
        if (tid.equals(state.exclusiveLockHolder)) {
            state.exclusiveLockHolder = null;
        }

        // 释放共享锁
        state.sharedLockHolders.remove(tid);

        // 从事务记录中移除
        Set<PageId> pages = transactionLocks.get(tid);
        if (pages != null) {
            pages.remove(pid);
        }

        // 清理空的锁状态
        if (state.exclusiveLockHolder == null && state.sharedLockHolders.isEmpty()) {
            lockTable.remove(pid);
        }
    }

    /**
     * 释放事务持有的所有锁
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = transactionLocks.remove(tid);
        if (pages == null) {
            return;
        }

        for (PageId pid : pages) {
            PageLockState state = lockTable.get(pid);
            if (state != null) {
                if (tid.equals(state.exclusiveLockHolder)) {
                    state.exclusiveLockHolder = null;
                }
                state.sharedLockHolders.remove(tid);

                // 清理空的锁状态
                if (state.exclusiveLockHolder == null && state.sharedLockHolders.isEmpty()) {
                    lockTable.remove(pid);
                }
            }
        }
    }

    /**
     * 检查事务是否持有指定页面的锁
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        PageLockState state = lockTable.get(pid);
        if (state == null) {
            return false;
        }

        return tid.equals(state.exclusiveLockHolder) || state.sharedLockHolders.contains(tid);
    }

    /**
     * 获取事务持有锁的所有页面
     */
    public synchronized Set<PageId> getLockedPages(TransactionId tid) {
        Set<PageId> pages = transactionLocks.get(tid);
        if (pages == null) {
            return new HashSet<>();
        }
        return new HashSet<>(pages); // 返回副本，避免并发修改
    }

    /**
     * 获取指定页面的锁类型（调试用）
     */
    public synchronized LockType getLockType(TransactionId tid, PageId pid) {
        PageLockState state = lockTable.get(pid);
        if (state == null) {
            return null;
        }

        if (tid.equals(state.exclusiveLockHolder)) {
            return LockType.EXCLUSIVE;
        }

        if (state.sharedLockHolders.contains(tid)) {
            return LockType.SHARED;
        }

        return null;
    }
}