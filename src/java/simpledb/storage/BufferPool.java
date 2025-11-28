package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;


    /** 最大页面数量 */
    private final int numPages;

    /** 页面缓存：PageId -> Page */
    private final Map<PageId, Page> pageCache;

    /** LRU顺序记录：使用LinkedList维护访问顺序 */
    private final LinkedList<PageId> lruList;

    /** 锁管理器 */
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
        this.lruList = new LinkedList<>();
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // 1. 获取锁（Lab4）
        LockManager.LockType lockType = (perm == Permissions.READ_ONLY)
                ? LockManager.LockType.SHARED
                : LockManager.LockType.EXCLUSIVE;

        boolean acquired = lockManager.acquireLock(tid, pid, lockType, 500); // 500ms超时
        if (!acquired) {
            throw new TransactionAbortedException();
        }

        // 2. 从缓存获取
        synchronized (this) {
            if (pageCache.containsKey(pid)) {
                // 更新LRU顺序
                lruList.remove(pid);
                lruList.addLast(pid);
                return pageCache.get(pid);
            }

            // 3. 缓存满了需要驱逐
            if (pageCache.size() >= numPages) {
                evictPage();
            }

            // 4. 从磁盘读取
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);

            // 5. 加入缓存
            pageCache.put(pid, page);
            lruList.addLast(pid);

            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // 获取该事务持有锁的所有页面
        Set<PageId> lockedPages = lockManager.getLockedPages(tid);

        if (commit) {
            // FORCE策略：提交时刷所有脏页到磁盘
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // NO-STEAL策略：回滚时丢弃内存中的修改
            synchronized (this) {
                for (PageId pid : lockedPages) {
                    Page page = pageCache.get(pid);
                    if (page != null && page.isDirty() != null) {
                        // 从磁盘重新读取干净的页面
                        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                        Page cleanPage = dbFile.readPage(pid);
                        pageCache.put(pid, cleanPage);
                    }
                }
            }
        }

        // 释放所有锁
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);

        synchronized (this) {
            for (Page page : dirtyPages) {
                page.markDirty(true, tid);

                PageId pid = page.getId();
                // 更新缓存
                if (pageCache.containsKey(pid)) {
                    lruList.remove(pid);
                }
                pageCache.put(pid, page);
                lruList.addLast(pid);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);

        synchronized (this) {
            for (Page page : dirtyPages) {
                page.markDirty(true, tid);

                PageId pid = page.getId();
                // 更新缓存
                if (pageCache.containsKey(pid)) {
                    lruList.remove(pid);
                }
                pageCache.put(pid, page);
                lruList.addLast(pid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pageCache.keySet()) {
            flushPage(pid);
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        pageCache.remove(pid);
        lruList.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pageCache.get(pid);
        if (page == null) {
            return;
        }

        // 只刷脏页
        TransactionId dirtyTid = page.isDirty();
        if (dirtyTid != null) {
            // 写入磁盘
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
            // 标记为干净
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : pageCache.keySet()) {
            Page page = pageCache.get(pid);
            if (page != null && tid.equals(page.isDirty())) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // 从LRU列表头部开始找（最久未使用的）
        PageId evictPid = null;

        for (PageId pid : lruList) {
            Page page = pageCache.get(pid);
            // NO-STEAL: 不能驱逐脏页
            if (page != null && page.isDirty() == null) {
                evictPid = pid;
                break;
            }
        }

        if (evictPid == null) {
            throw new DbException("All pages in buffer pool are dirty, cannot evict");
        }

        // 驱逐页面（已经是干净的，不需要刷盘）
        pageCache.remove(evictPid);
        lruList.remove(evictPid);
    }

}
