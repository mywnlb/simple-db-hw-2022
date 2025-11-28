package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * 文件结构：
 * ┌──────────────────────────────────────────────────────────────┐
 * │  Page 0  │  Page 1  │  Page 2  │  ...  │  Page N-1           │
 * └──────────────────────────────────────────────────────────────┘
 *
 * 每个 Page 结构：
 * ┌─────────────────────────────────────────────────────────────┐
 * │  Header (bitmap)  │  Slot 0  │  Slot 1  │  ...  │  Slot M   │
 * └─────────────────────────────────────────────────────────────┘
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /** 磁盘上的文件 */
    private final File file;

    /** 表结构描述 */
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f  the file that stores the on-disk backing store for this heap file.
     * @param td the TupleDesc of the table stored in this DbFile
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;

    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();
        int offset = pageNo * pageSize;

        byte[] data = new byte[pageSize];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // 检查页面是否超出文件范围
            if (offset + pageSize > raf.length()) {
                throw new IllegalArgumentException(
                        "Page number " + pageNo + " is out of range for file " + file.getName());
            }

            raf.seek(offset);
            raf.readFully(data);

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException("Error reading page " + pageNo + " from file " + file.getName(), e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int pageNo = page.getId().getPageNumber();
        int offset = pageNo * pageSize;

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            raf.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> dirtyPages = new ArrayList<>();

        // 1. 尝试在现有页面中找到有空闲slot的页面
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (page.getNumUnusedSlots() > 0) {
                // 找到有空位的页面，插入
                page.insertTuple(t);
                dirtyPages.add(page);
                return dirtyPages;
            } else {
                // 该页面没有空位，释放锁（可选优化，避免持有不必要的锁）
                // 注意：在严格2PL下，通常不会提前释放锁
                // Database.getBufferPool().unsafeReleasePage(tid, pid);
            }
        }

        // 2. 所有现有页面都满了，创建新页面
        HeapPageId newPid = new HeapPageId(getId(), numPages());

        // 创建空页面并写入磁盘
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        writePage(newPage);

        // 通过 BufferPool 获取新页面（这样会正确加锁并加入缓存）
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        page.insertTuple(t);
        dirtyPages.add(page);

        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        List<Page> dirtyPages = new ArrayList<>();

        RecordId rid = t.getRecordId();
        if (rid == null) {
            throw new DbException("Tuple has no RecordId, cannot delete");
        }

        PageId pid = rid.getPageId();

        // 验证该 tuple 属于这个文件
        if (pid.getTableId() != getId()) {
            throw new DbException("Tuple does not belong to this HeapFile");
        }

        // 获取页面（带写锁）
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        // 删除 tuple
        page.deleteTuple(t);
        dirtyPages.add(page);

        return dirtyPages;
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile.
     * The iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @param tid the transaction id
     * @return an iterator over all the tuples stored in this DbFile
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * HeapFile 的迭代器实现
     * 遍历所有页面的所有 tuple
     */
    private class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private int currentPageNo;
        private Iterator<Tuple> currentPageIterator;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.isOpen = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            currentPageNo = 0;
            currentPageIterator = null;

            // 加载第一个页面
            if (numPages() > 0) {
                loadPage(0);
            }
        }

        /**
         * 加载指定页面的迭代器
         */
        private void loadPage(int pageNo) throws DbException, TransactionAbortedException {
            if (pageNo < 0 || pageNo >= numPages()) {
                currentPageIterator = null;
                return;
            }

            HeapPageId pid = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            currentPageIterator = page.iterator();
            currentPageNo = pageNo;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }

            // 当前页面还有数据
            if (currentPageIterator != null && currentPageIterator.hasNext()) {
                return true;
            }

            // 尝试加载下一个有数据的页面
            while (currentPageNo < numPages() - 1) {
                loadPage(currentPageNo + 1);
                if (currentPageIterator != null && currentPageIterator.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException("Iterator is not open");
            }

            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples");
            }

            return currentPageIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("Iterator is not open");
            }

            close();
            open();
        }

        @Override
        public void close() {
            isOpen = false;
            currentPageIterator = null;
            currentPageNo = 0;
        }
    }
}

