package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 * <p>
 * Page 结构：
 * ┌───────────────────────────────────────────────────────────────────┐
 * │  Header (bitmap)      │  Tuple Slots                              │
 * │  ceil(numSlots/8)     │  numSlots * tupleSize bytes               │
 * │  bytes                │                                           │
 * ├───────────────────────┼───────────────────────────────────────────┤
 * │  bit0 bit1 ... bit7   │  Slot0  │  Slot1  │  ...  │  SlotN       │
 * │  [    byte 0     ]    │         │         │       │              │
 * └───────────────────────┴───────────────────────────────────────────┘
 * <p>
 * Header Bitmap 布局（每个 byte）：
 * ┌─────────────────────────────────────┐
 * │ bit0 │ bit1 │ bit2 │ ... │ bit7    │
 * │ LSB                          MSB   │
 * └─────────────────────────────────────┘
 * bit=1 表示对应 slot 被占用，bit=0 表示空闲
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * 脏页标记：记录最后修改该页的事务ID，null表示干净
     */
    private TransactionId dirtyTid;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        // 注意：必须先计算 numSlots，再计算 headerSize
        this.numSlots = calculateNumTuples();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[calculateHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * 计算每页可以存储的 tuple 数量
     * 公式：floor((pageSize * 8) / (tupleSize * 8 + 1))
     * <p>
     * 解释：每个 tuple 需要 tupleSize * 8 bits 存储数据，
     * 加上 1 bit 在 header 中标记是否有效
     */
    private int calculateNumTuples() {
        int pageSize = BufferPool.getPageSize();
        int tupleSize = td.getSize();
        // pageSize * 8 = 总 bit 数
        // tupleSize * 8 + 1 = 每个 tuple 需要的 bit 数（数据 + header中的1bit）
        return (int) Math.floor((pageSize * 8.0) / (tupleSize * 8 + 1));
    }

    /**
     * 计算 header 的字节数
     * 公式：ceil(numSlots / 8)
     */
    private int calculateHeaderSize() {
        return (int) Math.ceil(numSlots / 8.0);
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        return numSlots;

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return numSlots / 8;
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId rid = t.getRecordId();

        // 检查 tuple 是否属于这个页面
        if (rid == null) {
            throw new DbException("Tuple has no RecordId");
        }
        if (!rid.getPageId().equals(pid)) {
            throw new DbException("Tuple is not on this page");
        }

        int slotId = rid.getTupleNumber();

        // 检查 slot 范围
        if (slotId < 0 || slotId >= numSlots) {
            throw new DbException("Invalid slot number: " + slotId);
        }

        // 检查 slot 是否已经是空的
        if (!isSlotUsed(slotId)) {
            throw new DbException("Slot " + slotId + " is already empty");
        }

        // 标记 slot 为空
        markSlotUsed(slotId, false);

        // 清空 tuple
        tuples[slotId] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // 检查 TupleDesc 是否匹配
        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("TupleDesc mismatch");
        }

        // 寻找空闲 slot
        int emptySlot = -1;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                emptySlot = i;
                break;
            }
        }

        if (emptySlot == -1) {
            throw new DbException("Page is full, no empty slots");
        }

        // 插入 tuple
        tuples[emptySlot] = t;

        // 设置 RecordId
        t.setRecordId(new RecordId(pid, emptySlot));

        // 标记 slot 为已使用
        markSlotUsed(emptySlot, true);
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        if (dirty) {
            this.dirtyTid = tid;
        } else {
            this.dirtyTid = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return dirtyTid;
    }

    /**
     * Returns the number of unused (i.e., empty) slots on this page.
     */
    public int getNumUnusedSlots() {
        int count = 0;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        if (i < 0 || i >= numSlots) {
            return false;
        }

        int byteIndex = i / 8;
        int bitIndex = i % 8;

        // 使用位运算检查对应位是否为1
        // (header[byteIndex] >> bitIndex) & 1
        return ((header[byteIndex] >> bitIndex) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        if (i < 0 || i >= numSlots) {
            return;
        }

        int byteIndex = i / 8;
        int bitIndex = i % 8;

        if (value) {
            // 设置位为1：使用 OR 操作
            header[byteIndex] |= (1 << bitIndex);
        } else {
            // 设置位为0：使用 AND NOT 操作
            header[byteIndex] &= ~(1 << bitIndex);
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // 收集所有有效的 tuple
        List<Tuple> validTuples = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i) && tuples[i] != null) {
                validTuples.add(tuples[i]);
            }
        }
        return validTuples.iterator();
    }

}

