package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 *  * 执行计划示例：
 *  * ┌─────────────────────────────────────┐
 *  * │            Query Plan               │
 *  * │                                     │
 *  * │         ┌───────────┐               │
 *  * │         │  Project  │               │
 *  * │         └─────┬─────┘               │
 *  * │               │                     │
 *  * │         ┌─────▼─────┐               │
 *  * │         │  Filter   │               │
 *  * │         └─────┬─────┘               │
 *  * │               │                     │
 *  * │         ┌─────▼─────┐               │
 *  * │         │  SeqScan  │  ← 最底层     │
 *  * │         │ (table t) │               │
 *  * │         └───────────┘               │
 *  * └─────────────────────────────────────┘
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    /** 事务ID */
    private final TransactionId tid;

    /** 表ID */
    private int tableId;

    /** 表别名 */
    private String tableAlias;

    /** 底层文件迭代器 */
    private DbFileIterator dbFileIterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;

    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFileIterator = dbFile.iterator(tid);
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc originalTd = Database.getCatalog().getTupleDesc(tableId);
        int numFields = originalTd.numFields();

        Type[] types = new Type[numFields];
        String[] fieldNames = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            types[i] = originalTd.getFieldType(i);
            // 格式: tableAlias.fieldName
            String originalName = originalTd.getFieldName(i);
            fieldNames[i] = tableAlias + "." + originalName;
        }

        return new TupleDesc(types, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (dbFileIterator == null) {
            return false;
        }
        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (dbFileIterator == null) {
            throw new NoSuchElementException("Iterator not open");
        }

        Tuple tuple = dbFileIterator.next();
        if (tuple == null) {
            throw new NoSuchElementException("No more tuples");
        }
        return tuple;
    }

    public void close() {
        if (dbFileIterator != null) {
            dbFileIterator.close();
            dbFileIterator = null;
        }
    }

    /**
     * 重置迭代器到开头
     * @throws DbException
     * @throws NoSuchElementException
     * @throws TransactionAbortedException
     */
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (dbFileIterator != null) {
            dbFileIterator.rewind();
        }
    }
}
