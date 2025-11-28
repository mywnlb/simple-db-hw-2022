package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 * SELECT * FROM students WHERE age > 20
 * * 对应 SQL 中的 WHERE 子句：
 *  * SELECT * FROM students WHERE age > 20
 *  *
 *  * 执行计划：
 *  * ┌─────────────────┐
 *  * │     Filter      │  ← 过滤 age > 20
 *  * │  (age > 20)     │
 *  * └────────┬────────┘
 *  *          │ 满足条件的 tuple
 *  *          │
 *  * ┌────────▼────────┐
 *  * │    SeqScan      │  ← 读取所有 tuple
 *  * │   (students)    │
 *  * └─────────────────┘
 * ┌─────────────────────────────────────────────────────┐
 * │                    Filter                           │
 * │                                                     │
 * │   fetchNext():                                      │
 * │   ┌─────────────────────────────────────────┐      │
 * │   │ while (child.hasNext())                 │      │
 * │   │     tuple = child.next()                │      │
 * │   │     if (predicate.filter(tuple))  ──────┼──▶ return tuple
 * │   │         return tuple                    │      │
 * │   │ return null  (没有更多数据)             │      │
 * │   └─────────────────────────────────────────┘      │
 * │                      │                              │
 * │                      │ 获取下一个 tuple              │
 * │                      ▼                              │
 * │   ┌─────────────────────────────────────────┐      │
 * │   │            child (SeqScan)              │      │
 * │   └─────────────────────────────────────────┘      │
 * └─────────────────────────────────────────────────────┘
 * ┌─────────────────────────────────────────┐
 * │           Operator (abstract)           │
 * ├─────────────────────────────────────────┤
 * │  - next(): 调用 fetchNext()             │
 * │  - hasNext(): 检查是否有缓存的 tuple     │
 * │  - open()/close(): 管理状态             │
 * ├─────────────────────────────────────────┤
 * │  # fetchNext(): 子类实现                │
 * └─────────────────────────────────────────┘
 *            △
 *            │ extends
 *            │
 * ┌─────────────────────────────────────────┐
 * │              Filter                     │
 * ├─────────────────────────────────────────┤
 * │  - predicate: Predicate                 │
 * │  - child: OpIterator                    │
 * ├─────────────────────────────────────────┤
 * │  # fetchNext(): 过滤逻辑                │
 * └─────────────────────────────────────────┘
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    /** 过滤谓词 */
    private final Predicate predicate;

    /** 子操作符（数据来源） */
    private OpIterator child;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return predicate;

    }

    public TupleDesc getTupleDesc() {
        // Filter 不改变 tuple 的结构，直接返回子操作符的 TupleDesc
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // 先调用父类的 open
        super.open();
        // 再打开子操作符
        child.open();
    }

    public void close() {
        // 先关闭子操作符
        child.close();
        // 再调用父类的 close
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // 重置子操作符
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // 从子操作符中不断读取 tuple，直到找到满足谓词的
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (predicate.filter(tuple)) {
                return tuple;
            }
        }
        // 没有更多满足条件的 tuple
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child };

    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children != null && children.length > 0) {
            this.child = children[0];
        }
    }

}
