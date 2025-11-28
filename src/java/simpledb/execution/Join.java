package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 *
 * 实现 Nested Loop Join（嵌套循环连接）算法：
 *
 * for each tuple t1 in child1 (outer/left):
 *     for each tuple t2 in child2 (inner/right):
 *         if predicate.filter(t1, t2):
 *             emit (t1, t2)
 *
 * 示例：
 * SELECT * FROM students s, enrollments e WHERE s.id = e.student_id
 *
 * students (child1):        enrollments (child2):
 * ┌────┬───────┐            ┌────────────┬──────────┐
 * │ id │ name  │            │ student_id │ course   │
 * ├────┼───────┤            ├────────────┼──────────┤
 * │ 1  │ Alice │            │ 1          │ CS101    │
 * │ 2  │ Bob   │            │ 1          │ CS102    │
 * └────┴───────┘            │ 2          │ CS101    │
 *                           └────────────┴──────────┘
 *
 * 结果 (id=student_id):
 * ┌────┬───────┬────────────┬──────────┐
 * │ id │ name  │ student_id │ course   │
 * ├────┼───────┼────────────┼──────────┤
 * │ 1  │ Alice │ 1          │ CS101    │
 * │ 1  │ Alice │ 1          │ CS102    │
 * │ 2  │ Bob   │ 2          │ CS101    │
 * └────┴───────┴────────────┴──────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                    Nested Loop Join                             │
 * │                                                                 │
 * │   for each tuple t1 in child1 (outer):     ← 外层循环          │
 * │       for each tuple t2 in child2 (inner): ← 内层循环          │
 * │           if predicate.filter(t1, t2):                         │
 * │               emit merge(t1, t2)                               │
 * │                                                                 │
 * │   时间复杂度: O(n × m)                                          │
 * │   n = child1 的 tuple 数，m = child2 的 tuple 数               │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * fetchNext() 调用流程：
 *
 * ┌─────────────────────────────────────────────────────────────┐
 * │  currentTuple1 == null?                                     │
 * │       │                                                     │
 * │   Yes ▼                                                     │
 * │  ┌─────────────────────────────┐                           │
 * │  │ currentTuple1 = child1.next()│                           │
 * │  │ child2.rewind()              │  ← 重置内表               │
 * │  └─────────────────────────────┘                           │
 * │       │                                                     │
 * │       ▼                                                     │
 * │  ┌─────────────────────────────┐                           │
 * │  │ while (child2.hasNext())    │  ← 内层循环               │
 * │  │   tuple2 = child2.next()    │                           │
 * │  │   if (predicate.filter(...))│                           │
 * │  │       return merge(t1, t2)  │  ← 找到匹配，返回         │
 * │  └─────────────────────────────┘                           │
 * │       │                                                     │
 * │       ▼ 内层循环结束                                        │
 * │  currentTuple1 = null          ← 准备获取下一个外表 tuple   │
 * │       │                                                     │
 * │       ▼ 继续外层循环...                                     │
 * └─────────────────────────────────────────────────────────────┘
 *
 * SELECT * FROM students s, enrollments e WHERE s.id = e.student_id
 * ```
 * ```
 * child1 (students):          child2 (enrollments):
 * ┌────┬───────┐              ┌────────────┬──────────┐
 * │ id │ name  │              │ student_id │ course   │
 * ├────┼───────┤              ├────────────┼──────────┤
 * │ 1  │ Alice │              │ 1          │ CS101    │
 * │ 2  │ Bob   │              │ 1          │ CS102    │
 * └────┴───────┘              │ 2          │ CS101    │
 *                             └────────────┴──────────┘
 *
 * 执行过程：
 * ┌──────────────────────────────────────────────────────────────┐
 * │ t1=(1,Alice), t2=(1,CS101) → match! → emit (1,Alice,1,CS101) │
 * │ t1=(1,Alice), t2=(1,CS102) → match! → emit (1,Alice,1,CS102) │
 * │ t1=(1,Alice), t2=(2,CS101) → no match                        │
 * │ t1=(2,Bob),   t2=(1,CS101) → no match  (child2.rewind())     │
 * │ t1=(2,Bob),   t2=(1,CS102) → no match                        │
 * │ t1=(2,Bob),   t2=(2,CS101) → match! → emit (2,Bob,2,CS101)   │
 * └──────────────────────────────────────────────────────────────┘
 *
 * 结果：
 * ┌────┬───────┬────────────┬──────────┐
 * │ id │ name  │ student_id │ course   │
 * ├────┼───────┼────────────┼──────────┤
 * │ 1  │ Alice │ 1          │ CS101    │
 * │ 1  │ Alice │ 1          │ CS102    │
 * │ 2  │ Bob   │ 2          │ CS101    │
 * └────┴───────┴────────────┴──────────┘
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    /** 连接谓词 */
    private final JoinPredicate joinPredicate;

    /** 左表（外表）迭代器 */
    private OpIterator child1;

    /** 右表（内表）迭代器 */
    private OpIterator child2;

    /** 当前左表 tuple（用于嵌套循环） */
    private Tuple currentTuple1;

    /** 结果 TupleDesc */
    private TupleDesc resultTd;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.currentTuple1 = null;
        // 合并两个表的 TupleDesc
        this.resultTd = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return joinPredicate;

    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        int fieldIndex = joinPredicate.getField1();
        return child1.getTupleDesc().getFieldName(fieldIndex);
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        int fieldIndex = joinPredicate.getField2();
        return child2.getTupleDesc().getFieldName(fieldIndex);
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return resultTd;

    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        currentTuple1 = null;
    }

    public void close() {
        child1.close();
        child2.close();
        currentTuple1 = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        currentTuple1 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Nested Loop Join 实现
        // 外层循环：遍历 child1
        // 内层循环：遍历 child2

        while (currentTuple1 != null || child1.hasNext()) {
            // 如果当前没有外表 tuple，获取下一个
            if (currentTuple1 == null) {
                currentTuple1 = child1.next();
                // 重置内表迭代器，准备新一轮内层循环
                child2.rewind();
            }

            // 内层循环：遍历 child2
            while (child2.hasNext()) {
                Tuple tuple2 = child2.next();

                // 检查是否满足连接条件
                if (joinPredicate.filter(currentTuple1, tuple2)) {
                    // 满足条件，合并两个 tuple 并返回
                    return mergeTuples(currentTuple1, tuple2);
                }
            }

            // 内层循环结束，准备获取下一个外表 tuple
            currentTuple1 = null;
        }

        // 没有更多匹配的 tuple
        return null;
    }

    /**
     * 合并两个 tuple
     *
     * @param t1 左表 tuple
     * @param t2 右表 tuple
     * @return 合并后的 tuple
     */
    private Tuple mergeTuples(Tuple t1, Tuple t2) {
        Tuple result = new Tuple(resultTd);

        int index = 0;

        // 复制 t1 的所有字段
        for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
            result.setField(index++, t1.getField(i));
        }

        // 复制 t2 的所有字段
        for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
            result.setField(index++, t2.getField(i));
        }

        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child1, child2 };

    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children != null && children.length >= 2) {
            this.child1 = children[0];
            this.child2 = children[1];
            this.resultTd = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        }
    }

}
