package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 *
 * 用于 Join 操作中比较两个 tuple 的字段：
 *
 * 示例：
 * SELECT * FROM students s, enrollments e WHERE s.id = e.student_id
 *
 * JoinPredicate: field1=0 (s.id), op=EQUALS, field2=0 (e.student_id)
 *
 * ┌─────────────────┐     ┌──────────────────────┐
 * │ students (t1)   │     │ enrollments (t2)     │
 * ├─────────────────┤     ├──────────────────────┤
 * │ id=1, name=Alice│     │ student_id=1, course │
 * └────────┬────────┘     └──────────┬───────────┘
 *          │                         │
 *          └────────┬────────────────┘
 *                   ▼
 *          t1.getField(0).compare(EQUALS, t2.getField(0))
 *                   │
 *                   ▼
 *               true/false
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 第一个 tuple 中的字段索引 */
    private final int field1;

    /** 第二个 tuple 中的字段索引 */
    private final int field2;

    /** 比较操作符 */
    private final Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); either
     *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *               Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        Field f1 = t1.getField(field1);
        Field f2 = t2.getField(field2);
        return f1.compare(op, f2);
    }

    public int getField1() {
        return field1;
    }

    public int getField2() {
        return field2;
    }

    public Predicate.Op getOperator() {
        return op;
    }
}