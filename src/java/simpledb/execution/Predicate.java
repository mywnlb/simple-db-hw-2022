package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 *
 * 用于 Filter 操作中比较 tuple 的字段与常量值：
 *
 * 示例：
 * SELECT * FROM students WHERE age > 20
 *
 * Predicate: field=1 (age), op=GREATER_THAN, operand=IntField(20)
 *
 * ┌─────────────────────────┐
 * │ students tuple          │
 * ├─────────────────────────┤
 * │ name=Alice, age=25      │
 * └────────────┬────────────┘
 *              │
 *              ▼
 *    tuple.getField(1).compare(GREATER_THAN, IntField(20))
 *              │
 *              ▼
 *          25 > 20 → true
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /** 要比较的字段索引 */
    private final int fieldIndex;

    /** 比较操作符 */
    private final Op op;

    /** 比较的常量值 */
    private final Field operand;

    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against.
     * @param op      operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.fieldIndex = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        return fieldIndex;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        return op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        return operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        Field tupleField = t.getField(fieldIndex);
        return tupleField.compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        return String.format("f = %d op = %s operand = %s", fieldIndex, op.toString(), operand.toString());
    }
}