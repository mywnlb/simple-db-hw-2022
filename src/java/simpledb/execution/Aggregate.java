package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 *
 * 对应 SQL 中的聚合操作：
 * SELECT department, AVG(salary) FROM employees GROUP BY department
 *
 * 执行计划：
 * ┌─────────────────────┐
 * │     Aggregate       │  ← 聚合操作
 * │ (gfield=0, afield=1)│
 * │ (op=AVG)            │
 * └──────────┬──────────┘
 *            │
 * ┌──────────▼──────────┐
 * │      SeqScan        │
 * │    (employees)      │
 * └─────────────────────┘
 *┌─────────────────────────────────────────────────────────────┐
 * │                      Aggregate                              │
 * │  ┌───────────────────────────────────────────────────────┐ │
 * │  │  1. 根据字段类型选择 Aggregator                        │ │
 * │  │     INT_TYPE  → IntegerAggregator                     │ │
 * │  │     STRING_TYPE → StringAggregator                    │ │
 * │  │                                                       │ │
 * │  │  2. open() 时遍历子操作符，合并到 Aggregator          │ │
 * │  │                                                       │ │
 * │  │  3. fetchNext() 从 Aggregator.iterator() 返回结果    │ │
 * │  └───────────────────────────────────────────────────────┘ │
 * └─────────────────────────────────────────────────────────────┘
 *
 * ┌────────────────────────────────────────────────────────────────┐
 * │  SELECT dept, AVG(salary) FROM emp GROUP BY dept               │
 * └────────────────────────────────────────────────────────────────┘
 *                               │
 *                               ▼
 * ┌────────────────────────────────────────────────────────────────┐
 * │                     Aggregate.open()                           │
 * │  ┌──────────────────────────────────────────────────────────┐ │
 * │  │  child.open()                                             │ │
 * │  │                                                           │ │
 * │  │  while (child.hasNext())                                  │ │
 * │  │      aggregator.mergeTupleIntoGroup(child.next())         │ │
 * │  │                                                           │ │
 * │  │  aggregateIterator = aggregator.iterator()                │ │
 * │  │  aggregateIterator.open()                                 │ │
 * │  └──────────────────────────────────────────────────────────┘ │
 * └────────────────────────────────────────────────────────────────┘
 *                               │
 *                               ▼
 * ┌────────────────────────────────────────────────────────────────┐
 * │                   Aggregate.fetchNext()                        │
 * │  ┌──────────────────────────────────────────────────────────┐ │
 * │  │  return aggregateIterator.next()                          │ │
 * │  └──────────────────────────────────────────────────────────┘ │
 * └────────────────────────────────────────────────────────────────┘
 *
 * 有分组时：
 * ┌──────────────────────┬─────────────────────────┐
 * │ groupFieldName       │ OP(aggFieldName)        │
 * │ (原类型)             │ (INT_TYPE)              │
 * └──────────────────────┴─────────────────────────┘
 * 例如：department, AVG(salary)
 *
 * 无分组时：
 * ┌─────────────────────────┐
 * │ OP(aggFieldName)        │
 * │ (INT_TYPE)              │
 * └─────────────────────────┘
 * 例如：COUNT(name)
 * 工作流程：
 * 1. open() 时，遍历所有子 tuple，合并到 Aggregator 中
 * 2. fetchNext() 时，从 Aggregator 的 iterator 返回结果
 *
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                        职责分离                                      │
 * ├─────────────────────────────────┬───────────────────────────────────┤
 * │   IntegerAggregator             │          Aggregate                │
 * │   StringAggregator              │                                   │
 * ├─────────────────────────────────┼───────────────────────────────────┤
 * │   "如何计算"                     │   "如何获取数据 + 何时计算"       │
 * │                                 │                                   │
 * │   • 纯粹的聚合算法               │   • 是一个 Operator              │
 * │   • 不知道数据从哪来             │   • 可以放入执行计划树            │
 * │   • 只管 merge 和计算            │   • 实现 OpIterator 接口         │
 * │   • 不是 Operator               │   • 协调数据流                    │
 * └─────────────────────────────────┴───────────────────────────────────┘
 *
 * SELECT department, AVG(salary)
 * FROM employees
 * WHERE age > 30
 * GROUP BY department
 * ```
 * ```
 * 执行计划树（只有 Operator 才能组成）：
 *
 *         ┌─────────────┐
 *         │  Aggregate  │  ← Operator，可以连接其他节点
 *         └──────┬──────┘
 *                │
 *         ┌──────▼──────┐
 *         │   Filter    │  ← Operator
 *         └──────┬──────┘
 *                │
 *         ┌──────▼──────┐
 *         │   SeqScan   │  ← Operator
 *         └─────────────┘
 *
 * IntegerAggregator 不是 Operator，无法放入这个树！
 * 它只是 Aggregate 内部使用的"计算引擎"。
 * ```
 *
 * ## 策略模式
 * ```
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                     Strategy Pattern                            │
 * │                                                                 │
 * │   ┌─────────────────┐                                          │
 * │   │    Aggregate    │  ← Context (上下文)                       │
 * │   │   (Operator)    │                                          │
 * │   └────────┬────────┘                                          │
 * │            │ uses                                               │
 * │            ▼                                                    │
 * │   ┌─────────────────┐                                          │
 * │   │   Aggregator    │  ← Strategy Interface (策略接口)         │
 * │   │   (interface)   │                                          │
 * │   └────────┬────────┘                                          │
 * │            │                                                    │
 * │      ┌─────┴─────┐                                             │
 * │      ▼           ▼                                             │
 * │ ┌─────────┐ ┌─────────┐                                        │
 * │ │ Integer │ │ String  │  ← Concrete Strategies (具体策略)      │
 * │ │Aggregator│ │Aggregator│                                       │
 * │ └─────────┘ └─────────┘                                        │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * // ❌ 没有 Aggregate，代码会很丑陋
 * SeqScan scan = new SeqScan(tid, tableId);
 * Filter filter = new Filter(predicate, scan);
 *
 * // 必须手动遍历，无法融入执行计划
 * IntegerAggregator agg = new IntegerAggregator(0, Type.INT_TYPE, 1, Op.AVG);
 * filter.open();
 * while (filter.hasNext()) {
 *     agg.mergeTupleIntoGroup(filter.next());
 * }
 * OpIterator result = agg.iterator();
 *
 * // ❌ 如果还要再套一层 Join 或 Project？代码更复杂...
 *
 *
 *
 * // ✅ 有了 Aggregate，可以无缝组合
 * SeqScan scan = new SeqScan(tid, tableId);
 * Filter filter = new Filter(predicate, scan);
 * Aggregate agg = new Aggregate(filter, 1, 0, Op.AVG);  // 是一个 Operator！
 * Join join = new Join(joinPred, agg, anotherScan);     // 可以继续组合！
 *
 * // 统一的迭代接口
 * join.open();
 * while (join.hasNext()) {
 *     Tuple t = join.next();
 * }
 *
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /** 子操作符（数据来源） */
    private OpIterator child;

    /** 聚合字段索引 */
    private final int aggFieldIndex;

    /** 分组字段索引，-1 表示无分组 */
    private final int groupFieldIndex;

    /** 聚合操作类型 */
    private final Aggregator.Op aggOp;

    /** 聚合器（IntegerAggregator 或 StringAggregator） */
    private Aggregator aggregator;

    /** 聚合结果迭代器 */
    private OpIterator aggregateIterator;

    /** 子操作符的 TupleDesc */
    private TupleDesc childTd;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggFieldIndex = afield;
        this.groupFieldIndex = gfield;
        this.aggOp = aop;
        this.childTd = child.getTupleDesc();

        // 根据聚合字段类型选择合适的 Aggregator
        Type aggFieldType = childTd.getFieldType(afield);
        Type groupFieldType = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);

        if (aggFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, groupFieldType, afield, aop);
        } else {
            // StringField 只支持 COUNT
            this.aggregator = new StringAggregator(gfield, groupFieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return groupFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        if (groupFieldIndex == Aggregator.NO_GROUPING) {
            return null;
        }
        return childTd.getFieldName(groupFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return childTd.getFieldName(aggFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();

        // 打开子操作符
        child.open();

        // 遍历所有子 tuple，合并到 aggregator 中
        while (child.hasNext()) {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }

        // 获取聚合结果迭代器
        aggregateIterator = aggregator.iterator();
        aggregateIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggregateIterator != null && aggregateIterator.hasNext()) {
            return aggregateIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // 重置聚合结果迭代器
        if (aggregateIterator != null) {
            aggregateIterator.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggName = aggOp.toString() + "(" + childTd.getFieldName(aggFieldIndex) + ")";

        if (groupFieldIndex == Aggregator.NO_GROUPING) {
            // 无分组：只有聚合值
            return new TupleDesc(
                    new Type[] { Type.INT_TYPE },
                    new String[] { aggName }
            );
        } else {
            // 有分组：(groupVal, aggregateVal)
            Type groupFieldType = childTd.getFieldType(groupFieldIndex);
            String groupName = childTd.getFieldName(groupFieldIndex);

            return new TupleDesc(
                    new Type[] { groupFieldType, Type.INT_TYPE },
                    new String[] { groupName, aggName }
            );
        }
    }

    public void close() {
        super.close();
        child.close();
        if (aggregateIterator != null) {
            aggregateIterator.close();
            aggregateIterator = null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children != null && children.length > 0) {
            this.child = children[0];
            this.childTd = child.getTupleDesc();
        }
    }
}