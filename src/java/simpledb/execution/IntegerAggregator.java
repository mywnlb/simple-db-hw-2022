package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 *
 * 支持的聚合操作：MIN, MAX, SUM, AVG, COUNT
 *
 * 示例：
 * SELECT department, AVG(salary) FROM employees GROUP BY department
 *
 * ┌────────────┬─────────┐
 * │ department │ salary  │
 * ├────────────┼─────────┤      聚合后：
 * │ Engineering│ 100000  │      ┌────────────┬─────────────┐
 * │ Engineering│ 120000  │  ──▶ │ department │ AVG(salary) │
 * │ Sales      │ 80000   │      ├────────────┼─────────────┤
 * │ Sales      │ 90000   │      │ Engineering│ 110000      │
 * │ Engineering│ 110000  │      │ Sales      │ 85000       │
 * └────────────┴─────────┘      └────────────┴─────────────┘
 *
 * MIN Math.min(current, newValue)
 * MAX Math.max(current, newValue)
 * SUM current + newValue
 * COUNT current + 1
 * AVG 维护 sum 和 count，返回 sum / count（整数除法）
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /** 分组字段索引，NO_GROUPING 表示无分组 */
    private final int gbFieldIndex;

    /** 分组字段类型 */
    private final Type gbFieldType;

    /** 聚合字段索引 */
    private final int aggFieldIndex;

    /** 聚合操作类型 */
    private final Op aggOp;

    /**
     * 聚合结果存储
     * Key: 分组字段值 (无分组时使用 null 作为 key)
     * Value: 当前聚合值
     */
    private final Map<Field, Integer> aggResults;

    /**
     * 用于计算 AVG 的计数器
     * Key: 分组字段值
     * Value: 该分组的 tuple 数量
     */
    private final Map<Field, Integer> countMap;

    /**
     * 用于计算 AVG 的求和
     * Key: 分组字段值
     * Value: 该分组的总和
     */
    private final Map<Field, Integer> sumMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     *
     * // SELECT dept, SUM(salary) FROM emp GROUP BY dept
     * IntegerAggregator agg = new IntegerAggregator(
     *     0,              // gbfield: department 在索引 0
     *     Type.INT_TYPE,  // gbfieldtype
     *     1,              // afield: salary 在索引 1
     *     Aggregator.Op.SUM
     * );
     *
     * // 逐个合并 tuple
     * while (child.hasNext()) {
     *     agg.mergeTupleIntoGroup(child.next());
     * }
     *
     * // 获取结果迭代器
     * OpIterator results = agg.iterator();
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
        this.aggOp = what;
        this.aggResults = new HashMap<>();
        this.countMap = new HashMap<>();
        this.sumMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // 获取分组字段值
        Field gbField = null;
        if (gbFieldIndex != NO_GROUPING) {
            gbField = tup.getField(gbFieldIndex);
        }

        // 获取聚合字段值
        IntField aggField = (IntField) tup.getField(aggFieldIndex);
        int aggValue = aggField.getValue();

        // 根据聚合操作类型进行处理
        switch (aggOp) {
            case MIN:
                if (!aggResults.containsKey(gbField)) {
                    aggResults.put(gbField, aggValue);
                } else {
                    aggResults.put(gbField, Math.min(aggResults.get(gbField), aggValue));
                }
                break;

            case MAX:
                if (!aggResults.containsKey(gbField)) {
                    aggResults.put(gbField, aggValue);
                } else {
                    aggResults.put(gbField, Math.max(aggResults.get(gbField), aggValue));
                }
                break;

            case SUM:
                if (!aggResults.containsKey(gbField)) {
                    aggResults.put(gbField, aggValue);
                } else {
                    aggResults.put(gbField, aggResults.get(gbField) + aggValue);
                }
                break;

            case COUNT:
                if (!aggResults.containsKey(gbField)) {
                    aggResults.put(gbField, 1);
                } else {
                    aggResults.put(gbField, aggResults.get(gbField) + 1);
                }
                break;

            case AVG:
                // AVG 需要同时维护 sum 和 count
                if (!sumMap.containsKey(gbField)) {
                    sumMap.put(gbField, aggValue);
                    countMap.put(gbField, 1);
                } else {
                    sumMap.put(gbField, sumMap.get(gbField) + aggValue);
                    countMap.put(gbField, countMap.get(gbField) + 1);
                }
                // 计算当前平均值（整数除法）
                aggResults.put(gbField, sumMap.get(gbField) / countMap.get(gbField));
                break;

            default:
                throw new IllegalArgumentException("Unknown aggregation operator: " + aggOp);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // 构建结果 TupleDesc
        TupleDesc resultTd;
        if (gbFieldIndex == NO_GROUPING) {
            // 无分组：只有聚合值
            resultTd = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            // 有分组：(groupVal, aggregateVal)
            resultTd = new TupleDesc(new Type[] { gbFieldType, Type.INT_TYPE });
        }

        // 构建结果 Tuple 列表
        List<Tuple> tuples = new ArrayList<>();

        for (Map.Entry<Field, Integer> entry : aggResults.entrySet()) {
            Tuple tuple = new Tuple(resultTd);

            if (gbFieldIndex == NO_GROUPING) {
                // 无分组：只设置聚合值
                tuple.setField(0, new IntField(entry.getValue()));
            } else {
                // 有分组：设置分组值和聚合值
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }

            tuples.add(tuple);
        }

        return new TupleIterator(resultTd, tuples);
    }

}
