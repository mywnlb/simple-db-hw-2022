package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 *
 * 注意：StringAggregator 只支持 COUNT 操作
 * （字符串字段不能进行 MIN、MAX、SUM、AVG 等数值操作）
 *
 * 示例：
 * SELECT department, COUNT(name) FROM employees GROUP BY department
 *
 * ┌────────────┬─────────┐
 * │ department │ name    │
 * ├────────────┼─────────┤      聚合后：
 * │ Engineering│ Alice   │      ┌────────────┬──────────────┐
 * │ Engineering│ Bob     │  ──▶ │ department │ COUNT(name)  │
 * │ Sales      │ Charlie │      ├────────────┼──────────────┤
 * │ Sales      │ David   │      │ Engineering│ 3            │
 * │ Engineering│ Eve     │      │ Sales      │ 2            │
 * └────────────┴─────────┘      └────────────┴──────────────┘
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /** 分组字段索引，NO_GROUPING 表示无分组 */
    private final int gbFieldIndex;

    /** 分组字段类型 */
    private final Type gbFieldType;

    /** 聚合字段索引 */
    private final int aggFieldIndex;

    /** 聚合操作类型（只支持 COUNT） */
    private final Op aggOp;

    /**
     * 聚合结果存储
     * Key: 分组字段值 (无分组时使用 null 作为 key)
     * Value: COUNT 值
     */
    private final Map<Field, Integer> countMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT operation");
        }

        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
        this.aggOp = what;
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // 获取分组字段值
        Field gbField = null;
        if (gbFieldIndex != NO_GROUPING) {
            gbField = tup.getField(gbFieldIndex);
        }

        // 只支持 COUNT，直接增加计数
        if (!countMap.containsKey(gbField)) {
            countMap.put(gbField, 1);
        } else {
            countMap.put(gbField, countMap.get(gbField) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
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

        for (Map.Entry<Field, Integer> entry : countMap.entrySet()) {
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