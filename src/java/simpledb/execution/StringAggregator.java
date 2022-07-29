package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldType;
    private final TupleDesc td; // 根据gbfield创建的TupleDesc
    // https://wenku.baidu.com/view/627f3a07bfd126fff705cc1755270722182e5947.html
    // 分组列作为key，聚合列作为value
    private final Map<Field, StringAggregator.AggResult> results = new HashMap<>();

    private static class AggResult {
        private int count;

        public void merge() {
            count ++;
        }

        public int intResult() {
            return count;
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("This operator cannot be handled");
        }
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;

        if (gbfield != Aggregator.NO_GROUPING) {
            // 返回的TupleDesc有分组列和聚合列
            this.td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});
        } else {
            // 只有聚合列
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field keyField = null;
        if (gbfield != Aggregator.NO_GROUPING) {
            keyField = tup.getField(gbfield);
        }
        // 如果key有对应的value，则把值合并到那个value中，否则new一个新的value合并值进去
        // computeIfAbsent: 如果 key 对应的 value 不存在，则使用获取 remappingFunction
        // 重新计算后的值，并保存为该 key 的 value，否则返回 value
        results.computeIfAbsent(keyField, key -> new StringAggregator.AggResult()).merge();
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>(results.size());
        for (Map.Entry<Field, StringAggregator.AggResult> entry: results.entrySet()) {
            Tuple tuple = new Tuple(td);
            if (gbfield != Aggregator.NO_GROUPING) {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue().intResult()));
            } else {
                tuple.setField(0, new IntField(entry.getValue().intResult()));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
