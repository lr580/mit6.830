package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import simpledb.common.Type;
//import simpledb.execution.Aggregator.Op;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private class Result {
        private String ans;
        private int count = 0;

        public void merge(String s) {
            ans = ans == null ? s : ans;
            if (what == Op.COUNT) {
                ++count;
            } else if (what == Op.MAX) {
                if (ans.compareTo(s) < 0) {// ans<s
                    ans = s;
                }
            } else if (what == Op.MIN) {
                if (ans.compareTo(s) > 0) {// ans>s
                    ans = s;
                }
            }
        }

        public Field get() {
            if (what == Op.MAX || what == Op.MIN) {
                return new StringField(ans, Type.STRING_LEN);
            }
            return new IntField(count);
        }
    }

    private final HashMap<Field, Result> groups = new HashMap<>();
    private final int gbfield;
//    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // DONE: some code goes here
        this.gbfield = gbfield;
//        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        Type atype = what == Op.MAX || what == Op.MIN ? Type.STRING_TYPE : Type.INT_TYPE;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { atype });
        } else {
            td = new TupleDesc(new Type[] { gbfieldtype, atype });
        }
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // DONE: some code goes here
        Field key = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        Result origin = groups.get(key);
        if (origin == null) {
            origin = new Result();
        }
        String val = tup.getField(afield).toString();
        origin.merge(val);
        groups.put(key, origin);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
     *         using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public OpIterator iterator() {
        // DONE: some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Result> it : groups.entrySet()) {
            Tuple tuple = new Tuple(td);
            if (afield == NO_GROUPING) {
                tuple.setField(0, it.getValue().get());
            } else {
                tuple.setField(0, it.getKey());
                tuple.setField(1, it.getValue().get());
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
