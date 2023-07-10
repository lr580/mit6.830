package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    public static abstract class Result implements Cloneable {
        protected int ans = 0;

        public abstract void merge(int val);

        public int get() {
            return ans;
        }

        public Object clone() {
            try {
                return super.clone();
            } catch (CloneNotSupportedException e) {
                return null;
            }
        }
    };

    public static class ResultMin extends Result {
        public ResultMin() {
            ans = Integer.MAX_VALUE;
        }

        public void merge(int val) {
            ans = Math.min(ans, val);
        }
    }

    public static class ResultMax extends Result {
        public ResultMax() {
            ans = Integer.MIN_VALUE;
        }

        public void merge(int val) {
            ans = Math.max(ans, val);
        }
    }

    public static class ResultSum extends Result {
        public void merge(int val) {
            ans += val;
        }
    }

    public static class ResultAvg extends Result {
        protected int cnt = 0;

        public void merge(int val) {
            ans += val;
            ++cnt;
        }

        @Override
        public int get() {
            return ans / cnt;
        };
    }

    public static class ResultCnt extends Result {
        public void merge(int val) {
            ++ans;
        }
    }

    // init value for each group
    // ordered by MIN, MAX, SUM, AVG, COUNT, SUM_COUNT, SC_AVG
    private Result[] blanks = new Result[] { new ResultMin(), new ResultMax(), new ResultSum(),
            new ResultAvg(), new ResultCnt(), new ResultCnt(), new ResultAvg() };

    private HashMap<Field, Result> groups = new HashMap<>();

    private int getVal(Tuple tup) {
        return Integer.parseInt(tup.getField(afield).toString());
    }

    private Field getKey(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            return null;
        }
        return tup.getField(gbfield);
    }

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // DONE: some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // DONE: some code goes here
        Result res = groups.get(getKey(tup));
        if (res == null) {
//            System.out.println("QwQ");
            res = (Result) blanks[what.ordinal()].clone();
        }
        res.merge(getVal(tup));
        groups.put(getKey(tup), res);
    }

    public TupleDesc getTupleDesc() {
        TupleDesc td = null;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        }
        return td;
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
        TupleDesc td = getTupleDesc();
        List<Tuple> res = new ArrayList<>(groups.size());
        for (Map.Entry<Field, Result> it : groups.entrySet()) {
            Tuple tup = new Tuple(td);
            if (gbfield != NO_GROUPING) {
                tup.setField(0, it.getKey());
                tup.setField(1, new IntField(it.getValue().get()));
            } else {
                tup.setField(0, new IntField(it.getValue().get()));
            }
            res.add(tup);
        }
        return new TupleIterator(td, res);
//        throw new UnsupportedOperationException("please implement me for lab2");
    }

//    public static void main(String[] args) {
//        IntegerAggregator ia = new IntegerAggregator(-1, Type.INT_TYPE, 1, Op.AVG);
//        TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
//        
//        Tuple tup1 = new Tuple(td);
//        tup1.setField(0, new IntField(1));
//        tup1.setField(1, new IntField(2));
//        ia.mergeTupleIntoGroup(tup1);
//        Tuple tup2 = new Tuple(td);
//        tup1.setField(0, new IntField(1));
//        tup1.setField(1, new IntField(4));
//        ia.mergeTupleIntoGroup(tup2);
//        
//        OpIterator it = ia.iterator();
//
//        try {
//            for (it.open(); it.hasNext();) {
//                Tuple tup = it.next();
////                System.out.println(tup.getField(0) + " " + tup.getField(1));
//                System.out.println(tup.getField(0));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
