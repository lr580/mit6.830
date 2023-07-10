package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
//import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child, result;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private Aggregator ag;

    private void init() {
        Type gtype = null;
        if (gfield != Aggregator.NO_GROUPING) {
            gtype = child.getTupleDesc().getFieldType(gfield);
        }
        if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            ag = new StringAggregator(gfield, gtype, afield, aop);
        } else {
            ag = new IntegerAggregator(gfield, gtype, afield, aop);
        }
    }

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
        // DONE: some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        init();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // DONE: some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of
     *         the groupby field in the <b>OUTPUT</b> tuples. If not, return null;
     */
    public String groupFieldName() {
        // DONE: some code goes here
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // DONE: some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        // DONE: some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // DONE: some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        // DONE: some code goes here
        child.open();
        while (child.hasNext()) {
            ag.mergeTupleIntoGroup(child.next());
        }
        result = ag.iterator();
        result.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is
     * the field by which we are grouping, and the second field is the result of
     * computing the aggregate. If there is no group by field, then the result tuple
     * should contain one field representing the result of the aggregate. Should
     * return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // DONE: some code goes here
        if (result.hasNext()) {
            return result.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // DONE: some code goes here
//        child.rewind();
        result.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this
     * will have one field - the aggregate column. If there is a group by field, the
     * first field will be the group by field, and the second will be the aggregate
     * value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
     * in the constructor, and child_td is the TupleDesc of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        // DONE: some code goes here
        return ag.getTupleDesc(); // added interface function
    }

    public void close() {
        // DONE: some code goes here
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // DONE: some code goes here
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // DONE: some code goes here
        child = children[0];
    }

}
