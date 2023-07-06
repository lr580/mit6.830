package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read tuples
     * to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // DONE: some code goes here
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // DONE: some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // DONE: some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        // DONE: some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // DONE: some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // DONE: some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that pass
     * the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more
     *         tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
            throws NoSuchElementException, TransactionAbortedException, DbException {
        // DONE: some code goes here
        for (;;) {
            if (!child.hasNext()) {
                return null;
            }
            Tuple t = child.next();
            if (p.filter(t)) {
                return t;
            }
        }
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
