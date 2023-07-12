package simpledb.execution;

import java.io.IOException;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final TransactionId tid;
    private final int tableId;
    private Tuple res;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // DONE: some code goes here
        this.child = child;
        this.tid = t;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // DONE: some code goes here
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // DONE: some code goes here
        child.open();
        
        int cnt = 0;
        while (child.hasNext()) {
            ++cnt;
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
            } catch (NoSuchElementException | DbException | IOException
                    | TransactionAbortedException e) {
                e.printStackTrace();
            }
        }
        res = new Tuple(getTupleDesc());
        res.setField(0, new IntField(cnt));
        
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
     * Inserts tuples read from child into the tableId specified by the constructor.
     * It returns a one field tuple containing the number of inserted records.
     * Inserts should be passed through BufferPool. An instances of BufferPool is
     * available via Database.getBufferPool(). Note that insert DOES NOT need check
     * to see if a particular tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if
     *         called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // DONE: some code goes here
        Tuple r = res;
        res = null;
        return r;
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
