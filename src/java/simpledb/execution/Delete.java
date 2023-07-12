package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private OpIterator child;
    private final TransactionId tid;
    private Tuple res;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // DONE: some code goes here
        this.child = child;
        this.tid = t;
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
                Database.getBufferPool().deleteTuple(tid, child.next());
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
