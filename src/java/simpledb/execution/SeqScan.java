package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
//    private int tableid;
    private String tableAlias;
    private TupleDesc td;// added prefix(tableAlias)
    private DbFileIterator it;

    private void init(int tableid, String tableAlias) {
//        this.tableid = tableid;
        this.tableAlias = tableAlias;
        DbFile f = Database.getCatalog().getDatabaseFile(tableid);
        td = Database.getCatalog().getTupleDesc(tableid);
        int n = td.numFields();
        Type[] types = new Type[n];
        String[] fields = new String[n];
        for (int i = 0; i < n; ++i) {
            types[i] = td.getFieldType(i);
            String rawField = td.getFieldName(i);
            if (rawField != null) {
                rawField = tableAlias + "." + rawField;
            }
            fields[i] = rawField;
        }
        td = new TupleDesc(types, fields);
        it = f.iterator(tid);
    }

    /**
     * Creates a sequential scan over the specified table as a part of the specified
     * transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the
     *                   returned tupleDesc should have fields with name
     *                   tableAlias.fieldName (note: this class is not responsible
     *                   for handling a case where tableAlias or fieldName are null.
     *                   It shouldn't crash if they are, but the resulting name can
     *                   be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // DONE: some code goes here
        this.tid = tid;
        init(tableid, tableAlias);
    }

    /**
     * @return return the table name of the table the operator scans. This should be
     *         the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // DONE: some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the
     *                   returned tupleDesc should have fields with name
     *                   tableAlias.fieldName (note: this class is not responsible
     *                   for handling a case where tableAlias or fieldName are null.
     *                   It shouldn't crash if they are, but the resulting name can
     *                   be null.fieldName, tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // DONE: some code goes here
        init(tableid, tableAlias);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // DONE: some code goes here
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile, prefixed
     * with the tableAlias string from the constructor. This prefix becomes useful
     * when joining tables containing a field(s) with the same name. The alias and
     * name should be separated with a "." character (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile, prefixed
     *         with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // DONE: some code goes here
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // DONE: some code goes here
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        // DONE: some code goes here
        return it.next();
    }

    public void close() {
        // DONE: some code goes here
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        // DONE: some code goes here
        it.rewind();
    }
}
