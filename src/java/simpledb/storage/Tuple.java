package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Iterator;

import simpledb.common.Type;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    private TupleDesc tupleDesc;
    private ArrayList<Field> fields;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance
     *           with at least one field.
     */
    public Tuple(TupleDesc td) {
        // DONE: some code goes here
        tupleDesc = td;
        fields = new ArrayList<>(td.numFields());
        for (int i = 0, n = td.numFields(); i < n; ++i) {
            // 可以改更优雅，但暂时没必要
            if (td.getFieldType(i) == Type.INT_TYPE) {
                fields.add(new IntField(0));
            } else {
                fields.add(new StringField("", Type.STRING_LEN));
            }
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // DONE: some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be
     *         null.
     */
    public RecordId getRecordId() {
        // TODO: some code goes here
        return null;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // TODO: some code goes here
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // DONE: some code goes here
        fields.set(i, f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // DONE: some code goes here
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system
     * tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // DONE: some code goes here
//        throw new UnsupportedOperationException("Implement this");
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Field f : fields) {
            if (!first) {
                sb.append('\t');
            } else {
                first = false;
            }
            sb.append(f.toString());
        }
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // DONE: some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // DONE: some code goes here
        tupleDesc = td;// 都说only affecting the TupleDesc了就不动fields了
    }
}
