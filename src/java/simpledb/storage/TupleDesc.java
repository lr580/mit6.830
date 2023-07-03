package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        // ADDED
        @Override
        public int hashCode() {
            if (fieldName == null) {
                return 0;
            }
            return fieldName.hashCode() * (fieldType == Type.INT_TYPE ? 7 : 11);
        }
    }

    private ArrayList<TDItem> items;
    private int size; // 预处理
    private int hashCode; // 预处理
    private HashMap<String, Integer> name2index; // 预处理

    private void init(Type[] typeAr, String[] fieldAr) {
        int n = typeAr.length;
        name2index = new HashMap<>();
        size = 0;
        items = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            items.add(new TDItem(typeAr[i], fieldAr[i]));
//            items.set(i, new TDItem(typeAr[i], fieldAr[i]));
            size += typeAr[i].getLen();
            name2index.put(fieldAr[i], i);
        }
        hashCode = items.hashCode();
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are
     *         included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // DONE: some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified
     * types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // DONE: some code goes here
        init(typeAr, fieldAr);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of
     * the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // DONE: some code goes here
        this(typeAr, new String[typeAr.length]);
        assert (typeAr.length == 0 || items.get(0).fieldName == null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // DONE: some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // DONE: some code goes here
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // DONE: some code goes here
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        // DONE: some code goes here
        if (name == null || !name2index.containsKey(name)) {
            throw new NoSuchElementException();
        }
        return name2index.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
     *         that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // DONE: some code goes here
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // DONE: some code goes here
        int n1 = td1.numFields(), n2 = td2.numFields(), n = n1 + n2;
        Type[] typeAr = new Type[n];
        String[] fieldAr = new String[n];
        for (int i = 0; i < n1; ++i) {
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }
        for (int i = 0, j = n1; i < n2; ++i, ++j) {
            typeAr[j] = td2.getFieldType(i);
            fieldAr[j] = td2.getFieldName(i);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items and if
     * the i-th type in this TupleDesc is equal to the i-th type in o for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // DONE: some code goes here
        if (o == null || !(o instanceof TupleDesc)) {
            return false;
        }
        return hashCode == ((TupleDesc) o).hashCode;
    }

    public int hashCode() {
        return hashCode;
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
     * exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // DONE: some code goes here
        StringBuilder sb = new StringBuilder();
        if (items.size() != 0) {
            sb.append(items.get(0).toString());
        }
        for (int n = items.size(), i = 1; i < n; ++i) {
            sb.append(", ");
            sb.append(items.get(i).toString());
        }
        return sb.toString();
    }
}
