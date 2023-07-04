package simpledb.storage;

//import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {
    private final int tableId;
    private final int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a specific
     * table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // DONE: some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        // DONE: some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with this PageId
     */
    public int getPageNumber() {
        // DONE: some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of the table
     *         number and the page number (needed if a PageId is used as a key in a
     *         hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // DONE: some code goes here
        return tableId * 100007 + pgNo;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table ids are
     *         the same)
     */
    public boolean equals(Object o) {
        // DONE: some code goes here
//        return false;
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        HeapPageId other = (HeapPageId) o;
        return tableId == other.tableId && pgNo == other.pgNo;
    }

    /**
     * Return a representation of this object as an array of integers, for writing
     * to disk. Size of returned array must contain number of integers that
     * corresponds to number of args to one of the constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
