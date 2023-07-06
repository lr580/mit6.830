package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
//import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
//import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // DONE: some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // DONE: some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you
     * will need to generate this tableid somewhere to ensure that each HeapFile has
     * a "unique id," and that you always return the same value for a particular
     * HeapFile. We suggest hashing the absolute file name of the file underlying
     * the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // DONE: some code goes here
        return f.getAbsoluteFile().hashCode();// O(1)µÄgetAbsoluteFile
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // DONE: some code goes here
        return td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // DONE: some code goes here
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile file = new RandomAccessFile(f, "r");
            file.seek(offset);
            for (int i = 0, n = BufferPool.getPageSize(); i < n; ++i) {
                data[i] = file.readByte();
            }
            file.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
//            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // DONE: some code goes here
        // ceil(filesize/pagesize)
        return ((int) f.length() - 1) / BufferPool.getPageSize() + 1;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // DONE: some code goes here
        return new DbFileIterator() {
            private final TransactionId tId = tid;
            private int nowPageNo;// first unread page
            private boolean opened = false;
            private Iterator<Tuple> it;

            private void readPage() throws TransactionAbortedException, DbException {
                if (nowPageNo >= numPages()) {
                    it = null;
                    return;
                }
//                Page page = readPage() : no, it will skip buffer
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tId,
                        new HeapPageId(getId(), nowPageNo), Permissions.READ_ONLY);
                nowPageNo += 1;
                it = page.iterator();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // if not open throw?
                nowPageNo = 0;
                it = null;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                rewind();
            }

            @Override
            public Tuple next()
                    throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return it.next();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!opened) {
                    return false;
                }
                if (it == null || !it.hasNext()) {
                    readPage();
                    if (it == null || !it.hasNext()) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void close() {
                opened = false;
            }
        };
    }

}
