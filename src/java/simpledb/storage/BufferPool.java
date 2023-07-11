package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pages;

    public static class LRUStrategy {
        static final AtomicLong nowcnt = new AtomicLong(0);

        public long getNow() {
            return nowcnt.getAndIncrement();
        }

        final ConcurrentHashMap<PageId, Long> lastCnt = new ConcurrentHashMap<>();

        public void visitPage(PageId pageId) {
            lastCnt.put(pageId, getNow());
        }

        public PageId getLruPageId() {
            PageId minPage = null;
            Long minCnt = Long.MAX_VALUE;
            for (Map.Entry<PageId, Long> pair : lastCnt.entrySet()) {
                Long cnt = pair.getValue();
                if (cnt < minCnt) {
                    minCnt = cnt;
                    minPage = pair.getKey();
                }
            }
            assert minPage != null;
            return minPage;
        }

        public void removePage(PageId pageId) {
            lastCnt.remove(pageId);
        }
    }

    private final LRUStrategy lru;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // DONE: some code goes here
        this.numPages = numPages;
        pages = new ConcurrentHashMap<>();
        lru = new LRUStrategy();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public void addPage(PageId pid, Page page) {
        pages.put(pid, page);
        lru.visitPage(pid);
    }
    
    public void addPage( Page page) {
        addPage(page.getId(), page);
    }

    private DbFile getFile(int tableid) {
        return Database.getCatalog().getDatabaseFile(tableid);
    }

    private DbFile getFile(PageId pid) {
        return getFile(pid.getTableId());
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, a page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        
        // DONE: some code goes here
        Page page = pages.get(pid);
        if (page == null) {
            synchronized (this) {
//                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                DbFile file = getFile(pid);
                page = file.readPage(pid);
                while (pages.size() >= numPages) {
                    evictPage();

                }
                addPage(pid, page);
            }
        }
//        TransactionId dirtyTid = page.isDirty();
//        if (dirtyTid != null) {
//            pages.remove(pid);
//            getPage(dirtyTid, pid, perm);
//        }
        return page;
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }
    
    private void coverAll(TransactionId tid, List<Page> pages) {
        for (Page page : pages) {
            page.markDirty(true, tid);
            addPage(page);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will acquire
     * a write lock on the page the tuple is added to and any other pages that are
     * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
     * cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE: some code goes here
        // not necessary for lab1
        coverAll(tid, getFile(tableId).insertTuple(tid, t));
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from and any other pages that are updated. May
     * block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE: some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        coverAll(tid, getFile(tableId).deleteTuple(tid, t));
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
     * dirty data to disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1

    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages are removed from the
     * cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        lru.removePage(pid);
        Page page = pages.get(pid);
        TransactionId tid = page.isDirty();
        if (tid != null) {
            DbFile file = getFile(pid);
            try {
                file.writePage(page);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        PageId deletedPageId = lru.getLruPageId();
        removePage(deletedPageId);
    }

}
