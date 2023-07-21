package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private class LRUStrategy {
        final AtomicLong nowcnt = new AtomicLong(0);

        public long getNow() {
            return nowcnt.getAndIncrement();
        }

        final ConcurrentHashMap<PageId, Long> lastCnt = new ConcurrentHashMap<>();

        public void visitPage(PageId pageId) {
            lastCnt.put(pageId, getNow());
        }

        public PageId getLruPageId() throws DbException {
            PageId minPage = null;
            Long minCnt = Long.MAX_VALUE;
            for (Map.Entry<PageId, Long> pair : lastCnt.entrySet()) {
                Page page = pages.get(pair.getKey());
                if (null != page.isDirty()) {
                    continue;
                }
                Long cnt = pair.getValue();
                if (cnt < minCnt) {
                    minCnt = cnt;
                    minPage = pair.getKey();
                }
            }
            if (minPage == null) {
                throw new DbException("all dirty, no way evict");
            }
            return minPage;
        }

        public PageId getAnyLruPageId() throws DbException {
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

    public synchronized void addPage(PageId pid, Page page) {
        pages.put(pid, page);
        lru.visitPage(pid);
    }

    public synchronized void addPage(Page page) {
        addPage(page.getId(), page);
    }

    private DbFile getFile(int tableid) {
        return Database.getCatalog().getDatabaseFile(tableid);
    }

    private DbFile getFile(PageId pid) {
        return getFile(pid.getTableId());
    }

    private static class Locks {
        // maybe concurrent hash set better
//        public List<TransactionId> shares = Collections.synchronizedList(new ArrayList<>());
        public Set<TransactionId> shares = ConcurrentHashMap.newKeySet();
        public volatile TransactionId exclude = null;

        public synchronized boolean hasLock(TransactionId tid) {
            return tid.equals(exclude) || shares.contains(tid);
        }

        public synchronized void removeLock(TransactionId tid) {
            if (tid.equals(exclude)) {
                exclude = null;
            }
            if (shares.contains(tid)) {
                shares.remove(tid);
            }
        }

        private synchronized boolean getNoExclude(TransactionId tid, Permissions perm) {
            return exclude == null || exclude.equals(tid);
        }

        private synchronized boolean getNoInclude(TransactionId tid, Permissions perm) {
            return shares.size() == 0 || (shares.size() == 1 && shares.contains(tid));
        }

        public synchronized boolean canAdd(TransactionId tid, Permissions perm) {
            boolean noExclude = getNoExclude(tid, perm);
            if (perm == Permissions.READ_ONLY) {
                return noExclude;
            }
            boolean noShare = getNoInclude(tid, perm);
            return noExclude && noShare;
        }

        public synchronized List<TransactionId> waitAdd(TransactionId tid, Permissions perm) {
            ArrayList<TransactionId> ans = new ArrayList<>();
            boolean noExclude = getNoExclude(tid, perm);
            if (!noExclude) {
                ans.add(exclude);
            }
            if (perm == Permissions.READ_WRITE) {
                for (TransactionId tid2 : shares) {
                    if (!tid.equals(tid2) && tid2 != null) {
                        ans.add(tid2);
                    }
                }
            }
            return ans;
        }

        public synchronized void addLock(TransactionId tid, Permissions perm) {
            if (perm == Permissions.READ_ONLY) {
                if (exclude != null && exclude.equals(tid)) {
                    exclude = null;
                }
                if (!shares.contains(tid)) {
                    shares.add(tid);
                }
            } else {
                if (shares.contains(tid)) {
                    shares.remove(tid);
                }
                exclude = tid;
            }
        }
    }

//    private final static long MAX_TRANSACTION_TIME = 5000;// ms

    private final ConcurrentHashMap<PageId, Locks> pageLocks = new ConcurrentHashMap<>();

    private static class WaitTransaction {
        public final TransactionId tid;
        public final PageId pid;
        public final Permissions perm;

        public WaitTransaction(TransactionId tid, PageId pid, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            this.perm = perm;
        }
    }

    private class DeadLockChecker {

        public final Set<WaitTransaction> waits = ConcurrentHashMap.newKeySet();

        // if not deadlock return null, else any circled transaction
        public synchronized TransactionId isDeadLock() {
            // build graph
//            int id = (int) (Math.random() * 100);
            Map<TransactionId, ArrayList<TransactionId>> g = new HashMap<>();
            Map<TransactionId, Integer> ru = new HashMap<>();
            for (WaitTransaction wait : waits) {
                g.put(wait.tid, new ArrayList<>());
                ru.put(wait.tid, 0);
            }
            // build wait sources graph
            for (WaitTransaction wait : waits) {
                Locks locks = pageLocks.get(wait.pid);
                if (locks == null) { // no occupy
                    continue;
                }
                if (!locks.canAdd(wait.tid, wait.perm)) {
                    for (TransactionId v : locks.waitAdd(wait.tid, wait.perm)) {
//                        System.out.println(wait.tid.getId() + " -> " + v.getId() + " from " + id);
                        // double check avoid concurrent problem
                        if (g.get(wait.tid) == null) {
                            g.put(wait.tid, new ArrayList<>());
                            ru.put(wait.tid, 0);
                        }
                        g.get(wait.tid).add(v);
                        ru.put(v, ru.getOrDefault(v, 0) + 1);
                        if (g.get(v) == null) {
                            g.put(v, new ArrayList<>());
                        }
                    }
                }
            }
//            System.out.println(" ----- ");
            // topo sort
            Queue<TransactionId> q = new LinkedList<>();
            for (Map.Entry<TransactionId, Integer> pr : ru.entrySet()) {
                if (pr.getValue().equals(0)) {
                    q.add(pr.getKey());
                }
            }
            while (!q.isEmpty()) {
                TransactionId u = q.poll();
                for (TransactionId v : g.get(u)) {
                    ru.put(v, ru.get(v) - 1);
                    if (ru.get(v).equals(0)) {
                        q.add(v);
                    }
                }
            }
            TransactionId dead = null;
            for (Map.Entry<TransactionId, Integer> pr : ru.entrySet()) {
                if (!pr.getValue().equals(0)) {
                    dead = pr.getKey();
                    break;
                }
            }
            return dead;
        }
    }

    private final DeadLockChecker deadLockChecker = new DeadLockChecker();
    // must long enough, or it cannot pass BTreeTest system test
    private final static long MAX_TRANSACTION_TIME = 30000;// ms
    private final static int WAIT_EPOCH = 100; // ms

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
//        perm = Permissions.READ_WRITE;

        // DONE: some code goes here
        // can't sync all the steps of locks
        Locks locks = pageLocks.get(pid);
        if (locks == null) {
            pageLocks.put(pid, new Locks());
            locks = pageLocks.get(pid);
        }
        WaitTransaction wait = new WaitTransaction(tid, pid, perm);
        long now = System.currentTimeMillis();
        while (!locks.canAdd(tid, perm)) {
            if (System.currentTimeMillis() - now > MAX_TRANSACTION_TIME) {
                System.out.println("Transaction too long " + tid.getId() + " " + pid);
                deadLockChecker.waits.remove(wait);
                throw new TransactionAbortedException();
            }
            synchronized (this) {
                deadLockChecker.waits.add(wait);
                TransactionId dead = deadLockChecker.isDeadLock();
                if (dead != null) {
                    System.out.println("Dead lock found, to remove " + tid.getId());
                    deadLockChecker.waits.remove(wait);
                    throw new TransactionAbortedException();
                }
                if (locks.canAdd(tid, perm)) {
                    break;
                }
            }

            try {
                Thread.sleep(WAIT_EPOCH);
                
//                TransactionId t = locks.exclude;
//                if (t == null && locks.shares.size() > 0) {
//                    t = locks.shares.iterator().next();
//                }
//                if (t != null) {
//                    if (null == pages.get(pid)) {
//                        System.out.println(
//                                tid.getId() + " waits for " + t.getId() + " null page in " + perm);
//                    } else {
//                        System.out.println(tid.getId() + " waits for " + t.getId() + " "
//                                + pages.get(pid).getId() + " in " + perm);
//                    }
//                } else {
//                    System.out.println(tid.getId() + " waits for ghost");
//                }
                
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        synchronized (this) {
            deadLockChecker.waits.remove(wait);
            locks.addLock(tid, perm);
//        }

        Page page = pages.get(pid);
        if (page == null) {
//            synchronized (this) {
                DbFile file = getFile(pid);
                page = file.readPage(pid);
                while (pages.size() >= numPages) {
                    evictPage();
                }
                addPage(pid, page);
            }
            return page;
        }
//        System.out.println("Get page " + tid.getId() + " " + perm + " " + page.getId());
       
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
        // DONE: some code goes here
        // not necessary for lab1|lab2
        Locks locks = pageLocks.get(pid);
        if (locks != null) {
            locks.removeLock(tid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // DONE: some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // DONE: some code goes here
        // not necessary for lab1|lab2
        Locks locks = pageLocks.get(p);
        synchronized (locks) {
            return locks != null && locks.hasLock(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // DONE: some code goes here
        // not necessary for lab1|lab2

        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for (Page page : pages.values()) {
                TransactionId dirtyTid = page.isDirty();
                if (dirtyTid != null && dirtyTid.equals(tid)) {
//                    System.out.println("Releaze lock of " + page.getId() + " by " + tid.getId());
                    deletePage(page.getId());
                    pageLocks.get(page.getId()).removeLock(dirtyTid);
                }
            }
        }
        for (Map.Entry<PageId, Locks> pr : pageLocks.entrySet()) {
            Locks locks = pr.getValue();
            if (locks != null && locks.hasLock(tid)) {
//                System.out.println("Remove lock of " + pr.getKey() + " by " + tid.getId());
                locks.removeLock(tid);
            }
        }
//        for (Locks locks : pageLocks.values()) {
//            if (locks != null && locks.hasLock(tid)) {
//                locks.removeLock(tid);
//            }
//        }
    }

    private synchronized void coverAll(TransactionId tid, List<Page> pages) {
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
        // DONE: some code goes here
        // not necessary for lab1
        while (!pages.isEmpty()) {
            try {
                evictPageEvenDirty();
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
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
        // DONE: some code goes here
        // not necessary for lab1
        try {
            flushPage(pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized void deletePage(PageId pid) {
        lru.removePage(pid);
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // DONE: some code goes here
        // not necessary for lab1
        Page page = pages.get(pid);
        if (page == null) {// origin not in, nothing to flush
            return;
        }
        TransactionId tid = page.isDirty();
        if (tid != null) {
            DbFile file = getFile(pid);
            try {
                file.writePage(page);
            } catch (IOException e) {
                e.printStackTrace();
            }
//            System.out.println("Release lock of " + pid + " by " + tid.getId());
            pageLocks.get(pid).removeLock(tid);
        }
        page.markDirty(false, tid);
        deletePage(pid);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // DONE: some code goes here
        // not necessary for lab1|lab2
        for (Page page : pages.values()) {
            TransactionId dirtyTid = page.isDirty();
            if (dirtyTid != null && dirtyTid.equals(tid)) {
                try {
                    flushPage(page.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // DONE: some code goes here
        // not necessary for lab1
        PageId deletedPageId = lru.getLruPageId();
        removePage(deletedPageId);
    }

    private synchronized void evictPageEvenDirty() throws DbException {
        PageId deletedPageId = lru.getAnyLruPageId();
        removePage(deletedPageId);
    }
}
