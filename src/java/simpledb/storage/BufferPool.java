package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
//import simpledb.storage.evict.EvictStrategy;
import simpledb.storage.evict.LRUStrategy;

import java.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private Integer numPages;
    private Map<PageId, LRUStrategy.LinkedNode> pageCache;
    private LRUStrategy evict;
    private LockManager lockManager;

    // 锁
    class PageLock{
        private static final int SHARE = 0;
        private static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;
        public PageLock(TransactionId tid, int type){
            this.tid = tid;
            this.type = type;
        }
        public TransactionId getTid(){
            return tid;
        }
        public int getType(){
            return type;
        }
        public void setType(int type){
            this.type = type;
        }
    }

    class LockManager {
        ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap = new ConcurrentHashMap<>();
        /**
         * 获取锁
         */
        public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredType) {
            // 判断当前页是否当前有锁
            if (lockMap.get(pageId) == null) {
                // 创建锁
                PageLock pageLock = new PageLock(tid, requiredType);
                ConcurrentHashMap<TransactionId, PageLock> pageLocks = new ConcurrentHashMap<>();
                pageLocks.put(tid, pageLock);
                lockMap.put(pageId, pageLocks);
                return true;
            }
            // 获取当前页的锁队列
            ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pageId);
            // 当前页面上，事务 tid 没有锁
            if (pageLocks.get(tid) == null) {
                // 如果 当前页已经有多个锁，这说明多个锁肯定都是share锁
                if (pageLocks.size() > 1) {
                    // 如果是共享锁，新增获取对象
                    if (requiredType == PageLock.SHARE) {
                        // tid 请求锁
                        PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                        pageLocks.put(tid, pageLock);
                        lockMap.put(pageId, pageLocks);
                        return true;
                    }
                    // 如果是排他锁
                    else if (requiredType == PageLock.EXCLUSIVE) {
                        // tid 需要获取写锁，拒绝
                        return false;
                    }
                } else if (pageLocks.size() == 1) {
                    // 如果 当前页仅有一个锁，那么可能是share锁或exclusive锁
                    PageLock curLock = (PageLock) pageLocks.values().toArray()[0];
//                    for (PageLock lock : pageLocks.values()) {
//                        curLock = lock;
//                    }
                    if (curLock.getType() == PageLock.SHARE) {
                        // 如果请求的锁也是读锁
                        if (requiredType == PageLock.SHARE) {
                            // tid 请求锁
                            PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                            pageLocks.put(tid, pageLock);
                            lockMap.put(pageId, pageLocks);
                            return true;
                        }
                        // 如果是独占锁
                        else if (requiredType == PageLock.EXCLUSIVE) {
                            // tid 需要获取写锁，拒绝
                            return false;
                        }
                    }
                    // 如果是独占锁
                    else if (curLock.getType() == PageLock.EXCLUSIVE) {
                        // tid 需要获取写锁，拒绝
                        return false;
                    }
                }
            } else {
                // 当前页面上，事务 tid 有锁
                PageLock pageLock = pageLocks.get(tid);
                // 事务上是读锁
                if (pageLock.getType() == PageLock.SHARE) {
                    // 新请求的是读锁
                    if (requiredType == PageLock.SHARE) {
                        return true;
                    }
                    // 新请求是写锁
                    else if (requiredType == PageLock.EXCLUSIVE) {
                        // 如果该页面上只有一个锁，就是该事务的读锁
                        if (pageLocks.size() == 1) {
                            // 进行锁升级(升级为写锁)
                            pageLock.setType(PageLock.EXCLUSIVE);
                            pageLocks.put(tid, pageLock);
                            return true;
                        } else {
                        // 大于一个锁，说明有其他事务有共享锁，不能进行锁的升级
                            return false;
                        }
                    }
                }
                // 事务上是写锁
                // 无论请求的是读锁还是写锁，都可以直接返回获取
                return pageLock.getType() == PageLock.EXCLUSIVE;
            }
            return false;
        }

        /**
         * 释放锁
         */
        public synchronized boolean releaseLock(TransactionId tid, PageId pageId) {
            // 判断是否持有锁
            if (isHoldLock(tid, pageId)) {
                ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
                locks.remove(tid);
                if (locks.size() == 0) {
                    lockMap.remove(pageId);
                }
                return true;
            }
            return false;
        }

        /**
         * 判断是否持有锁
         */
        public synchronized boolean isHoldLock(TransactionId tid, PageId pageId) {
            ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
            if (locks == null) {
                return false;
            }
            PageLock pageLock = locks.get(tid);
            if (pageLock == null) {
                return false;
            }
            return true;
        }

        /**
         * 完成事务后释放所有锁
         */
        public synchronized void completeTransaction(TransactionId tid) {
            // 遍历所有的页，如果对应事务持有锁就会释放
            for (PageId pageId : lockMap.keySet()) {
                releaseLock(tid, pageId);
            }
        }
    }



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
        this.evict = new LRUStrategy(numPages);
        this.lockManager = new LockManager();
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

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // Lab 1 version
//        return this.pageCache.get(pid);
        int lockType = perm == Permissions.READ_ONLY ? PageLock.SHARE: PageLock.EXCLUSIVE;
        boolean isAcquired = false;
        long startTime = System.currentTimeMillis();
        // 死锁检测：超时检测或者借助是否成环都可。简单起见，超时检测即可通过测试，
        // 超时时间可以通过加随机偏移量将不同事务的超时时间离散开。
        while (!isAcquired) {
            isAcquired = lockManager.acquiredLock(pid, tid, lockType);
            long now = System.currentTimeMillis();
            // 如果500 ms内没获取锁就抛出异常
            if (now - startTime > 500) {
                throw new TransactionAbortedException();
            }
        }

        if (!pageCache.containsKey(pid)) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            // 判断是否超过大小
            if (pageCache.size() >= numPages) {
                this.evictPage();
            }
            // 放入LRU 双向链表中
            LRUStrategy.LinkedNode node = new LRUStrategy.LinkedNode(pid, page);
            evict.addToHead(node);
            pageCache.put(pid, node);
        }
        // 移动到头部
        evict.moveToHead(pageCache.get(pid));
        return this.pageCache.get(pid).getPage();
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        // 如果成功提交
        if(commit){
            // 刷新页面
            try{
                flushPages(tid);
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        // 如果提交失败，回滚
        else{
            restorePages(tid);
        }
        // 事务完成
        lockManager.completeTransaction(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = (DbFile) Database.getCatalog().getDatabaseFile(tableId);
        if (dbFile == null) {
            throw new DbException("BufferPool.insertTuple()中 tableId : " + tableId + " 的表不存在");
        }
        for (Page page: dbFile.insertTuple(tid, t)) {
            // 如果缓存池已满，执行淘汰策略
            if(pageCache.size() > numPages){
                evictPage();
            }
            // 获取节点，此时的页一定已经在缓存了，因为刚刚被修改的时候就已经放入缓存了 --- 错，有可能新增页面
            // 如果缓存中有当前节点，则更新，如果没有当前节点，则新建放入缓存
            LRUStrategy.LinkedNode node;
            if (pageCache.containsKey(page.getId())) {
                node = pageCache.get(page.getId());
                // 更新新的页内容
                node.setPage(page);
            } else {
                if (pageCache.size() >= numPages) {
                    evictPage();
                }
                node = new LRUStrategy.LinkedNode(page.getId(), page);
                evict.addToHead(node);
            }
            // 更新到缓存
            pageCache.put(page.getId(), node);
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = (DbFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        if (dbFile == null) {
            throw new DbException("BufferPool.deleteTuple() tableId : " + t.getRecordId().getPageId().getTableId() + " 的表不存在");
        }
        for (Page page : dbFile.deleteTuple(tid, t)) {
            // 获取节点，此时的页一定已经在缓存了，因为刚刚被修改的时候就已经放入缓存了
            LRUStrategy.LinkedNode node = pageCache.get(page.getId());
            // 更新新的页内容
            node.setPage(page);
            pageCache.put(page.getId(), node);    // 修改后的页要覆盖缓冲池中的页
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, LRUStrategy.LinkedNode> entry : pageCache.entrySet()) {
            Page page = entry.getValue().getPage();
            if (page.isDirty() != null) {   // 判断是否是脏页
                flushPage(page.getId());    // 脏页刷新
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pageCache.containsKey(pid)) {
            pageCache.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid).getPage();
        // 通过tableId找到对应的DbFile,并将page写入到对应的DbFile中
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);

        // append an update record to the log, with a before-image and after-image
        TransactionId dirtyTid = page.isDirty();    // 脏的页，返回该页的事务id
        // 这里是lab6添加的
        if (dirtyTid != null) {
            // 脏页刷盘前把日志写入磁盘先    // 反正页刷盘时crash，先把日志刷了先
            Database.getLogFile().logWrite(dirtyTid, page.getBeforeImage(), page); // UPDATE记录的操作，保存before-image 和 after-image
            Database.getLogFile().force();  // 强制刷盘，不用放在缓冲中
        }
        // 在调用writePage(flush)之前，在BufferPool.flushPage()中插入以下几行，其中flush是对被写入页面的引用。
        // 这将导致日志系统向日志写入更新。
        // 我们强迫日志在页面被写入磁盘之前确保日志记录在磁盘上。
        // 将page刷新到磁盘
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, LRUStrategy.LinkedNode> entry : pageCache.entrySet()) {
            Page page = entry.getValue().getPage();
            // lab6
            // 注意：我们不能在flushPage()中直接调用setBeforeImage()，
            // 因为即使事务没有提交，flushPage()也可能被调用。
            // 你的BufferPool.transactionComplete()为每一个被提交的事务搅乱的页面调用flushPage()。
            // 对于每一个这样的页面，在你刷新页面之后，添加一个对p.setBeforeImage()的调用。
            // (这是事务提交以后把当前页当成beforeImage，可以拿来rollback，一定要提交后才能作为beforeImage)
            // flushPage不一定是事务提交的刷盘，flushPages才是！！！！！！！！！！！
            // 注意BufferPool满的时候也会flushPage 这是STEAL策略，而lab4中实现的是NO-STEAL策略
            page.setBeforeImage();  // 设置oldData    // 这个事务要提交了，把这个页的before-image设为提交时的镜像
            if (tid.equals(page.isDirty())) {   // 判断是否是脏页
                flushPage(page.getId());    // 脏页刷新
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
//        PageId evictPageId = evict.getEvictPageId();
//        Page page = pageCache.get(evictPageId).getPage();
//        if (page.isDirty() != null) {
//            try {
//                flushPage(evictPageId);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        discardPage(evictPageId);

        // 一个事务的修改只有在它提交之后才会被写入磁盘。
        // 这意味着我们可以通过丢弃脏页并从磁盘重读来中止一个事务。
        // 因此，我们必须不驱逐脏页。这个策略被称为NO STEAL。
        //
        // 你将需要修改BufferPool中的evictPage方法。
        // 特别是，它必须永远不驱逐一个脏页。如果你的驱逐策略倾向于驱逐一个脏页，你将不得不找到一种方法来驱逐一个替代页。
        // 在缓冲池中的所有页面都是脏的情况下，你应该抛出一个DbException。
        // 如果你的驱逐策略驱逐了一个干净的页面，要注意事务可能已经持有被驱逐的页面的任何锁，并在你的实现中适当地处理它们
        PageId evictPageId;
        Page page;
        boolean isAllDirty = true;
        for (int i=0; i < pageCache.size(); i++) {
            // 获取淘汰页的ID 从队尾一个个取出，直到取到非脏页
            evictPageId = evict.getEvictPageId();
            // 获取淘汰的页
            page = pageCache.get(evictPageId).getPage();
            // 判断是否脏页
            if (page.isDirty() != null) {
                // 脏页放回去，放到队头了
                // 如果加了日志之后，可以刷脏页了不？事务未提交不能，把页刷到磁盘，redo日志好像可以
                evict.moveToHead(pageCache.get(evictPageId));
            } else {
                isAllDirty = false;
                // 刷盘，并删除页面
                try {
                    flushPage(evictPageId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                discardPage(evictPageId);
                break;
            }
        }
        if (isAllDirty) {throw new DbException("All page are dirty page.");}
   }

   public synchronized void restorePages(TransactionId tid) {
       // 遍历缓存中的所有页面，看是否是当前事务修改的页面
       for(LRUStrategy.LinkedNode node : pageCache.values()){
           PageId pageId = node.getPageId();
           Page page = node.getPage();
           // 如果脏页的 事务id 相同
           if (tid.equals(page.isDirty())){
               int tableId = pageId.getTableId();
               // 获取现有的表
               DbFile table = Database.getCatalog().getDatabaseFile(tableId);
               // 读取当前的页面
               Page pageFromDisk = table.readPage(pageId);

               // 写回内存
               node.setPage(pageFromDisk);
               pageCache.put(pageId, node);
               evict.moveToHead(node);
           }
       }
   }

}
