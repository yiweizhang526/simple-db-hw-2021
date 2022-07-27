package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // 计算page对应的偏移量
        int pageSize = BufferPool.getPageSize();
        int pageNumber = pid.getPageNumber();
        int offset = pageSize * pageNumber;
        // 初始化空Page
        Page page = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            page = new HeapPage(((HeapPageId) pid), data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
//                assert randomAccessFile != null;
                randomAccessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long len = this.file.length();
//        Database.getBufferPool();
        return (int) Math.ceil(len * 1.0/ BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    public static final class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;
        private Iterator<Tuple> iterator; // 可以理解在现有java的iterator迭代器上增加功能
        private int pageNumber;
        private final TransactionId tid;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // 开启HeapFile的迭代器，迭代表中元组的迭代器
            // 从0号页开始
            this.pageNumber = 0;
            // 获取0号页的迭代器，注意，这里并不是第0页，第1页，... 这样的迭代器，而是不同页各自的迭代器
            this.iterator = getIterator(pageNumber);
        }

        // 这里想不到更好的iterator实现方法了，只能这样做一个helper函数有利于hasNext()递归调用
        private Iterator<Tuple> getIterator(int pageNo) throws DbException, TransactionAbortedException {
            if (pageNo < 0 || pageNo >= heapFile.numPages()) {
                throw new DbException(String.format("heapfile %d does not contain page %d", heapFile.getId(), pageNo));
            }
            // 注意：一个file对应一个table，所以前面的tableid与这里的id一直，由getId()方法得到
            HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pageNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        // 这里有一个问题，这里实现的是HeapFileIterator，即文件的相连续的tuples可能不在同一个page上，即页面跳转
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return false;
            }
            // 这里使用while，是因为可能跳转一个页面后，下一个页面是空的，所以就要继续跳转
            while (iterator != null && !iterator.hasNext()) {
                if (pageNumber < heapFile.numPages() - 1) {
                    pageNumber ++;
                    iterator = getIterator(pageNumber);
                } else {
                    iterator = null;
                }
            }
            if (iterator == null) {
                return false;
            }
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // 如果迭代器为null 或 迭代器没有下一个指向，抛出异常
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            // 返回下一个指向
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

