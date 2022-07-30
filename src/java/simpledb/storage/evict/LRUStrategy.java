package simpledb.storage.evict;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description:    LRU页面丢弃策略
 * 根据历史访问记录来淘汰数据，核心思想为；如果数据最近被访问过，那么将来被访问的几率页更高
 * @author: WYG
 * @time: 2021/11/12 15:20
 */

public class LRUStrategy {

    public static class LinkedNode{
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;

        public LinkedNode() {}

        public LinkedNode(PageId pageId, Page page){
            this.pageId = pageId;
            this.page = page;
        }

        public void setPageId(PageId pageId) {
            this.pageId = pageId;
        }

        public void setPage(Page page) {
            this.page = page;
        }

        public Page getPage() {
            return page;
        }

        public PageId getPageId() {
            return pageId;
        }
    }

//    private Map<PageId, LinkedNode> map;
    // 头节点
    LinkedNode head;
    // 尾节点
    LinkedNode tail;

    public LRUStrategy(int numPages) {
        head = new LinkedNode();
        tail = new LinkedNode();
        head.next = tail;
        tail.prev = head;
//        map = new ConcurrentHashMap<>(numPages);
    }

    public void addToHead(LinkedNode node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    public void remove(LinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    public void moveToHead(LinkedNode node){
        remove(node);
        addToHead(node);
    }

    public LinkedNode removeTail(){
        LinkedNode node = tail.prev;
        remove(node);
//        map.remove(node.pageId);
        return node;
    }

    public PageId getEvictPageId() {
        return removeTail().getPageId();
    }

// ---------------------------------------------------------------------------------------------
//    public LRUStrategy(int numPages) {
//        head = new DLinkedNode();
//        tail = new DLinkedNode();
//        head.next = tail;
//        tail.prev = head;
//        map = new ConcurrentHashMap<>(numPages);
//    }
//
//    @Override
//    public void modifyData(PageId pageId) {
//        if (map.containsKey(pageId)) {
//            DLinkedNode node = map.get(pageId);
//            moveToHead(node);
//        } else {
//            DLinkedNode node = new DLinkedNode(pageId);
//            map.put(pageId, node);
//            addToHead(node);
//        }
//    }
//
//    @Override
//    public PageId getEvictPageId() {
//        return removeTail().getValue();
//    }
//
//    private void addToHead(DLinkedNode node) {
//        node.prev = head;
//        node.next = head.next;
//        head.next.prev = node;
//        head.next = node;
//    }
//
//    private void removeNode(DLinkedNode node) {
//        node.prev.next = node.next;
//        node.next.prev = node.prev;
//        map.remove(node.value);
//    }
//
//    private void moveToHead(DLinkedNode node) {
//        removeNode(node);
//        addToHead(node);
//    }
//
//    private DLinkedNode removeTail() {
//        DLinkedNode res = tail.prev;
//        removeNode(res);
//        return res;
//    }
}
