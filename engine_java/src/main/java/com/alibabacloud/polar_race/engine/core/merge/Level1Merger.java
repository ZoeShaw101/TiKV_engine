package com.alibabacloud.polar_race.engine.core.merge;

import com.alibabacloud.polar_race.engine.core.LSMDB;
import com.alibabacloud.polar_race.engine.core.LevelQueue;
import com.alibabacloud.polar_race.engine.core.stats.DBStats;
import com.alibabacloud.polar_race.engine.core.table.AbstractMapTable;
import com.alibabacloud.polar_race.engine.core.table.AbstractSortedMapTable;
import com.alibabacloud.polar_race.engine.core.table.FCMapTable;
import com.alibabacloud.polar_race.engine.core.table.IMapEntry;
import com.alibabacloud.polar_race.engine.core.utils.DateFormatter;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Level1Merger extends Thread {

    static final Logger log = LoggerFactory.getLogger(Level1Merger.class);

    private static final int MAX_SLEEP_TIME = 8 * 1000; // 5 seconds
    private static final int DEFAULT_MERGE_WAYS = 8; // 4 way merge
    private static final int CACHED_MAP_ENTRIES = 32;

    private List<LevelQueue> levelQueueList;
    private LSMDB sdb;
    private final DBStats stats;

    private volatile boolean stop = false;
    private CountDownLatch countDownLatch;
    private short shard;

    public Level1Merger(LSMDB sdb, List<LevelQueue> levelQueueList, CountDownLatch countDownLatch, short shard,
                        DBStats stats) {
        this.sdb = sdb;
        this.levelQueueList = levelQueueList;
        this.countDownLatch = countDownLatch;
        this.shard = shard;
        this.stats = stats;
    }

    @Override
    public void run() {
        while(!stop) {
            try {
                boolean merged = false;

                LevelQueue lq1 = levelQueueList.get(LSMDB.LEVEL1);
                LevelQueue lq2 = levelQueueList.get(LSMDB.LEVEL2);
                boolean hasLevel2MapTable = lq2.size() > 0;
                if ((!hasLevel2MapTable && lq1.size() >= DEFAULT_MERGE_WAYS) ||
                        (hasLevel2MapTable && lq1.size() >= DEFAULT_MERGE_WAYS - 1)) {
                    log.info("当前执行 running level 1 merging at " + DateFormatter.formatCurrentDate());
                    log.info("当前 queue size at level 1 is " + lq1.size() + ", current free memory size: " + Runtime.getRuntime().freeMemory());
                    log.info("当前 queue size at level 2 is " + lq2.size() + ", current free memory size: " + Runtime.getRuntime().freeMemory());

                    long start = System.nanoTime();
                    mergeSort(lq1, lq2, DEFAULT_MERGE_WAYS, sdb.getDir(), shard);
                    stats.recordMerging(LSMDB.LEVEL1, System.nanoTime() - start);

                    merged = true;
                    log.info("End running level 1 to 2 merging at " + DateFormatter.formatCurrentDate());
                }

                if (!merged) {
                    Thread.sleep(MAX_SLEEP_TIME);
                }

            } catch (Exception ex) {
                log.error("Error occured in the level 1 to 2 merger", ex);
            }

        }

        this.countDownLatch.countDown();
        log.info("Stopped level 1 to 2 merge thread " + this.getName());
    }

    public static void mergeSort(LevelQueue lq1, LevelQueue lq2, int ways, String dir, short shard) throws IOException, ClassNotFoundException {
        boolean hasLevel2MapTable = lq2.size() > 0;
        List<AbstractMapTable> tables = new ArrayList<AbstractMapTable>(ways);
        lq1.getReadLock().lock();
        try {
            Iterator<AbstractMapTable> iter = lq1.descendingIterator();
            for(int i = 0; i < ways - 1; i++) {
                tables.add(iter.next());
            }
            if (hasLevel2MapTable) {
                tables.add(lq2.get(0));
            } else {
                tables.add(iter.next());
            }
        } finally {
            lq1.getReadLock().unlock();
        }

        long expectedInsertions = 0;
        for(AbstractMapTable table : tables) {
            expectedInsertions += table.getAppendedSize();
        }
        if (expectedInsertions > Integer.MAX_VALUE) expectedInsertions = Integer.MAX_VALUE;
        // target table
        AbstractSortedMapTable sortedMapTable = new FCMapTable(dir, shard, LSMDB.LEVEL2, System.nanoTime(), (int)expectedInsertions);

        PriorityQueue<QueueElement> pq = new PriorityQueue<QueueElement>();

        // build initial heap
        QueueElement qe;
        IMapEntry me;

        for(AbstractMapTable table : tables) {
            qe = new QueueElement();
            qe.sortedMapTable = table;
            qe.size = qe.sortedMapTable.getAppendedSize();
            qe.index = 0;
            qe.queue = new LinkedList<IMapEntry>();
            me = qe.getNextMapEntry();
            if (me != null) {
                qe.key = me.getKey();
                qe.mapEntry = me;
                qe.keyHash = me.getKeyHash();
                pq.add(qe);
            }
        }

        Queue<IMapEntry> targetCacheQueue = new LinkedList<IMapEntry>();   //批量写入以提速
        // merge sort
        QueueElement qe1, qe2;
        IMapEntry mapEntry;
        log.info("开始执行level1-2归并排序...");
        while(pq.size() > 0) {
            qe1 = pq.poll();
            // remove old/stale entries
            while(pq.peek() != null && qe1.keyHash == pq.peek().keyHash && BytesUtil.KeyComparator(qe1.key, pq.peek().key) == 0) {
                qe2 = pq.poll();
                me = qe2.getNextMapEntry();
                if (me != null) {
                    qe2.key = me.getKey();
                    qe2.mapEntry = me;
                    qe2.keyHash = me.getKeyHash();
                    pq.add(qe2);
                }
            }
            targetCacheQueue.add(qe1.mapEntry);
            if (targetCacheQueue.size() >= CACHED_MAP_ENTRIES * DEFAULT_MERGE_WAYS) {
                while(targetCacheQueue.size() > 0) {
                    mapEntry = targetCacheQueue.poll();
                    byte[] value = mapEntry.getValue();
                    sortedMapTable.appendNew(mapEntry.getKey(), mapEntry.getKeyHash(), value);
                }
            }
            me = qe1.getNextMapEntry();
            if (me != null) {
                qe1.key = me.getKey();
                qe1.mapEntry = me;
                qe1.keyHash = me.getKeyHash();
                pq.add(qe1);
            }
        }

        log.info("开始执行level1-2归并排序完成!");

        // remaining cached entries
        while(targetCacheQueue.size() > 0) {
            mapEntry = targetCacheQueue.poll();
            byte[] value = mapEntry.getValue();
            sortedMapTable.appendNew(mapEntry.getKey(), mapEntry.getKeyHash(), value);
        }

        // persist metadata
        sortedMapTable.reMap();
        sortedMapTable.saveMetadata();

        // switching
        lq1.getWriteLock().lock();
        lq2.getWriteLock().lock();
        try {
            for(int i = 0; i < ways - 1; i++) {
                lq1.removeLast();
            }
            if (hasLevel2MapTable) {
                lq2.removeLast();
            } else {
                lq1.removeLast();
            }
            for(AbstractMapTable table : tables) {
                table.markUsable(false);
            }

            sortedMapTable.markUsable(true);
            lq2.addFirst(sortedMapTable);
        } finally {
            lq2.getWriteLock().unlock();
            lq1.getWriteLock().unlock();
        }

        for(AbstractMapTable table : tables) {
            table.close();
            table.delete();
        }
    }

    public void setStop() {
        this.stop = true;
        log.info("Stopping level 1 to 2 merge thread " + this.getName());
    }

    static class QueueElement implements Comparable<QueueElement> {

        AbstractMapTable sortedMapTable;
        long size;
        int index;
        byte[] key;
        int keyHash;
        IMapEntry mapEntry;
        LinkedList<IMapEntry> queue;

        // cache optimization
        public IMapEntry getNextMapEntry() throws IOException {
            IMapEntry me = queue.poll();
            if (me != null) return me;
            if (me == null) {
                int count = 0;
                while(index < size && count < CACHED_MAP_ENTRIES) {
                    IMapEntry mapEntry = sortedMapTable.getMapEntry(index);
                    // eager loading
                    mapEntry.getKey();
                    mapEntry.getValue();
                    queue.add(mapEntry);
                    index++;
                    count++;
                }
            }
            return queue.poll();
        }

        @Override
        public int compareTo(QueueElement other) {
            if (keyHash < other.keyHash) return -1;
            else if (keyHash > other.keyHash) return 1;
            else {
                if (BytesUtil.KeyComparator(key, other.key) < 0) {
                    return -1;
                } else if (BytesUtil.KeyComparator(key, other.key) > 0) {
                    return 1;
                } else {
                    if (this.sortedMapTable.getLevel() == LSMDB.LEVEL1 && other.sortedMapTable.getLevel() == LSMDB.LEVEL1) {
                        if (sortedMapTable.getCreatedTime() > other.sortedMapTable.getCreatedTime()) {
                            return -1;
                        } else if (sortedMapTable.getCreatedTime() < other.sortedMapTable.getCreatedTime()) {
                            return 1;
                        } else {
                            return 0;
                        }
                    } else {
                        if (this.sortedMapTable.getLevel() == LSMDB.LEVEL1) return -1;
                        else return 1;
                    }
                }
            }
        }
    }
}

