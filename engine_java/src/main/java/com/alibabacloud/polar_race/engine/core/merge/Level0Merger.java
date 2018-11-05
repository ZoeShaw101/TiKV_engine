package com.alibabacloud.polar_race.engine.core.merge;

import com.alibabacloud.polar_race.engine.core.LSMDB;
import com.alibabacloud.polar_race.engine.core.LevelQueue;
import com.alibabacloud.polar_race.engine.core.stats.DBStats;
import com.alibabacloud.polar_race.engine.core.table.*;
import com.alibabacloud.polar_race.engine.core.utils.DateFormatter;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * LSM的写放大缺点
 */

public class Level0Merger extends Thread {

    static final Logger log = LoggerFactory.getLogger(Level0Merger.class);

    private static final int MAX_SLEEP_TIME = 4 * 1000; // 2 seconds

    public static final int DEFAULT_MERGE_WAYS = 4; // 当level 0 的 memtable 达到k个时, 进行K路归并算法

    private List<LevelQueue> levelQueueList;
    private LSMDB sdb;
    private final DBStats stats;

    private volatile boolean stop = false;
    private CountDownLatch countDownLatch;
    private short shard;

    public Level0Merger(LSMDB sdb, List<LevelQueue> levelQueueList, CountDownLatch countDownLatch, short shard,
                        DBStats stats) {
        this.sdb = sdb;
        this.levelQueueList = levelQueueList;
        this.countDownLatch = countDownLatch;
        this.shard = shard;
        this.stats = stats;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                LevelQueue levelQueue0 = levelQueueList.get(LSMDB.LEVEL0);
                if (levelQueue0 != null && levelQueue0.size() >= DEFAULT_MERGE_WAYS) {
                    log.info("开始执行 level 0 merge thread at " + DateFormatter.formatCurrentDate());
                    log.info("当前 queue size at level 0 is " + levelQueue0.size() + ", current free memory size: " + Runtime.getRuntime().freeMemory());

                    /*List<MemoryMXBean> pools = ManagementFactory.getPlatformMXBeans(MemoryMXBean.class);  //查看堆外内存
                    for (MemoryMXBean bean : pools) {
                        log.info("heap init: " + bean.getHeapMemoryUsage().getInit() + ", heap used:" + bean.getHeapMemoryUsage().getUsed());
                        log.info("non heap init: " + bean.getNonHeapMemoryUsage().getInit() + ", non heap used:" + bean.getNonHeapMemoryUsage().getUsed());
                    }*/

                    long start = System.nanoTime();
                    LevelQueue levelQueue1 = levelQueueList.get(LSMDB.LEVEL1);

                    mergeSort(levelQueue0, levelQueue1, DEFAULT_MERGE_WAYS, sdb.getDir(), shard);

                    stats.recordMerging(LSMDB.LEVEL0, System.nanoTime() - start);

                    log.info("Stopped running level 0 merge thread at " + DateFormatter.formatCurrentDate());

                } else {
                    Thread.sleep(MAX_SLEEP_TIME);
                }

            } catch (Exception ex) {
                log.error("Error occured in the level0 merge dumper", ex);
            }

        }

        this.countDownLatch.countDown();
        log.info("Stopped level 0 merge thread " + this.getName());
    }

    public static void mergeSort(LevelQueue source, LevelQueue target, int ways, String dir, short shard) throws IOException, ClassNotFoundException {
        List<HashMapTable> tables = new ArrayList<HashMapTable>(ways);
        source.getReadLock().lock();
        try {
            Iterator<AbstractMapTable> iter = source.descendingIterator();
            for (int i = 0; i < ways; i++) {
                tables.add((HashMapTable) iter.next());
            }
        } finally {
            source.getReadLock().unlock();
        }

        int expectedInsertions = 0;
        for (HashMapTable table : tables) {
            expectedInsertions += table.getRealSize();
        }
        log.info("分配merge目标table...");
        // target table
        MMFMapTable sortedMapTable = new MMFMapTable(dir, shard, LSMDB.LEVEL1, System.nanoTime(), expectedInsertions, ways);

        PriorityQueue<QueueElement> pq = new PriorityQueue<QueueElement>();

        log.info("创建堆...");
        QueueElement qe;
        List<Map.Entry<ByteArrayWrapper, InMemIndex>> list;
        Map.Entry<ByteArrayWrapper, InMemIndex> me;

        for (HashMapTable table : tables) {
            qe = new QueueElement();
            final HashMapTable hmTable = table;
            qe.hashMapTable = hmTable;
            list = new ArrayList<Map.Entry<ByteArrayWrapper, InMemIndex>>(qe.hashMapTable.getEntrySet());
            Collections.sort(list, new Comparator<Map.Entry<ByteArrayWrapper, InMemIndex>>() {

                @Override
                public int compare(
                        Map.Entry<ByteArrayWrapper, InMemIndex> o1,
                        Map.Entry<ByteArrayWrapper, InMemIndex> o2) {
                    IMapEntry mapEntry1 = hmTable.getMapEntry(o1.getValue().getIndex());
                    IMapEntry mapEntry2 = hmTable.getMapEntry(o2.getValue().getIndex());
                    try {
                        int hash1 = mapEntry1.getKeyHash();
                        int hash2 = mapEntry2.getKeyHash();
                        if (hash1 < hash2) return -1;
                        else if (hash1 > hash2) return 1;
                        else {
                            return o1.getKey().compareTo(o2.getKey());
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Fail to get hash code in map entry", e);
                    }

                }

            });
            qe.iterator = list.iterator();
            if (qe.iterator.hasNext()) {
                me = qe.iterator.next();
                qe.key = me.getKey().getData();
                qe.inMemIndex = me.getValue();
                IMapEntry mapEntry = table.getMapEntry(qe.inMemIndex.getIndex());
                qe.keyHash = mapEntry.getKeyHash();
                pq.add(qe);
            }
        }
        list = null;
        qe = null;

        //System.gc();

        log.info("执行归并排序...");  //todo:归并排序的时候会系统oom
        QueueElement qe1, qe2;
        IMapEntry mapEntry;
        boolean flag = true;
        //byte[] value;
        while (pq.size() > 0) {
            //log.info("priority queue size=" + pq.size());
            qe1 = pq.poll();
            // remove old/stale entries
            while (pq.peek() != null && qe1.keyHash == pq.peek().keyHash && BytesUtil.KeyComparator(qe1.key, pq.peek().key) == 0) {
                qe2 = pq.poll();
                if (qe2.iterator.hasNext()) {
                    me = qe2.iterator.next();
                    qe2.key = me.getKey().getData();
                    qe2.inMemIndex = me.getValue();
                    mapEntry = qe2.hashMapTable.getMapEntry(qe2.inMemIndex.getIndex());
                    qe2.keyHash = mapEntry.getKeyHash();
                    pq.add(qe2);
                }
            }

            mapEntry = qe1.hashMapTable.getMapEntry(qe1.inMemIndex.getIndex());
            byte[] value = mapEntry.getValueAddress();
            sortedMapTable.appendNew(mapEntry.getKey(), mapEntry.getKeyHash(), value);
            if (flag) {
                log.info("首次进行归并排序，写入数据到table");
                flag = false;
            }
            if (qe1.iterator.hasNext()) {
                me = qe1.iterator.next();
                qe1.key = me.getKey().getData();
                qe1.inMemIndex = me.getValue();
                IMapEntry mEntry = qe1.hashMapTable.getMapEntry(qe1.inMemIndex.getIndex());
                qe1.keyHash = mEntry.getKeyHash();
                pq.add(qe1);
            }
        }
        qe1 = null;
        qe2 = null;

        //System.gc();

        log.info("归并排序执行完毕，保存meta信息...");
        // persist metadata
        sortedMapTable.reMap();
        sortedMapTable.saveMetadata();

        log.info("保存进level 1...");   //todo:这里可能产生死锁？？
        source.getWriteLock().lock();
        target.getWriteLock().lock();
        try {
            for (int i = 0; i < ways; i++) {
                source.removeLast();
            }
            for (HashMapTable table : tables) {  //source的table都标记为不可用
                table.markUsable(false);
            }

            sortedMapTable.markUsable(true);
            target.addFirst(sortedMapTable);

        } finally {
            target.getWriteLock().unlock();
            source.getWriteLock().unlock();
        }

        for (HashMapTable table : tables) {
            table.close();
            table.delete();
        }
        log.info("完成merge操作!");
        //System.gc();
    }

    public void setStop() {
        this.stop = true;
        log.info("Stopping level 0 merge thread " + this.getName());
    }

    static class QueueElement implements Comparable<QueueElement> {
        HashMapTable hashMapTable;
        Iterator<Map.Entry<ByteArrayWrapper, InMemIndex>> iterator;
        int keyHash;
        byte[] key;
        InMemIndex inMemIndex;

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
                    if (hashMapTable.getCreatedTime() > other.hashMapTable.getCreatedTime()) {
                        return -1;
                    } else if (hashMapTable.getCreatedTime() < other.hashMapTable.getCreatedTime()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }
        }

    }
}