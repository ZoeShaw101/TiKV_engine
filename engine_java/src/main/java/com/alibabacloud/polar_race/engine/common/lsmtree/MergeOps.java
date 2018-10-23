package com.alibabacloud.polar_race.engine.common.lsmtree;

import com.alibabacloud.polar_race.engine.common.utils.BytesUtil;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * 归并排序
 */

public class MergeOps {
    private Logger logger = Logger.getLogger(MergeOps.class);

    class MergeEntry implements Comparable<MergeEntry> {
        byte[] mapping;
        int precedence;
        long currentIndex;
        long entryNum;

        MergeEntry(byte[] mapping, int precedence, long entryNum) {
            this.mapping = mapping;
            this.precedence = precedence;
            this.entryNum = entryNum;
            currentIndex = 0;
        }

        KVEntry head() {
            return new KVEntry(Arrays.copyOfRange(mapping, (int) currentIndex, (int) currentIndex + LSMTree.KEY_BYTE_SIZE),
                    Arrays.copyOfRange(mapping, (int) currentIndex + LSMTree.KEY_BYTE_SIZE,
                            (int) currentIndex + LSMTree.KEY_BYTE_SIZE + LSMTree.VALUE_BYTE_SIZE));
        }

        boolean isDone() {return currentIndex == entryNum;}

        @Override
        public int compareTo(MergeEntry o) {
            if (this.head().equals(o.head())) {
                return this.precedence - o.precedence;  //保证同一个的SSTable里相同key只保留最近写入的一个记录
            } else {
                return BytesUtil.KeyComparator(this.head().getKey(), o.head().getKey());
            }
        }
    }

    private BlockingQueue<MergeEntry> priorityQueue;

    public MergeOps() {
        this.priorityQueue = new PriorityBlockingQueue<>();
    }

    public List<SSTable> merge() {
        return null;
    }

    public void add(byte[] mapping, long entryNum) {
        MergeEntry mergeEntry = new MergeEntry(mapping, priorityQueue.size(), entryNum);
        priorityQueue.add(mergeEntry);
    }

    public KVEntry next() {
        MergeEntry current, next;
        current = priorityQueue.peek();
        next = new MergeEntry(current.mapping, current.precedence, current.entryNum);
        next.currentIndex = current.currentIndex;
        //保证同一个的SSTable里相同key只保留最近写入的一个记录
        while ((Arrays.equals(next.head().getKey(), current.head().getKey())) && !priorityQueue.isEmpty()) {
            priorityQueue.poll();
            next.currentIndex++;
            if (!next.isDone()) priorityQueue.add(next);
            if (priorityQueue.isEmpty()) break;
            next = priorityQueue.peek();
        }
        return current.head();  //返回的是这段的mapping的头一个head
    }

    public boolean isDone() {
        return priorityQueue.isEmpty();
    }
}
