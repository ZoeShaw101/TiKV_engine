package com.alibabacloud.polar_race.engine.common.lsmtree;

import com.alibabacloud.polar_race.engine.common.utils.Utils;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 归并排序
 */

public class MergeOps {
    private Logger logger = Logger.getLogger(MergeOps.class);

    class MergeEntry {
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

        //todo: 这种数组直接拷贝的方式是否比较低效
        KVEntry head() {
            return new KVEntry(Arrays.copyOfRange(mapping, (int) currentIndex, (int) currentIndex + LSMTree.KEY_BYTE_SIZE),
                    Arrays.copyOfRange(mapping, (int) currentIndex + LSMTree.KEY_BYTE_SIZE,
                            (int) currentIndex + LSMTree.KEY_BYTE_SIZE + LSMTree.VALUE_BYTE_SIZE));
        }

        boolean isDone() {return currentIndex == entryNum;}
    }

    private PriorityQueue<MergeEntry> priorityQueue;

    public MergeOps() {
        this.priorityQueue = new PriorityQueue<>((a, b) -> {
            if (a.head().equals(b.head())) {
                return b.precedence - a.precedence;  //保证同一个的SSTable里相同key只保留最近写入的一个记录
            } else {
                return Utils.KeyComparator(a.head().getKey(), b.head().getKey(), LSMTree.KEY_BYTE_SIZE);
            }
        });
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
        next = current;
        //保证同一个的SSTable里相同key只保留最近写入的一个记录
        while ((Arrays.equals(next.head().getKey(), current.head().getKey())) && !priorityQueue.isEmpty()) {
            priorityQueue.poll();
            next.currentIndex++;
            if (!next.isDone()) priorityQueue.add(next);
            next = priorityQueue.peek();
        }
        return current.head();
    }

    public boolean isDone() {
        return priorityQueue.isEmpty();
    }
}
