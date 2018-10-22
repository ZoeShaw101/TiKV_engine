package com.alibabacloud.polar_race.engine.common.lsmtree;

import javafx.util.Pair;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;


/**
 * Level逻辑结构
 * 每个Level中包含一些Run，这些Run的以时间顺序组织：最近使用的Run都在level的最前面
 */
public class Level {
    private int maxRuns;
    private long maxRunSize;
    private Deque<SSTable> runs;  //最近时间的插入到最前面，LRU

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private WriteLock writeLock = readWriteLock.writeLock();
    private ReadLock readLock = readWriteLock.readLock();


    public Level(int maxRuns, long maxRunSize) {
        this.maxRuns = maxRuns;
        this.maxRunSize = maxRunSize;
        runs = new ArrayDeque<>();
    }

    public Deque<SSTable> getRuns() {
        return runs;
    }

    public long getMaxRunSize() {
        return maxRunSize;
    }

    public boolean hasRemaining() {
        //条件应该是当前层还有空的table或者还有table没写满
        return maxRuns > runs.size() || runs.getFirst().getSize() < runs.getFirst().getMaxSize();
    }

    public WriteLock getWriteLock() {
        return writeLock;
    }

    public ReadLock getReadLock() {
        return readLock;
    }
}
