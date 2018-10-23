package com.alibabacloud.polar_race.engine.common.core;

import com.alibabacloud.polar_race.engine.common.core.table.AbstractMapTable;

import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LevelQueue extends LinkedList<AbstractMapTable> {

    private static final long serialVersionUID = 1L;

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
    private ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();

    public ReentrantReadWriteLock.WriteLock getWriteLock() {
        return writeLock;
    }

    public ReentrantReadWriteLock.ReadLock getReadLock() {
        return readLock;
    }

}
