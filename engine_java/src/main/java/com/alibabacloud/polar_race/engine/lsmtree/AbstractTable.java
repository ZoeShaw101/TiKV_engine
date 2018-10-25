package com.alibabacloud.polar_race.engine.lsmtree;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractTable {

    protected long maxSize;  //当前table最多的entry个数
    protected AtomicLong size = new AtomicLong(0);  //当前table里的entry个数
    protected AtomicBoolean immutable = new AtomicBoolean(false);

    public abstract boolean put(byte[] key, byte[] value);

    public abstract byte[] get(byte[] key);

    public void markImmutable(boolean immutable) {
        this.immutable.set(immutable);
    }

    public long getMaxSize() {
        return maxSize;
    }

    public long getSize() {
        return size.get();
    }

}
