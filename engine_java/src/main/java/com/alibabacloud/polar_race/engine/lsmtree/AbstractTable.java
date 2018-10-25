package com.alibabacloud.polar_race.engine.lsmtree;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTable {

    protected AtomicBoolean immutable = new AtomicBoolean(false);

    public abstract boolean put(byte[] key, byte[] value);

    public abstract byte[] get(byte[] key);

    public void markImmutable(boolean immutable) {
        this.immutable.set(immutable);
    }
}
