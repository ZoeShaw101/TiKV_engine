package com.alibabacloud.polar_race.engine.core.stats;

import java.util.concurrent.atomic.AtomicLong;

public class SingleStats {
    private final AtomicLong value = new AtomicLong(0);

    public void increaseValue() {
        value.getAndIncrement();
    }

    public void setValue(long val) {
        value.set(val);
    }

    public long getValue() {
        return value.get();
    }
}
