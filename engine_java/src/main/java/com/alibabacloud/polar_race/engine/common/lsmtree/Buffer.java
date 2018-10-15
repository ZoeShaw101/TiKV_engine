package com.alibabacloud.polar_race.engine.common.lsmtree;

import java.util.HashSet;
import java.util.Set;

public class Buffer {
    private static int MAX_SIZE;
    private Set<Entry> entries;

    public Buffer(int maxSize) {
        MAX_SIZE = maxSize;
        entries = new HashSet<>();
    }

    public boolean write(byte[] key, byte[] val) {
        return true;
    }

    public byte[] read(byte[] key) {
        return null;
    }

    public Set<Entry> getEntries() {
        return entries;
    }

    public void clear() {
        entries.clear();
    }
}
