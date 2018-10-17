package com.alibabacloud.polar_race.engine.common.lsmtree;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 内存中的缓存：MemTable
 *
 */

public class MemTable {
    private static int MAX_SIZE;
    private Map<byte[], byte[]> entries;

    public MemTable(int maxSize) {
        MAX_SIZE = maxSize;
        entries = new ConcurrentSkipListMap<>((k1, k2) -> new String(k1).compareTo(new String(k2)));  //按key的大小升序
    }

    public boolean write(byte[] key, byte[] val) {
        if (entries.size() >= MAX_SIZE) {
            return false;
        }
        entries.put(key, val);
        return true;
    }

    public byte[] read(byte[] key) {
        if (entries.containsKey(key))
            return entries.get(key);
        return null;
    }

    public Map<byte[], byte[]> getEntries() {
        return entries;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public void clear() {
        entries.clear();
    }
}
