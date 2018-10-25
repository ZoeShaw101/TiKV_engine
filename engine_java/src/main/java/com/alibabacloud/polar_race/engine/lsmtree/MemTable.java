package com.alibabacloud.polar_race.engine.lsmtree;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 内存中的缓存：MemTable
 *
 */

public class MemTable extends AbstractTable implements Cloneable {
    private Logger logger = Logger.getLogger(MemTable.class);

    private static int MAX_SIZE;
    private Map<byte[], byte[]> entries;

    public MemTable(int maxSize) {
        MAX_SIZE = maxSize;
        entries = new ConcurrentSkipListMap<>((k1, k2) -> new String(k1).compareTo(new String(k2)));  //按key的大小升序
    }

    public boolean put(byte[] key, byte[] val) {
        if (entries.size() >= MAX_SIZE) {
            return false;
        }
        entries.put(key, val);
        logger.info("数据写入内存表key=" + new String(key) + ", memtable.size=" + entries.size());
        return true;
    }

    public byte[] get(byte[] key) {
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

    @Override
    public MemTable clone() {
        try {
            MemTable table = (MemTable) super.clone();
            table.entries = ((ConcurrentSkipListMap<byte[], byte[]>) this.entries).clone();
            return table;
        } catch (CloneNotSupportedException e) {
            logger.error("克隆对象出错！");
        }
        return null;
    }
}
