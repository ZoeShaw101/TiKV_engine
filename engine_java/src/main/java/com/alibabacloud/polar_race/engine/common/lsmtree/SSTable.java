package com.alibabacloud.polar_race.engine.common.lsmtree;

import com.alibabacloud.polar_race.engine.common.utils.KVEntry;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * 使用NIO的内存映射文件来加速文件读写
 *
 */

public class SSTable {
    private Logger logger = Logger.getLogger(SSTable.class);

    private static final String DB_STORE_PATH = "/lsmdb/data/table0.sst";
    private long maxSize;
    private double BFbitPerEntry;
    private BloomFilter bloomFilter;
    private List<Long> fencePointers;

    private long mappingOffset;
    private int mappingFileId;

    public SSTable(long maxSize, double BFbitPerEntry) {
        this.maxSize = maxSize;
        this.BFbitPerEntry = BFbitPerEntry;
        bloomFilter = new BloomFilter((long) (maxSize * BFbitPerEntry));

        mappingOffset = 0;
        mappingFileId = -1;
    }

    public void write(byte[] key, byte[] value) {
        bloomFilter.set(key);
        RandomAccessFile file = null;
        KVEntry mapping = new KVEntry(key, value);
        long mappingLength = mapping.getKeySize() + mapping.getValueSize();
        try {
            file = new RandomAccessFile(DB_STORE_PATH, "rw");
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, mappingOffset, mappingLength);
            buffer.put(mapping.getBytes());
            logger.info("写入内存映射：key=" + new String(key) + ", value=" + new String(value));  //本地测试的时候key value都是String类型
        } catch (Exception e) {
            logger.error("内存映射文件错误" + e);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    logger.error("关闭文件出错" + e);
                }
            }
        }
        mappingOffset += mappingLength;
    }

    public byte[] read(byte[] key) {

        return null;
    }
}
