package com.alibabacloud.polar_race.engine.common.lsmtree;

import com.alibabacloud.polar_race.engine.common.utils.FileHelper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * 使用NIO的内存映射文件来加速文件读写
 *
 */

public class SSTable {
    private Logger logger = Logger.getLogger(SSTable.class);

    static final String DB_STORE_DIR = "/lsmdb";
    private long maxSize;
    private long size;
    private String minKey;
    private String maxKey;   //维护一个table内最大的key值
    private int tableIndex;
    private int levelIndex;
    private String tableFilePath;
    private BloomFilter bloomFilter;
    private List<byte[]> fencePointers;   //每个SSTbale的key指针
    private long mappingOffset;   //即当前的SSTable的size

    public SSTable(long maxSize, double BFbitPerEntry, int tableIndex, int levelIndex) {
        this.maxSize = maxSize;
        this.tableIndex = tableIndex;
        this.levelIndex = levelIndex;
        bloomFilter = new BloomFilter((long) (maxSize * BFbitPerEntry));
        fencePointers = new ArrayList<>();
        mappingOffset = 0;
        size = 0;
        maxKey = "";
        minKey = "";
        if (!FileHelper.fileExists(DB_STORE_DIR)) {
            try {
                FileHelper.createDir(DB_STORE_DIR);
            } catch (Exception e) {
                logger.error("创建数据库目录失败" + e);
            }
        }
        tableFilePath = DB_STORE_DIR + "/level" + levelIndex + "_table" + tableIndex + ".sst";
        if (!FileHelper.fileExists(tableFilePath)) {
            try {
                FileHelper.createFile(tableFilePath);
            } catch (Exception e) {
                logger.error("创建sstable文件失败" + e);
            }
        }
    }

    public void write(byte[] key, byte[] value) {
        assert mappingOffset < maxSize;
        bloomFilter.set(key);
        if (mappingOffset % LSMTree.BLOCK_SIZE == 0) {   //那么fencePointer的大小就等于table中页数的大小，i处的值就是该table中第i页的第一个key的值
            fencePointers.add(key);
        }
        RandomAccessFile file = null;
        KVEntry mapping = new KVEntry(key, value);
        long mappingLength = LSMTree.KEY_BYTE_SIZE + LSMTree.VALUE_BYTE_SIZE;
        try {
            file = new RandomAccessFile(tableFilePath, "rw");
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, mappingOffset, mappingLength);
            buffer.put(mapping.toBytes());
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
        if (minKey.length() == 0 || minKey.compareTo(new String(key)) > 0) {
            minKey = new String(key);
        }
        if (maxKey.length() == 0 || maxKey.compareTo(new String(key)) < 0) {
            maxKey = new String(key);
        }
        mappingOffset += mappingLength;
        size++;
    }

    public byte[] read(byte[] key) {
        byte[] val = null;
        if (!bloomFilter.isSet(key) || !checkKeyBound(key)) {
            return val;
        }
        int nextPage = findUpperBound(key);
        int pageIndex = nextPage - 1;
        RandomAccessFile file = null;
        byte[] readBytes = new byte[LSMTree.BLOCK_SIZE];
        try {
            file = new RandomAccessFile(tableFilePath, "rw");
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, pageIndex * LSMTree.BLOCK_SIZE, LSMTree.BLOCK_SIZE);
            buffer.get(readBytes);
        } catch (Exception e) {
            logger.error("读取SSTable出错" + e);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    logger.error("关闭文件出错" + e);
                }
            }
        }
        //todo: 这里顺序找效率低
        for (int i = 0; i < LSMTree.BLOCK_SIZE / LSMTree.ENTRY_BYTE_SIZE; i += LSMTree.ENTRY_BYTE_SIZE) {
            byte[] tmpKey = new byte[LSMTree.KEY_BYTE_SIZE];
            int idx = 0, j = i;
            for (; j < LSMTree.KEY_BYTE_SIZE; j++) {
                tmpKey[idx++] = readBytes[j];
            }
            if (Arrays.equals(key, tmpKey)) {
                val = new byte[LSMTree.VALUE_BYTE_SIZE];
                System.arraycopy(readBytes, j, val, 0, LSMTree.VALUE_BYTE_SIZE);
                break;
            }
        }
        logger.info("从SSTable读出key=" + new String(key) + ", value=" + new String(val));
        return val;
    }

    private boolean checkKeyBound(byte[] key) {
        if (new String(key).compareTo(maxKey) > 0)
            return false;
        if (new String(key).compareTo(new String(fencePointers.get(0))) < 0)
            return false;
        return true;
    }

    /**
     * 二分查找，在SSTable中内找到最后一个大于当前key的block index
     */
    private int findUpperBound(byte[] key) {
        int begin = 0, end = fencePointers.size() - 1;
        while (begin < end) {
            int mid = begin + (end - begin) / 2;
            if (new String(fencePointers.get(mid)).compareTo(new String(key)) < 0) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }


    public String getMaxKey() {
        return maxKey;
    }

    public String getMinKey() {
        return minKey;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void close() {

    }

    public long getSize() {
        return size;
    }

    public Logger getLogger() {
        return logger;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getLevelIndex() {
        return levelIndex;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void setTableIndex(int tableIndex) {
        this.tableIndex = tableIndex;
    }

    public void setLevelIndex(int levelIndex) {
        this.levelIndex = levelIndex;
    }

    public static String getDbStoreDir() {
        return DB_STORE_DIR;
    }

    public String getTableFilePath() {
        return tableFilePath;
    }
}
