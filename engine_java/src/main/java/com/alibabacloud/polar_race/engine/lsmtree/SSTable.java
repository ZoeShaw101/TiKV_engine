package com.alibabacloud.polar_race.engine.lsmtree;

import com.alibabacloud.polar_race.engine.utils.FileHelper;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import org.apache.log4j.Logger;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 使用NIO的内存映射文件来加速文件读写
 *
 */

public class SSTable extends AbstractTable {
    private Logger logger = Logger.getLogger(SSTable.class);

    private byte[] maxKey;   //维护一个table内最大的key值
    private int tableIndex;
    private int levelIndex;
    private String tableFilePath;

    private GuavaBloomFilter bloomFilter;
    private List<byte[]> fencePointers;   //每个SSTbale的key指针，可以看成是block的稀疏索引

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    public SSTable(long maxSize, double BFbitPerEntry, int tableIndex, int levelIndex,
                   byte[] maxKey, List<byte[]> fencePointers, GuavaBloomFilter bloomFilter) {
        this.maxSize = maxSize;
        this.tableIndex = tableIndex;
        this.levelIndex = levelIndex;
        //bloomFilter = new BloomFilter((long) (maxSize * BFbitPerEntry));
        if (maxKey != null)
            this.maxKey = maxKey;
        else
            this.maxKey = new byte[0];
        if (fencePointers != null)
            this.fencePointers = fencePointers;
        else
            this.fencePointers = new ArrayList<>();
        if (bloomFilter != null)
            this.bloomFilter = bloomFilter;
        else
            this.bloomFilter = new GuavaBloomFilter(maxSize);

        tableFilePath = LSMTree.DB_STORE_DIR + "/level" + levelIndex + "_table" + tableIndex + ".sst";
        if (!FileHelper.fileExists(tableFilePath)) {
            try {
                FileHelper.createFile(tableFilePath);
            } catch (Exception e) {
                logger.error("创建sstable文件失败" + e);
            }
        }
    }

    public boolean put(byte[] key, byte[] value) {
        if( size.get() >= maxSize) {
            logger.error("level=" + levelIndex + ", table=" + tableIndex + " 已超过最大长度限制");
            return false;
        }
        readWriteLock.writeLock().lock();
        bloomFilter.set(key);
        RandomAccessFile file = null;
        KVEntry mapping = new KVEntry(key, value);
        long mappingLength = LSMTree.KEY_BYTE_SIZE + LSMTree.VALUE_BYTE_SIZE;
        try {
            file = new RandomAccessFile(tableFilePath, "rw");
            long offset  = file.length();
            if (offset % LSMTree.BLOCK_SIZE == 0) {   //那么fencePointer的大小就等于table中页数的大小，i处的值就是该table中第i页的第一个key的值
                fencePointers.add(key);
            }
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, offset, mappingLength);
            buffer.put(mapping.toBytes());
            if (maxKey.length == 0 || BytesUtil.KeyComparator(maxKey, key) < 0) {
                maxKey = key;
            }
            size.getAndIncrement();
            if (LSMTree.DEBUG_ENABLE) {
                logger.info("数据写入内存映射文件level=" + levelIndex +  ", table=" + tableIndex +
                        ", offset=" + offset +": key=" + new String(key) + " ,当前table内的entry个数为size=" + size);
            }
        } catch (Exception e) {
            logger.error("内存映射文件错误" + e);
        } finally {
            FileHelper.closeFile(file);
            readWriteLock.writeLock().unlock();
        }
        return true;
    }

    public byte[] get(byte[] key) {
        byte[] val = null;
        readWriteLock.readLock().lock();
        if (!bloomFilter.isSet(key) || !checkKeyBound(key)) {
            return val;
        }
        /*if (!checkKeyBound(key)) {
            return val;
        }*/
        int nextPage = findUpperBound(key);
        int pageIndex = nextPage == 0 ? 0 : nextPage - 1;
        RandomAccessFile file = null;
        byte[] readBytes = new byte[LSMTree.BLOCK_SIZE];
        try {
            file = new RandomAccessFile(tableFilePath, "rw");
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, pageIndex * LSMTree.BLOCK_SIZE, LSMTree.BLOCK_SIZE);
            buffer.get(readBytes);
        } catch (Exception e) {
            logger.error("读取SSTable出错" + e);
        } finally {
            FileHelper.closeFile(file);
            readWriteLock.readLock().unlock();
        }
        //todo: 这里顺序找效率低 => 也可以变为二分
        byte[] tmpKey = new byte[LSMTree.KEY_BYTE_SIZE];
        int entryNum = LSMTree.BLOCK_SIZE / LSMTree.ENTRY_BYTE_SIZE;
        for (int i = 0, start = 0; i < entryNum; i++, start += LSMTree.ENTRY_BYTE_SIZE) {
            System.arraycopy(readBytes, start, tmpKey, 0, LSMTree.KEY_BYTE_SIZE);
            if (Arrays.equals(key, tmpKey)) {
                val = new byte[LSMTree.VALUE_BYTE_SIZE];
                System.arraycopy(readBytes, start + LSMTree.KEY_BYTE_SIZE, val, 0, LSMTree.VALUE_BYTE_SIZE);
                break;
            }
        }
        if (val != null && LSMTree.DEBUG_ENABLE) {
            logger.info("从SSTable读出key=" + new String(key) + ", value=" + new String(val));
        }
        return val;
    }

    private boolean checkKeyBound(byte[] key) {
        if (maxKey.length != 0 && BytesUtil.KeyComparator(key, maxKey) > 0)
            return false;
        if (BytesUtil.KeyComparator(key, fencePointers.get(0)) < 0)
            return false;
        return true;
    }

    /**
     * 二分查找，在SSTable中内找到最后一个大于当前key的block index
     */
    private int findUpperBound(byte[] key) {
        int begin = 0, end = fencePointers.size();
        while (begin < end) {
            final int mid = begin + (end - begin) / 2;
            if (BytesUtil.KeyComparator(fencePointers.get(mid), key) <= 0) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }

    public void close() {

    }

    public byte[] getMaxKey() {
        return maxKey;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getLevelIndex() {
        return levelIndex;
    }

    public void setTableIndex(int tableIndex) {
        this.tableIndex = tableIndex;
    }

    public void setLevelIndex(int levelIndex) {
        this.levelIndex = levelIndex;
    }

    public String getTableFilePath() {
        return tableFilePath;
    }

    public GuavaBloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public List<byte[]> getFencePointers() {
        return fencePointers;
    }
}
