package com.alibabacloud.polar_race.engine.bucket;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import com.alibabacloud.polar_race.engine.utils.CommonConfig;
import com.alibabacloud.polar_race.engine.utils.FileHelper;
import com.alibabacloud.polar_race.engine.utils.FileManager;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程分桶
 */

public class NewBucketDB {

    private static final Logger logger = LoggerFactory.getLogger(NewBucketDB.class);

    static final String DATA_DIR_PATH = "/data";
    static final String INDEX_DIR_PATH = "/index";
    static final String INDEX_FILE = "/all.idx";

    //每个线程的
    private BucketReaderWriter[] bucketReaderWriters;
    private CountDownLatch[] countDownLatches;
    private ExecutorService executors = Executors.newFixedThreadPool(CommonConfig.MAP_COUNT);

    private long[] sortedKeys;

    public NewBucketDB() {
        this.bucketReaderWriters = new BucketReaderWriter[CommonConfig.MAP_COUNT];
        this.countDownLatches = new CountDownLatch[CommonConfig.MAP_COUNT];
    }

    public void open(String path) throws EngineException {
        long start = System.currentTimeMillis();
        try {
            if (!FileHelper.fileExists(path + DATA_DIR_PATH)) {
                FileHelper.createDir(path + DATA_DIR_PATH);
            }
            if (!FileHelper.fileExists(path + INDEX_DIR_PATH)) {
                FileHelper.createDir(path + INDEX_DIR_PATH);
            }
            for (int i = 0; i < CommonConfig.MAP_COUNT; i++) {
                countDownLatches[i] = new CountDownLatch(1);
                bucketReaderWriters[i] = new BucketReaderWriter(path, i, countDownLatches[i]);
            }
            for (int i = 0; i < CommonConfig.MAP_COUNT; i++) {
                countDownLatches[i].await();
            }
            executors.shutdownNow();

            int mapSize = 0, index = 0;
            for (int i = 0; i < CommonConfig.MAP_COUNT; i++) {
                mapSize += bucketReaderWriters[i].indexMap.size();
            }
            logger.info("MapSize=" + mapSize);

            sortedKeys = new long[mapSize];
            for (int i = 0; i < CommonConfig.MAP_COUNT; i++) {
                Iterator<LongCursor> iter = bucketReaderWriters[i].indexMap.keys().iterator();
                while (iter.hasNext()) {
                    sortedKeys[index++] = iter.next().value;
                }
            }
            Arrays.sort(sortedKeys);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("初始化出错！" + e);
        }
        logger.info("初始化耗时 : " + (System.currentTimeMillis() - start));
    }

    private int keyHash(byte[] key) {
        int keyHash = Arrays.hashCode(key);
        keyHash = Math.abs(keyHash);
        return keyHash % CommonConfig.MAP_COUNT;
    }

    private int keyHash2(byte[] key) {
        return (key[0] + CommonConfig.MAP_COUNT) % CommonConfig.MAP_COUNT;
    }

    private int keyHash3(byte[] key) {
        return (int) (BytesUtil.bytes2long(key, 0) & (CommonConfig.MAP_COUNT - 1));

    }

    private int keyHash4(byte[] key) {
        return FileManager.get(Thread.currentThread().getName());
    }

    public void write(byte[] key, byte[] value) throws EngineException {
        int idx = keyHash2(key);
        bucketReaderWriters[idx].write(key, value);
    }

    public byte[] read(byte[] key) throws EngineException {
        return this.read(key, BytesUtil.bytes2long(key, 0));
    }

    private byte[] read(byte[] key, long keyLong) throws EngineException {
        try {
            int idx = keyHash2(key);
            long pos = bucketReaderWriters[idx].indexMap.getOrDefault(keyLong, -1);
            if (pos == -1) {
                throw new EngineException(RetCodeEnum.NOT_FOUND, "not found");
            }
            return bucketReaderWriters[idx].read(pos << 12);
        } catch (Exception e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found");
        }
    }

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        long min = lower == null ? Long.MIN_VALUE : BytesUtil.bytes2long(lower, 0);
        long max = upper == null ? Long.MAX_VALUE : BytesUtil.bytes2long(upper, 0);
        logger.info("Range lower=" + new String(lower) + ", upper=" + new String(upper));

        for (int i = 0; i < sortedKeys.length; i++) {
            long key = sortedKeys[i];
            if (key < min) continue;
            if (key >= max) break;
            byte[] value  = this.read(BytesUtil.long2bytes(key, 0));
            visitor.visit(BytesUtil.long2bytes(key, 0), value);
        }

        /*int fileId = 0;
        int blockSize = 200 * 1024 * 1024;  //每次映射200M
        while (fileId < CommonConfig.MAP_COUNT) {
            boolean flag = true;
            AtomicInteger atomicInteger = new AtomicInteger();
            if (bucketReaderWriters[fileId].indexWritePos.get() > 0) {
                while (flag) {
                    try {
                        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(blockSize);
                        bucketReaderWriters[fileId].indexFileChannel.read(byteBuffer, atomicInteger.getAndIncrement() * blockSize);
                        byteBuffer.clear();
                        for (int i = 0; i < blockSize / CommonConfig.INDEX_KEY_LENGTH; i++) {
                            long keyLong = byteBuffer.getLong();
                            int pos = byteBuffer.getInt();
                            if (keyLong != 0) {
                                byte[] key = BytesUtil.long2bytes(keyLong, 0);
                                byte[] value = bucketReaderWriters[i].read(pos);
                                visitor.visit(key, value);
                            } else {
                                flag = false;
                                break;
                            }
                        }
                        byteBuffer.clear();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            fileId++;
        }*/
    }

    public void close() {
        logger.info("Close DB");
        for (int i = 0; i < CommonConfig.MAP_COUNT; i++) {
            bucketReaderWriters[i].close();
        }
    }

    class BucketReaderWriter {
        private String path;
        private long fileIdx;
        private RandomAccessFile dataRaf;  //data direct io
        private FileOutputStream dataOS;
        private FileChannel indexFileChannel;
        private MappedByteBuffer indexMappedBuffer;  //index mmap
        private AtomicInteger indexWritePos = new AtomicInteger(0);

        //ConcurrentSkipListMap<Long, Integer> sortedIndexMap = new ConcurrentSkipListMap<>();  //有序的indexMap
        LongIntHashMap indexMap = new LongIntHashMap();

        public BucketReaderWriter(String path, long fileIdx, CountDownLatch latch) throws Exception {
            this.fileIdx = fileIdx;
            this.path = path;
            Init(latch);
        }

        public void Init(CountDownLatch latch) {
            executors.submit(() -> {
                try {
                    String dataFilePath = path + NewBucketDB.DATA_DIR_PATH + "/data_" + fileIdx + ".dat";
                    if (!FileHelper.fileExists(dataFilePath)) {
                        FileHelper.createFile(dataFilePath);
                    }
                    String indexFilePath = path + NewBucketDB.INDEX_DIR_PATH + "/index_" + fileIdx + ".idx";
                    if (!FileHelper.fileExists(indexFilePath)) {
                        FileHelper.createFile(indexFilePath);
                    }

                    dataOS = new FileOutputStream(new File(dataFilePath), true);  //direct io
                    dataRaf = new RandomAccessFile(new File(dataFilePath), "r");
                    indexFileChannel = new RandomAccessFile(new File(indexFilePath), "rw").getChannel();

                    long cnt = dataRaf.length() / (long) CommonConfig.VALUE_SIZE;
                    indexWritePos.set((int) cnt);

                    if (indexWritePos.get() == 0) {
                        indexMappedBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, CommonConfig.IDX_MMAP_FILE_SIZE);
                        return;
                    }
                    int blockSize = 128 * 1024 * 1024, idx = 0;
                    boolean flag = true;
                    while (flag) {
                        try {
                            indexMappedBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, (idx++) * blockSize, blockSize);
                            for (int i = 0; i < blockSize / CommonConfig.INDEX_KEY_LENGTH; i++) {
                                long key = indexMappedBuffer.getLong();
                                int pos = indexMappedBuffer.getInt();
                                if (key != 0) {
                                    indexMap.put(key, pos);
//                                    sortedIndexMap.put(key, pos);
                                } else {
                                    flag = false;
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            logger.error("初始化索引map失败！");
                        }
                    }
                    FileManager.unmap(indexMappedBuffer);

                    ////sort
                    /*ByteBuffer oldValueBuffer = ByteBuffer.allocate(CommonConfig.VALUE_SIZE);
                    ByteBuffer newValueBuffer = ByteBuffer.allocate(CommonConfig.VALUE_SIZE);
                    AtomicInteger sortedPos = new AtomicInteger(0);
                    sortedIndexMap.forEach((key, pos) -> {
                        long oldPos = (long) pos;
                        long newPos = sortedPos.getAndIncrement();
                        try {
                            dataRaf.getChannel().read(oldValueBuffer, oldPos * CommonConfig.VALUE_SIZE);
                            dataRaf.getChannel().read(newValueBuffer, newPos * CommonConfig.VALUE_SIZE);
                            oldValueBuffer.clear();
                            newValueBuffer.clear();
                            dataRaf.getChannel().write(newValueBuffer, oldPos * CommonConfig.VALUE_SIZE);
                            dataRaf.getChannel().write(oldValueBuffer, newPos * CommonConfig.VALUE_SIZE);
                            sortedIndexMap.put(key, (int) newPos);
                            oldValueBuffer.clear();
                            newValueBuffer.clear();
                        } catch (IOException e) {
                            logger.error("排序indexMap出错！" + e);
                        }
                    });*/

                } catch (Exception e) {
                    logger.error("Index map " + this.fileIdx + " 初始化失败" + e);
                } finally {
                    latch.countDown();
                }
            });
        }

        public synchronized byte[] read(long pos) throws Exception {
            byte[] ret = new byte[CommonConfig.VALUE_SIZE];
            dataRaf.seek(pos);
            dataRaf.read(ret);
            return ret;
        }

        public synchronized void write(byte[] key, byte[] val) throws EngineException {
            try {
                dataOS.write(val);
                int pos = indexWritePos.getAndIncrement();
                indexMappedBuffer.putLong(BytesUtil.bytes2long(key, 0));
                indexMappedBuffer.putInt(pos);
                if (!indexMappedBuffer.hasRemaining()) {
                    indexMappedBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,
                            indexWritePos.get() * CommonConfig.INDEX_KEY_LENGTH, CommonConfig.IDX_MMAP_FILE_SIZE);
                }

            } catch (Exception e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "写失败 : " + new String(key));
            }
        }

        public void close() {
            try {
                dataOS.flush();
                dataOS.getFD().sync();
                dataOS.close();
                indexMappedBuffer.force();
                indexFileChannel.close();
                dataRaf.close();
            } catch (IOException e) {
                logger.error("关闭数据库出错！"  + e);
            }
        }
    }
}
