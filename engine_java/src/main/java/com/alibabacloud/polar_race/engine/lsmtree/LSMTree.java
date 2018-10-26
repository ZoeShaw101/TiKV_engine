package com.alibabacloud.polar_race.engine.lsmtree;

import com.alibabacloud.polar_race.engine.utils.DaemonThreadFactory;
import com.alibabacloud.polar_race.engine.utils.FileHelper;
import com.alibabacloud.polar_race.engine.utils.Serialization;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Log Structured Merge Tree
 * 核心思想：将随机写转换为顺序写来大幅提高写入操作的性能
 *
 *  当内存中的MemTable满了的时候，需要将MemTable和磁盘的第一个level作归并排序
 *  SSTable: 内存中的数据结构，包括磁盘中排序文件的指针、一个固定大小的布隆过滤器和一个范围指针
 *
 *  Merge策略：K路归并排序
 *
 */

public class LSMTree {
    private Logger logger = Logger.getLogger(LSMTree.class);

    //系统参数，可用于调节性能
    static final double BF_BITS_PER_ENTRY = 0.5;
    static final int TREE_DEPTH = 5;
    static final int TREE_FANOUT = 4;  //每层level的sstable个数
    static final int BUFFER_NUM_BLOCKS = 50;
    static final int THREAD_COUNT = 4;
    static final int KEY_BYTE_SIZE = 8;  //4B
    static final int VALUE_BYTE_SIZE = 4000;   //4KB
    static final int ENTRY_BYTE_SIZE = 4008;
    static final int BLOCK_SIZE = ENTRY_BYTE_SIZE * 100;  //每个SSTable的BLOCK大小
    static final int BUFFER_MAX_ENTRIES = BUFFER_NUM_BLOCKS * BLOCK_SIZE / ENTRY_BYTE_SIZE;  //最大页数 * 每页大小 ／ Entry大小 => 500个entry
    static final double FALSE_POSITIVE_PROBABILITY = 0.0001;

    static String DB_STORE_DIR;
    private static String MANIFEST_FILE_PATH ; //记录当前level的sstable的maxKey bloomFilter fencePointer
    private static String LOG_FILE_PATH;

    //内存中的
    private MemTable memTable;
    private List<Level> levels;
    private ManifestInfo manifestInfo;

    private Object sstableCreationLock;
    private LevelMerger levelMerger;

    //private ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final AtomicInteger count = new AtomicInteger(0);

    public LSMTree(String path) {
        DB_STORE_DIR = path;
        MANIFEST_FILE_PATH =  DB_STORE_DIR + "/manifest";
        LOG_FILE_PATH = DB_STORE_DIR +  "/redo.log";

        memTable = new MemTable(BUFFER_MAX_ENTRIES);
        levels = new ArrayList<>();
        sstableCreationLock = new Object();

        long maxRunSize = BUFFER_MAX_ENTRIES;
        int depth = TREE_DEPTH;
        while ((depth--) > 0) {
            levels.add(new Level(TREE_FANOUT, maxRunSize));
            maxRunSize *= TREE_FANOUT;
        }

        Init();
    }

    private void Init() {
        if (!FileHelper.fileExists(DB_STORE_DIR)) {
            try {
                FileHelper.createDir(DB_STORE_DIR);
            } catch (Exception e) {
                logger.error("创建数据库目录失败" + e);
            }
        }
        if (!FileHelper.fileExists(LOG_FILE_PATH)) {
            try {
                FileHelper.createFile(LOG_FILE_PATH);
            } catch (Exception e) {
                logger.error("创建日志文件失败" + e);
            }
        }
        if (!FileHelper.fileExists(MANIFEST_FILE_PATH)) {
            try {
                FileHelper.createFile(MANIFEST_FILE_PATH);
                manifestInfo = new ManifestInfo();
            } catch (Exception e) {
                logger.error("创建manifest文件失败" + e);
            }
        } else {
            manifestInfo = loadManifestInfos();
        }
        initLevels();
        checkRedoLog();
    }

    private void startLevelMerger() {
        levelMerger = new LevelMerger(this.levels, this.manifestInfo);
        levelMerger.start();
    }

    private void initLevels() {
        List<String> fileNames = FileHelper.getDirFiles(DB_STORE_DIR);
        for (String fileName : fileNames) {
            if (!fileName.startsWith("level")) continue;
            String[] prefix = fileName.split("\\.")[0].split("_");
            int levelIdx = Integer.valueOf(prefix[0].substring(5));
            int tableIdx = Integer.valueOf(prefix[1].substring(5));
            int idx = levelIdx * TREE_FANOUT + tableIdx;
            levels.get(levelIdx).getRuns().addFirst(new SSTable(
                    levels.get(levelIdx).getMaxRunSize(),
                    BF_BITS_PER_ENTRY,
                    tableIdx,
                    levelIdx,
                    manifestInfo.getMaxKeyInfos().get(idx),
                    manifestInfo.getFencePointerInfos().get(idx),
                    manifestInfo.getBloomFilterInfos().get(idx)));
        }
    }

    private ManifestInfo loadManifestInfos() {
        RandomAccessFile file = null;
        ManifestInfo info = null;
        try {
            file = new RandomAccessFile(MANIFEST_FILE_PATH, "rw");
            byte[] infos = new byte[(int) file.length()];
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
            buffer.get(infos);
            info = (ManifestInfo) Serialization.deserialize(infos);
        } catch (Exception e) {
            logger.info("初始化读manifest文件出错" + e);
        } finally {
            FileHelper.closeFile(file);
        }
        return info;
    }

    private void checkRedoLog() {
        File file = new File(LOG_FILE_PATH);
        if (file.length() == 0) return;
        List<Object> entries = FileHelper.readObjectFromFile(LOG_FILE_PATH);
        logger.info("需要恢复的数据有" + entries.size() + "个");
        for (Object o : entries) {
            KVEntry e = (KVEntry) o;
            this.put(e.getKey(), e.getValue());
            logger.info("恢复数据：key=" + new String(e.getKey()) + ", value=" + new String(e.getValue()));
        }
    }

    /**
     * 一次写入操作只涉及一次磁盘顺序写或一次内存写入，所以很快
     */
    public void put(byte[] key, byte[] value) {
        rwLock.writeLock().lock();
        FileHelper.writeObjectToFile(new KVEntry(key, value), LOG_FILE_PATH);
        rwLock.writeLock().unlock();
        this.put(key, value, System.currentTimeMillis());
        rwLock.writeLock().lock();
        FileHelper.clearFileContent(LOG_FILE_PATH);
        rwLock.writeLock().unlock();
    }

    private void put(byte[] key, byte[] value, long createdTime) {
        try {
            boolean success = memTable.put(key, value);
            if (!success) {   //写满了
                synchronized (sstableCreationLock) {
                    success = memTable.put(key, value);
                    if (!success) {  //刷到level 0 磁盘
                        //------ 这里应该等当前merge结束再能写第一个level0的table ------
                        if (!levels.get(0).hasRemaining()) {
                            this.startLevelMerger();
                            levelMerger.getCountDownLatch().await();
                        }
                        memTable.markImmutable(true);
                        Level level = levels.get(0);
                        level.getWriteLock().lock();
                        int idx = levels.get(0).getRuns().size();
                        if (level.getRuns().size() < level.getMaxRuns()) {
                            level.getRuns().addFirst(new SSTable(
                                    levels.get(0).getMaxRunSize(),
                                    BF_BITS_PER_ENTRY,
                                    levels.get(0).getRuns().size(),
                                    0,
                                    manifestInfo.getMaxKeyInfos().get(idx),
                                    manifestInfo.getFencePointerInfos().get(idx),
                                    manifestInfo.getBloomFilterInfos().get(idx)));
                        }
                        level.getWriteLock().unlock();
                        for (final Map.Entry<byte[], byte[]> entry : memTable.getEntries().entrySet()) {
                            levels.get(0).getRuns().getFirst().put(entry.getKey(), entry.getValue());
                        }
                        memTable.markImmutable(false);
                        memTable.clear();
                        memTable.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("写入数据出错！" + e);
        }
    }

    public byte[] get(final byte[] key) {
        byte[] latestVal;
        //0.先在buffer中找
        if ((latestVal = memTable.get(key)) != null)
            return latestVal;
        //1.buffer中不存在，则在runs中查找
        for (Level level : levels) {
            for (AbstractTable ssTable : level.getRuns()) {
                latestVal = ssTable.get(key);
                if (latestVal != null)
                    return latestVal;
            }
        }
        return latestVal;
    }

    /**
     * 关闭系统
     * 0.将内存的Memtable中的信息刷到磁盘
     * 1.将每个level的信息都保存到manifest文件中
     *
     * todo:系统在关闭前意外退出，那么有的信息可能没被保存下来
     */
    public void close() {
        if (!memTable.isEmpty()) {
            try {
                if (!levels.get(0).hasRemaining()) {
                    this.startLevelMerger();
                    levelMerger.getCountDownLatch().await();
                }
                Level level = levels.get(0);
                level.getWriteLock().lock();
                int idx = levels.get(0).getRuns().size();
                if (level.getRuns().size() < level.getMaxRuns()) {
                    level.getRuns().addFirst(new SSTable(
                            levels.get(0).getMaxRunSize(),
                            BF_BITS_PER_ENTRY,
                            levels.get(0).getRuns().size(),
                            0,
                            manifestInfo.getMaxKeyInfos().get(idx),
                            manifestInfo.getFencePointerInfos().get(idx),
                            manifestInfo.getBloomFilterInfos().get(idx)));
                }
                level.getWriteLock().unlock();
                for (final Map.Entry<byte[], byte[]> entry : memTable.getEntries().entrySet()) {
                    levels.get(0).getRuns().getFirst().put(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                logger.error("将最后内存中的数据写会磁盘出错！" + e);
            }
        }
        for (Level level : levels) {
            for (AbstractTable atable : level.getRuns()) {
                SSTable table = (SSTable) atable;
                int idx = table.getTableIndex() * table.getLevelIndex();
                manifestInfo.getMaxKeyInfos().put(idx, table.getMaxKey());
                manifestInfo.getFencePointerInfos().put(idx, table.getFencePointers());
                manifestInfo.getBloomFilterInfos().put(idx, table.getBloomFilter());
            }
        }

        RandomAccessFile file = null;
        byte[] infos = null;
        try {
            infos = Serialization.serialize(manifestInfo);
            file = new RandomAccessFile(MANIFEST_FILE_PATH, "rw");
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, infos.length);
            buffer.put(infos);
        } catch (Exception e) {
            logger.info("写manifest文件出错" + e);
        } finally {
            FileHelper.closeFile(file);
            logger.info("关闭系统");
        }

        levelMerger.setStop();
    }

}
