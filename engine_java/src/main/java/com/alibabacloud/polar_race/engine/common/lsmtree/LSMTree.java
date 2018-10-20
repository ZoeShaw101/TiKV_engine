package com.alibabacloud.polar_race.engine.common.lsmtree;

import com.alibabacloud.polar_race.engine.common.utils.FileHelper;
import com.alibabacloud.polar_race.engine.common.utils.Serialization;
import com.alibabacloud.polar_race.engine.common.wal.RedoLog;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
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
    static final int TREE_FANOUT = 10;  //每层level的sstable个数
    static final int BUFFER_NUM_BLOCKS = 5;
    static final int THREAD_COUNT = 4;
    static final int KEY_BYTE_SIZE = 8;  //4B
    static final int VALUE_BYTE_SIZE = 4000;   //4KB
    static final int ENTRY_BYTE_SIZE = 4008;
    static final int BLOCK_SIZE = ENTRY_BYTE_SIZE * 100;  //每个SSTable的BLOCK大小
    static final int BUFFER_MAX_ENTRIES = BUFFER_NUM_BLOCKS * BLOCK_SIZE / ENTRY_BYTE_SIZE;  //最大页数 * 每页大小 ／ Entry大小 => 500个entry
    static final double FALSE_POSITIVE_PROBABILITY = 0.0001;

    static final String DB_STORE_DIR = "/Users/shaw/lsmdb";
    private static final String MANIFEST_FILE_PATH = "/Users/shaw/lsmdb/manifest"; //记录当前level的sstable的maxKey bloomFilter fencePointer
    private static final String LOG_FILE_PATH = "/Users/shaw/lsmdb/redo.log";

    //内存中的
    private MemTable memTable;
    private List<Level> levels;
    private ManifestInfo manifestInfo;

    //private ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    public LSMTree(String path) {
        memTable = new MemTable(BUFFER_MAX_ENTRIES);
        levels = new ArrayList<>();
        //thinIndex = new HashMap<>();
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
            write(e.getKey(), e.getValue());
            logger.info("恢复数据：key=" + new String(e.getKey()) + ", value=" + new String(e.getValue()));
        }
    }

    /**
     * 一次写入操作只涉及一次磁盘顺序写或一次内存写入，所以很快
     */
    public void write(byte[] key, byte[] value) {
        FileHelper.writeObjectToFile(new KVEntry(key, value), LOG_FILE_PATH);
        //0.先看能不能插入buffer
        if (memTable.write(key, value)) {
            return;
        }
        //todo: 应该放在后台线程，而不用阻塞put操作
        mergeOps();
        assert memTable.write(key, value);
    }

    private void clearRedoLog() {
        File file = new File(LOG_FILE_PATH);
        try {
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write("");
            fileWriter.flush();
        } catch (Exception e) {
            logger.error("删除redoLog文件失败！");
        }
    }


    private void mergeOps() {
        //1.如果不能插入buffer，说明buffer已满，则看能不能将buffer刷到level 0上，先看需不需要进行归并操作
        mergeDown(0);
        //2.buffer刷到level 0上，如果当前sstable满了，则创建一个新的
        synchronized (this) {
            if (levels.get(0).getRuns().size() == 0
                    || levels.get(0).getRuns().getFirst().getSize() >= levels.get(0).getRuns().getFirst().getMaxSize()) {
                int idx = levels.get(0).getRuns().size();
                levels.get(0).getRuns().addFirst(new SSTable(
                        levels.get(0).getMaxRunSize(),
                        BF_BITS_PER_ENTRY,
                        levels.get(0).getRuns().size(),
                        0,
                        manifestInfo.getMaxKeyInfos().get(idx),
                        manifestInfo.getFencePointerInfos().get(idx),
                        manifestInfo.getBloomFilterInfos().get(idx)));
            }
            final Map<byte[], byte[]> imutableMap = memTable.getEntries();
            for (Map.Entry<byte[], byte[]> entry : imutableMap.entrySet()) {  //这个输出就是按顺序的
                levels.get(0).getRuns().getFirst().write(entry.getKey(), entry.getValue());
            }
            //3.清空buffer，并重新插入
            memTable.clear();
        }
        //如果write成功了应该把此时的log删掉，但是万一此时正在写log呢？所以应该加锁
        rwLock.writeLock().lock();
        clearRedoLog();
        rwLock.writeLock().unlock();
    }

    public byte[] read(final byte[] key) {
        byte[] latestVal;
        //0.先在buffer中找
        if ((latestVal = memTable.read(key)) != null)
            return latestVal;
        //1.buffer中不存在，则在runs中查找，todo:这里可以多线程并发地找
        for (Level level : levels) {
            for (SSTable ssTable : level.getRuns()) {
                latestVal = ssTable.read(key);
                if (latestVal != null)
                    return latestVal;
            }
        }
        /*AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger latestRun = new AtomicInteger(-1);
        ReentrantLock lock = new ReentrantLock();
        try {
            pool.execute(() -> {
                int currentRun = counter.getAndIncrement();
                Run run = getRun(currentRun);
                byte[] currentVal = run.read(key);
                if (currentVal != null);

            });
            pool.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("在runs中查找key出错", e);
        }*/
        return latestVal;
    }

    /**
     * 自上而下进行merge操作
     */
    private void mergeDown(int currentLevel) {
        int nextLevel;
        if (levels.get(currentLevel).getRemaining() > 0) {
            return;
        } else if (currentLevel >= levels.size()) {
            logger.info("已经到达最后一层，没有多余的空间了");
            return;
        } else {
            nextLevel = currentLevel + 1;
        }
        //如果下一层的还有没有剩余空间，那么还要递归下一层
        if (levels.get(nextLevel).getRemaining() == 0) {
            mergeDown(nextLevel);
            assert levels.get(nextLevel).getRemaining() > 0;
        }
        //找到下一层是空闲的，则在把当前层所有的SSTable都归并，并放入下一层的第一个SSTable，然后清空当前层
        MergeOps mergeOps = new MergeOps();
        RandomAccessFile file = null;
        byte[] mapping = null;
        try {
            for (SSTable table : levels.get(currentLevel).getRuns()) {
                rwLock.readLock().lock();
                file = new RandomAccessFile(table.getTableFilePath(), "rw");
                mapping = new byte[(int) table.getMaxSize() * ENTRY_BYTE_SIZE];
                MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY,
                        0, table.getMaxSize() * LSMTree.ENTRY_BYTE_SIZE);
                buffer.get(mapping);
                mergeOps.add(mapping, table.getSize());
                rwLock.readLock().unlock();
            }
            int idx = nextLevel * TREE_FANOUT + levels.get(nextLevel).getRuns().size();
            levels.get(nextLevel).getWriteLock().lock();
            levels.get(nextLevel).getRuns().addFirst(new SSTable(
                    levels.get(nextLevel).getMaxRunSize(),
                    BF_BITS_PER_ENTRY,
                    levels.get(nextLevel).getRuns().size(),
                    nextLevel,
                    manifestInfo.getMaxKeyInfos().get(idx),
                    manifestInfo.getFencePointerInfos().get(idx),
                    manifestInfo.getBloomFilterInfos().get(idx)));
            levels.get(nextLevel).getWriteLock().unlock();
            while (!mergeOps.isDone()) {
                KVEntry entry = mergeOps.next();
                levels.get(nextLevel).getRuns().getFirst().write(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error("内存映射文件错误" + e);
        } finally {
            FileHelper.closeFile(file);
        }
        levels.get(currentLevel).getRuns().clear();
    }

    /**
     * 关闭系统
     * 0.将内存的Memtable中的信息刷到磁盘
     * 1.将每个level的信息都保存到manifest文件中
     */
    public void close() {
        logger.info("关闭系统");
        if (!memTable.isEmpty()) {
            mergeOps();
        }
        for (Level level : levels) {
            for (SSTable table : level.getRuns()) {
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
        }
    }

}
