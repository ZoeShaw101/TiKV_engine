package com.alibabacloud.polar_race.engine.core;

import com.alibabacloud.polar_race.engine.core.merge.Level0Merger;
import com.alibabacloud.polar_race.engine.core.merge.Level1Merger;
import com.alibabacloud.polar_race.engine.core.stats.DBStats;
import com.alibabacloud.polar_race.engine.core.stats.FileStatsCollector;
import com.alibabacloud.polar_race.engine.core.stats.Operations;
import com.alibabacloud.polar_race.engine.core.table.*;
import com.alibabacloud.polar_race.engine.core.utils.DateFormatter;
import com.alibabacloud.polar_race.engine.core.utils.FileUtil;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import com.alibabacloud.polar_race.engine.utils.FileHelper;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WiscKey : key / value 分离存储，因为LSM只要保证key有序就行了, value进行批量写入，减小SSD写放大
 * */


public class LSMDB {

    static final Logger logger = LoggerFactory.getLogger(LSMDB.class);

    public static final int INMEM_LEVEL = -1;
    public static final int LEVEL0 = 0;
    public static final int LEVEL1 = 1;
    public static final int LEVEL2 = 2;
    public static final int MAX_LEVEL = 2;

    private static final String VALUE_LOG = "/value.log";
    private static final String VALUE_TMP_LOG = "/value.tmp.log";
    private static final int VALUE_NUM_THRESHOLD = 10000;
    static final int KEY_BYTE_SIZE = 8;  //8B
    private static final int VALUE_BYTE_SIZE = 4096;   //4KB
    static final int ENTRY_BYTE_SIZE = 4104;

    private volatile HashMapTable[] activeInMemTables;
    private Object[] activeInMemTableCreationLocks;
    private List<LevelQueue>[] levelQueueLists;
    private final DBStats stats = new DBStats();

    private String dir;
    private DBConfig config;
    private Level0Merger[] level0Mergers;
    private Level1Merger[] level1Mergers;
    private CountDownLatch[] countDownLatches;
    private FileStatsCollector fileStatsCollector;

    private FileChannel tmpValueFileChannel;
    private FileChannel valueFileChannel;

    private ByteBuffer tmpValueLogBuf = ByteBuffer.allocateDirect(VALUE_NUM_THRESHOLD * ENTRY_BYTE_SIZE);
    private ThreadLocalByteBuffer valueBuf = new ThreadLocalByteBuffer(ByteBuffer.allocate(VALUE_BYTE_SIZE));

    private boolean closed = false;
    private AtomicBoolean isFirstGet = new AtomicBoolean(true);
    public static final boolean DEBUG_ENABLE = false;

    private AtomicInteger putCounter = new AtomicInteger(0);
    private ReentrantLock logLock = new ReentrantLock();
    public AtomicLong valueAddress = new AtomicLong(0);  //offset
    public AtomicLong tmpValueAddress = new AtomicLong(0);  //offset

    public LSMDB(String dir) {
        this(dir, new DBConfig());
    }

    @SuppressWarnings("unchecked")
    public LSMDB(String dir, DBConfig config) {
        logger.info("初始化DB配置...，现在内存大小：" + Runtime.getRuntime().freeMemory() + "，时间：" + DateFormatter.formatCurrentDate());
        this.dir = dir;
        this.config = config;

        activeInMemTables = new HashMapTable[config.getShardNumber()];

        activeInMemTableCreationLocks = new Object[config.getShardNumber()];
        for(int i = 0; i < config.getShardNumber(); i++) {
            activeInMemTableCreationLocks[i] = new Object();
        }

        // initialize level queue list
        levelQueueLists = new ArrayList[config.getShardNumber()];
        for(int i = 0; i < config.getShardNumber(); i++) {
            levelQueueLists[i] = new ArrayList<LevelQueue>(MAX_LEVEL + 1);
            for(int j = 0; j <= MAX_LEVEL; j++) {
                levelQueueLists[i].add(new LevelQueue());
            }
        }

        try {
            this.loadMapTables();
        } catch (Exception ex) {
            throw new RuntimeException("Fail to load on disk map tables!", ex);
        }

        this.fileStatsCollector = new FileStatsCollector(stats, levelQueueLists);
        //this.fileStatsCollector.start();

        this.startLevelMergers();

        if (!FileHelper.fileExists(this.dir + VALUE_LOG)) {
            try {
                FileHelper.createFile(this.dir + VALUE_LOG);
            } catch (Exception e) {
                logger.error("创建value log文件失败" + e);
            }
        }
        if (!FileHelper.fileExists(this.dir + VALUE_TMP_LOG)) {
            try {
                FileHelper.createFile(this.dir + VALUE_TMP_LOG);
            } catch (Exception e) {
                logger.error("创建tmp value log文件失败" + e);
            }
        }

        String tmpValuePath = this.dir + VALUE_TMP_LOG;
        String  valuePath = this.dir + VALUE_LOG;
        try {
            this.tmpValueFileChannel = new RandomAccessFile(tmpValuePath, "rw").getChannel();
            this.valueFileChannel = new RandomAccessFile(valuePath, "rw").getChannel();
            this.valueAddress.set(this.valueFileChannel.size());
            logger.info("当前value log size=" + this.valueFileChannel.size());
        } catch (IOException e) {
            logger.info("初始化File Channel出错！" + e);
        }

        //this.checkValueLog();  //是否有数据需要恢复
    }

    private void startLevelMergers() {
        countDownLatches = new CountDownLatch[this.config.getShardNumber()];
        for(int i = 0; i < this.config.getShardNumber(); i++) {
            countDownLatches[i] = new CountDownLatch(2);
        }
        level0Mergers = new Level0Merger[config.getShardNumber()];
        level1Mergers = new Level1Merger[config.getShardNumber()];

        for(short i = 0; i < this.config.getShardNumber(); i++) {
            level0Mergers[i] = new Level0Merger(this, this.levelQueueLists[i], countDownLatches[i], i, stats);
            level0Mergers[i].start();
            level1Mergers[i] = new Level1Merger(this, this.levelQueueLists[i], countDownLatches[i], i, stats);
            level1Mergers[i].start();
        }
    }

    private void loadMapTables() throws IOException, ClassNotFoundException {
        logger.info("加载磁盘的table信息...");
        File dirFile = new File(dir);
        if (!dirFile.exists())  {
            dirFile.mkdirs();
        }
        String fileNames[] = dirFile.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String filename) {
                if (filename.endsWith(AbstractMapTable.INDEX_FILE_SUFFIX)) return true;
                return false;
            }

        });

        // new DB, setup new active map table
        if (fileNames == null || fileNames.length == 0) {
            for(short i = 0; i < this.config.getShardNumber(); i++) {
                this.activeInMemTables[i] = new HashMapTable(dir, i, LEVEL0, System.nanoTime());
                this.activeInMemTables[i].markUsable(true);
                this.activeInMemTables[i].markImmutable(false); // mutable
                this.activeInMemTables[i].setCompressionEnabled(this.config.isCompressionEnabled());
            }
            logger.info("新建引擎！不需要加载table信息");
            return;
        }

        PriorityQueue<AbstractMapTable> pq = new PriorityQueue<AbstractMapTable>();
        for(String fileName : fileNames) {
            int dotIndex = fileName.lastIndexOf(".");
            if (dotIndex > 0) {
                fileName = fileName.substring(0, dotIndex);
            }
            String[] parts = fileName.split("-");
            Preconditions.checkArgument(parts != null && parts.length == 3, "on-disk table file names corrupted!");
            int level = Integer.parseInt(parts[1]);
            if (level == LEVEL0) {
                pq.add(new HashMapTable(dir, fileName));
            } else if (level == LEVEL1) {
                pq.add(new MMFMapTable(dir, fileName));
            } else {
                pq.add(new FCMapTable(dir, fileName));
            }
        }

        Preconditions.checkArgument(pq.size() > 0, "on-disk table file names corrupted!");

        // setup active map table
        for(int i = 0; i < this.config.getShardNumber(); i++) {
            AbstractMapTable table = pq.poll();
            Preconditions.checkArgument(table.getLevel() == 0, "on-disk table file names corrupted, no level 0 map tables");
            this.activeInMemTables[table.getShard()] = (HashMapTable) table;
            this.activeInMemTables[table.getShard()].markUsable(true);
            this.activeInMemTables[table.getShard()].markImmutable(false); // mutable
            this.activeInMemTables[table.getShard()].setCompressionEnabled(this.config.isCompressionEnabled());
        }

        //初始化level的table信息
        while(!pq.isEmpty()) {
            AbstractMapTable table = pq.poll();
            if (table.isUsable()) {
                int level = table.getLevel();
                LevelQueue lq = levelQueueLists[table.getShard()].get(level);
                lq.addLast(table);
            } else { // garbage
                table.close();
                table.delete();
            }
        }

        logger.info("加载磁盘的table信息完成！");
    }


    public void put(byte[] key, byte[] value) {
        this.put(key, value, AbstractMapTable.NO_TIMEOUT, System.currentTimeMillis(), false);
    }

    public void put(byte[] key, byte[] value, long timeToLive) {
        this.put(key, value, timeToLive, System.currentTimeMillis(), false);
    }

    private short getShard(byte[] key) {
        int keyHash = Arrays.hashCode(key);
        keyHash = Math.abs(keyHash);
        return (short) (keyHash % this.config.getShardNumber());
    }

    /**
     * 恢复数据
     */
    private void checkValueLog() {
        logger.info("恢复tmp value log中的数据...");
        String tmpValuePath = this.dir + VALUE_TMP_LOG;
        String  valuePath = this.dir + VALUE_LOG;
        try {
            long size = this.tmpValueFileChannel.size();
            if (size == 0) {
                logger.info("没有数据要恢复！");
                return;
            }
            int recoveryNum = (int) size / ENTRY_BYTE_SIZE;
            logger.info("共有" + recoveryNum + "个数据需要恢复, value log size=" + this.valueFileChannel.size());

            //this.flushTmpValueLog(true);
            this.tmpValueLogBuf.clear();
            this.tmpValueFileChannel.read(this.tmpValueLogBuf, 0);
            final byte[] buf = new byte[this.tmpValueLogBuf.position()];
            this.tmpValueLogBuf.flip();
            this.tmpValueLogBuf.get(buf, 0, buf.length);
            byte[] key, value;
            FileHelper.clearFileContent(tmpValuePath);
            for (int i = 0, offset = 0; i < recoveryNum; i++, offset += ENTRY_BYTE_SIZE) {
                key = new byte[KEY_BYTE_SIZE];
                value = new byte[VALUE_BYTE_SIZE];
                System.arraycopy(buf, offset, key, 0, KEY_BYTE_SIZE);
                System.arraycopy(buf, offset + KEY_BYTE_SIZE, value, 0, VALUE_BYTE_SIZE);
                this.put(key, value);
                logger.info("恢复数据key=" + new String(key));
            }
        } catch (IOException e) {
            logger.info("恢复数据出错！" + e);
        }
    }

    private long putToValueLog(byte[] key, byte[] value) {
        String tmpValuePath = this.dir + VALUE_TMP_LOG;
        long curValueAddress = -1;
        logLock.lock();
        try {
            int index = (int) this.tmpValueAddress.get() / ENTRY_BYTE_SIZE;
            if (index  == VALUE_NUM_THRESHOLD) {
                logger.info("tmp value log address 已经达到阈值，刷到value log");
                this.tmpValueLogBuf.clear();
                this.tmpValueFileChannel.read(this.tmpValueLogBuf, 0);
                this.tmpValueLogBuf.flip();        //注意需将buffer由写模式转换成读模式
                this.valueFileChannel.write(this.tmpValueLogBuf, this.valueAddress.get());
                this.valueAddress.addAndGet(this.tmpValueAddress.get());
                this.tmpValueAddress.set(0);
                this.valueFileChannel.force(true);
                FileHelper.clearFileContent(tmpValuePath);
                logger.info("value log offset=" + this.valueAddress.get());
            }

            curValueAddress = this.valueAddress.get() + this.tmpValueAddress.get();
            this.tmpValueFileChannel.write(ByteBuffer.wrap(key), this.tmpValueAddress.get());
            this.tmpValueFileChannel.write(ByteBuffer.wrap(value), this.tmpValueAddress.get() + KEY_BYTE_SIZE);
            this.tmpValueAddress.addAndGet(ENTRY_BYTE_SIZE);
            this.tmpValueFileChannel.force(true);
        } catch (IOException e) {
            logger.error("写入value tmp log出错！" + e);
        } finally {
            logLock.unlock();
        }
        return curValueAddress;
    }


    private void put(byte[] key, byte[] value, long timeToLive, long createdTime, boolean isDelete) {
        Preconditions.checkArgument(key != null && key.length > 0, "key is empty");
        Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
        ensureNotClosed();

        long curValueAddress = -1;
        this.logLock.lock();
        try {
            curValueAddress = this.valueAddress.get();
            this.valueFileChannel.write(ByteBuffer.wrap(value), this.valueAddress.get());
            this.valueAddress.addAndGet(VALUE_BYTE_SIZE);
            this.valueFileChannel.force(true);
        } catch (IOException e) {
            logger.error("写入value log出错！" + e);
        } finally {
            this.logLock.unlock();
        }

        long start = System.nanoTime();
        String operation = isDelete ? Operations.DELETE : Operations.PUT;
        try {
            short shard = this.getShard(key);
            boolean success = this.activeInMemTables[shard].put(key, curValueAddress);  //this.valueAddress.get()不同线程得到值不一致

            if (!success) { // overflow
                synchronized(activeInMemTableCreationLocks[shard]) {
                    success = this.activeInMemTables[shard].put(key, curValueAddress); // synchronized对新生成table的对象加锁
                    if (!success) { // move to level queue 0
                        this.activeInMemTables[shard].markImmutable(true);
                        LevelQueue lq0 = this.levelQueueLists[shard].get(LEVEL0);
                        lq0.getWriteLock().lock();
                        try {
                            lq0.addFirst(this.activeInMemTables[shard]);
                            logger.info("queue0.size=" + lq0.size());
                        } finally {
                            lq0.getWriteLock().unlock();
                        }
                        @SuppressWarnings("resource")
                        HashMapTable tempTable = new HashMapTable(dir, shard, LEVEL0, System.nanoTime());
                        tempTable.markUsable(true);
                        tempTable.markImmutable(false); //mutable
                        tempTable.put(key, curValueAddress);
                        // switch on
                        this.activeInMemTables[shard] = tempTable;
                        tempTable = null;
                    }
                }
            }
            putCounter.incrementAndGet();
            if (putCounter.get() % 50000 == 0) {
                logger.info("已写入" + putCounter.get() + "个数");
            }
            if (DEBUG_ENABLE) {
                logger.info("数据写入：key=" + new String(key));
            }
        } catch(IOException ioe) {
            stats.recordDBError(operation);
            if (isDelete) {
                throw new RuntimeException("Fail to delete key, IOException occurr", ioe);
            }
            throw new RuntimeException("Fail to put key & value, IOException occurr", ioe);
        } finally {
            stats.recordDBOperation(operation, INMEM_LEVEL, System.nanoTime() - start);
        }
    }

    private byte[] getValueAddress(byte[] key) {
        long start = System.nanoTime();
        int reachedLevel = INMEM_LEVEL;
        try {
            short shard = this.getShard(key);
            // check active hashmap table first
            Result result = this.activeInMemTables[shard].get(key);
            if (result.isFound()) {
                return result.getValue();
            } else {
                // check level0 hashmap tables
                reachedLevel = LEVEL0;
                LevelQueue lq0 = levelQueueLists[shard].get(LEVEL0);
                lq0.getReadLock().lock();
                try {
                    if (lq0 != null && lq0.size() > 0) {
                        for(AbstractMapTable table : lq0) {
                            result = table.get(key);
                            if (result.isFound()) return result.getValue();
                        }
                    }
                } finally {
                    lq0.getReadLock().unlock();
                }
                // check level 1-2 on disk sorted tables
                searchLevel12: {
                    for(int level = 1; level <= MAX_LEVEL; level++) {
                        reachedLevel = level;
                        LevelQueue lq = levelQueueLists[shard].get(level);
                        lq.getReadLock().lock();
                        try {
                            if (lq.size() > 0) {
                                for(AbstractMapTable table : lq) {
                                    result = table.get(key);
                                    if (result.isFound()) return result.getValue();
                                }
                            }
                        } finally {
                            lq.getReadLock().unlock();
                        }
                    }
                }
            }
        }
        catch(IOException ioe) {
            stats.recordDBError(Operations.GET);
            throw new RuntimeException("Fail to get value by key, IOException occurr", ioe);
        } finally {
            stats.recordDBOperation(Operations.GET, reachedLevel, System.nanoTime() - start);
        }
        return null; // no luck
    }

    /**
     * get首先从table中拿到value address，然后还要根据address读一次value log，才最终拿到value值
     */
    public byte[] get(byte[] key) {
        Preconditions.checkArgument(key != null && key.length > 0, "key is empty");
        ensureNotClosed();
        final byte[] valueAddress = this.getValueAddress(key);
        if (valueAddress == null) {
            logger.info("key=" + new String(key) + "没找到value地址！");
            return null;
        }
        byte[] value = null;
        try {
            final long offset = BytesUtil.BytesToLong(valueAddress);
            logger.info("offset=" + offset);
            this.valueBuf.get().clear();
            this.valueFileChannel.read(this.valueBuf.get(), offset);
            value = this.valueBuf.get().array();
        } catch (IOException e) {
            logger.error("读取value 出错！" + e);
        }
        if (value == null) logger.info("value值为null！, key=" + new String(key));
        return value;
    }

    /**
     * 最后关闭系统时还需将tmp value log里的数刷到value log里，清空tmp value log
     */
    private void  flushTmpValueLog(boolean isRecovery) {
        String tmpValuePath = this.dir + VALUE_TMP_LOG;
        this.logLock.lock();
        try {
            long size = this.tmpValueFileChannel.size();
            if (size == 0) return;
            logger.info("将tmp value log中的数据全部刷到vlaue log中...");
            logger.info("before flush tmp, value log file size=" + this.valueFileChannel.size());
            this.valueFileChannel.transferFrom(this.tmpValueFileChannel, this.valueFileChannel.size(), this.tmpValueFileChannel.size());
            FileHelper.clearFileContent(tmpValuePath);
            logger.info("after flush tmp, value log file size=" + this.valueFileChannel.size());
        } catch (IOException e) {
            logger.info("持久化tmp vlaue log数据出错！" + e);
        } finally {
            this.logLock.unlock();
            if (!isRecovery) {
                if (this.valueFileChannel != null) try {
                    this.valueFileChannel.close();
                } catch (IOException e) {
                    logger.error("关闭value log出错！" + e);
                }
                if (this.tmpValueFileChannel != null) try {
                    this.tmpValueFileChannel.close();
                } catch (IOException e) {
                    logger.error("关闭value log出错！" + e);
                }
            }
        }
    }

    public void close() throws IOException {
        logger.info("正在关闭存储引擎..." + "时间：" + DateFormatter.formatCurrentDate());
        if (closed) return;

        //fileStatsCollector.setStop();

        for(int i = 0; i < config.getShardNumber(); i++) {
            this.activeInMemTables[i].close();
        }
        logger.info("activeInMemTable正常关闭！");

        for(int i = 0; i < config.getShardNumber(); i++) {
            this.level0Mergers[i].setStop();
            this.level1Mergers[i].setStop();
        }
        for(int i = 0; i < config.getShardNumber(); i++) {
            try {
                logger.info("Shard " + i + " waiting level 0 & 1 merge threads to exit...");
                this.countDownLatches[i].await(3000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore;
            }

        }

        for(int i = 0; i < config.getShardNumber(); i++) {
            for(int j = 0; j <= MAX_LEVEL; j++) {
                LevelQueue lq = this.levelQueueLists[i].get(j);
                for(AbstractMapTable table : lq) {
                    table.close();
                }

            }
        }

        closed = true;
        logger.info("引擎正常关闭！" + "时间：" + DateFormatter.formatCurrentDate());
    }


    protected void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("You can't work on a closed SDB.");
        }
    }

    public String getDir() {
        return this.dir;
    }
}

