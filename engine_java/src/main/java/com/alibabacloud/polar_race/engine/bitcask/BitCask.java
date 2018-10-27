package com.alibabacloud.polar_race.engine.bitcask;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.wal.RedoLog;
import com.alibabacloud.polar_race.engine.utils.FileHelper;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Bitcask模型是一种日志型键值模型。
 * 所谓日志型，是指它不直接支持随机写入，而是像日志一样支持追加操作。Bitcask模型将随机写入转化为顺序写入
 * 核心思想就是维护索引文件和数据文件
 *
 * todo: 对文件行加锁，而不是对整个文件加锁
 *
 */

public class BitCask {

    private Logger logger = Logger.getLogger(BitCask.class);

    private final String dbName;  //数据库名
    private static final long DEFAULT_ACTIVE_FILE_SIZE = 1024 * 1024 * 128;  //活跃文件最大大小
    private static final long DEFAULT_INDEX_FILE_SIZE = 4096;
    private static final String INDEX_DIR = "/index";
    private static final String DATA_DIR = "/data";
    private static final String LOG_DIR = "/log";
    private static final String HINT_FILE_NAME = "/index_file";  //索引文件
    private static final String LOG_FILE_PATH = "/redoLog_file";
    private static final boolean DEBUG_ENABLE = false;

    private AtomicLong activeFileId = new AtomicLong(0);  //活跃文件
    private AtomicLong activeFileOffset = new AtomicLong(0);  //记录活跃文件的当前写入位置
    private Map<KeyWapper, BitCaskIndex> _indexer;  //索引哈希表
    private Map<byte[], List<RedoLog>> redoLogMap;  //重做日志表，用于意外掉电时恢复

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();  //读写锁

    public BitCask(String dbName) throws EngineException {
        this.dbName = dbName;
        this._indexer = new ConcurrentHashMap<>();
        this.redoLogMap = new ConcurrentHashMap<>();
        //timer = new Timer();
        Init();
    }

    /**
     * 初始化工作
     * 0.将索引文件加载进内存
     * 1.找到当前目录下ID最大的文件，即当前活跃文件
     * 3.检查redoLog
     */
    private void Init() throws EngineException {
        if (!FileHelper.fileExists(dbName)) {
            FileHelper.createDir(dbName);
        }
        if (!FileHelper.fileExists(dbName + INDEX_DIR)) {
            FileHelper.createDir(dbName + INDEX_DIR);
        }
        String hintFilePath = dbName + INDEX_DIR + HINT_FILE_NAME;
        if (!FileHelper.fileExists(hintFilePath)) {
            try {
                FileHelper.createFile(hintFilePath);
            } catch (Exception e) {
                logger.error(e);
            }
        } else
            loadIndex(hintFilePath);
        if (!FileHelper.fileExists(dbName + DATA_DIR)) {
            FileHelper.createDir(dbName + DATA_DIR);
        } else {
            activeFileId.set(FileHelper.findActiveFileId(dbName + DATA_DIR));
            logger.info("当前活跃文件id：" + activeFileId.get());
        }
        final String redoLogPath = dbName + LOG_DIR + LOG_FILE_PATH;
        if (!FileHelper.fileExists(dbName + LOG_DIR)) {
            try {
                FileHelper.createDir(dbName + LOG_DIR);
                FileHelper.createFile(redoLogPath);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        checkRedoLog(redoLogPath);
    }

    private void loadIndex(String filePath) throws EngineException {
        List<Object> indexes = FileHelper.readObjectFromFile(filePath);
        if (indexes == null) return;
        for (Object obj : indexes) {
            if (obj == null) continue;
            BitCaskIndex index = (BitCaskIndex) obj;
            _indexer.put(new KeyWapper(index.getKey()), index);
            logger.debug("加载索引：index=" + index.toString());
        }
    }

    /**
     * 系统意外停止恢复数据
     * 将同一个key的操作，存入一个map中,且对于同一个key的操作，只找它最近的那个最新值
     */
    private void checkRedoLog(String filePath) throws EngineException {
        File file = new File(filePath);
        if (file.length() == 0) return;
        List<Object> logs = FileHelper.readObjectFromFile(filePath);
        for (Object log : logs) {
            byte[] key = ((RedoLog) log).getKey();
            if (!redoLogMap.containsKey(key)) {
                redoLogMap.put(key, new ArrayList<>());
            }
            redoLogMap.get(key).add((RedoLog) log );
        }
        for (byte[] key : redoLogMap.keySet()) {
            Collections.sort(redoLogMap.get(key), (a, b) -> (int) (b.getTimestamp() - a.getTimestamp()));
        }
        int cnt = 0;
        for (byte[] key : redoLogMap.keySet()) {
            RedoLog lastLog = redoLogMap.get(key).get(0);
            if (lastLog.isCommit()) continue;
            put(lastLog.getKey(), lastLog.getNewValue());
            cnt++;
            logger.info("执行数据恢复redoLog=" + lastLog.toString());
        }
        logger.info("要恢复的数据有" + cnt + "个");
    }

    private void clearAllRedoLog(String logPath) {
        File file = new File(logPath);
        if (!FileHelper.fileExists(logPath)) return;
        try {
            if (! file.delete())
                logger.error("删除redoLog文件失败！");
        } catch (Exception e) {
            logger.error("删除redoLog文件失败！");
        }
    }

    public byte[] get(byte[] key) throws EngineException {
        if (!_indexer.containsKey(new KeyWapper(key)) || !_indexer.get(new KeyWapper(key)).isValid())
            return null;
        return readBytesFromFile(_indexer.get(new KeyWapper(key)));
    }

    /**
     */
    public int put(byte[] key, byte[] value) throws EngineException {
        RedoLog curLog = new RedoLog();
        curLog.setBegin(true);
        curLog.setKey(key);
        curLog.setNewValue(value);
        curLog.setTimestamp(new Date().getTime());
        writeToRedoLog(curLog);

        checkActiveFile(dbName + DATA_DIR);
        writeBytesToFile(key, value, dbName + DATA_DIR);

        curLog.setCommit(true);
        curLog.setTimestamp(new Date().getTime());
        writeToRedoLog(curLog);
        return 0;
    }


    /**
     * 根据索引从物理数据文件中读取数据
     */
    private byte[] readBytesFromFile(BitCaskIndex index) throws EngineException {
        byte[] bytes = new byte[index.getValueSize()];
        RandomAccessFile file = null;
        String filePath = dbName + DATA_DIR + "/" + String.valueOf(index.getFileId()) + ".data";
        try {
            file = new RandomAccessFile(filePath, "rw");
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "物理数据文件不存在!");
        }
        rwLock.readLock().lock();
        try {
            file.seek(index.getValueOffset());
            file.read(bytes);
            if (DEBUG_ENABLE) {
                logger.info("从数据文件" + activeFileId.get() + "读出为value=" + new String(bytes) + "的数据");
            }
        } catch (IOException e) {
            bytes = null;
            throw new EngineException(RetCodeEnum.IO_ERROR, "读取物理数据文件出错!");
        } finally {
            try {
                file.close();
                rwLock.readLock().unlock();
            } catch (IOException e) {
                logger.error("关闭文件出错!");
            }
        }
        return bytes;
    }

    private void writeToRedoLog(RedoLog redoLog) {
        String redoLogPath = dbName + LOG_DIR + LOG_FILE_PATH;
        try {
            FileHelper.writeObjectToFile(redoLog, redoLogPath);
            if (DEBUG_ENABLE) {
                logger.info("操作写入redoLog: RedoLog=" + redoLog.toString());
            }
        } catch (Exception e) {
            logger.error("写入redoLog出错!", e);
        }
    }

    private void checkActiveFile(String dir) throws EngineException {
        String filePath = dir + "/" + String.valueOf(activeFileId.get()) + ".data";
        File file = new File(filePath);
        if (file.length() >= DEFAULT_ACTIVE_FILE_SIZE) {
            if (DEBUG_ENABLE) {
                logger.info("活跃文件大小超过限制，创建新的活跃文件");
            }
            activeFileId.incrementAndGet();
            String newFilePath = dir + "/" + String.valueOf(activeFileId.get()) + ".data";
            if (!FileHelper.fileExists(filePath)) {
                try {
                    FileHelper.createFile(newFilePath);
                } catch (Exception e) {
                    throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "创建新物理数据文件失败!");
                }
            }
        }
    }

    /**
     * 将数据写入物理文件
     */
    private void writeBytesToFile(byte[] key, byte[] bytes, String dir) throws EngineException {
        RandomAccessFile file = null;
        String filePath = dir + "/" + String.valueOf(activeFileId.get()) + ".data";
        rwLock.writeLock().lock();
        try {
            file = new RandomAccessFile(filePath, "rw");
            long offset = file.length();
            activeFileOffset.compareAndSet(activeFileOffset.get(), offset);
            file.seek(offset);
            file.write(bytes);
            if (DEBUG_ENABLE) {
                logger.info("向数据文件" + activeFileId.get() + " , offset=" + activeFileOffset.get()
                        + " 写入key=" + new String(key) + ", value=" + new String(bytes) + "的数据");
            }
            updateIndex(key, bytes);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "写物理数据文件出错!");
        } finally {
            try {
                if (file != null) {
                    file.close();
                    rwLock.writeLock().unlock();
                }
            } catch (IOException e) {
                logger.error("关闭文件出错!");
            }
        }

    }

    private void updateIndex(byte[] key, byte[] bytes) throws EngineException {
        BitCaskIndex index = new BitCaskIndex(key, this.activeFileId.get(),
                bytes.length, this.activeFileOffset.get(), new Date().getTime(), true);
        this._indexer.put(new KeyWapper(key), index);
        String filePath = dbName + INDEX_DIR + HINT_FILE_NAME;
        FileHelper.writeObjectToFile(index, filePath);
        if (DEBUG_ENABLE) {
            logger.info("写入索引文件：index=" + index.toString());
        }
    }


    public void close() {
        //如果是正常关闭，那么就清空所有redoLog
        String redoLogPath = dbName + LOG_DIR + LOG_FILE_PATH;
        clearAllRedoLog(redoLogPath);
        logger.info("成功关闭系统，清空redoLog文件");
    }

    /**
     * 定时合并文件操作
     */
    private void Merge() {

    }
}
