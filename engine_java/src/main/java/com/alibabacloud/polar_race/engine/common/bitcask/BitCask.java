package com.alibabacloud.polar_race.engine.common.bitcask;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.NewObjectOutputStream;
import com.alibabacloud.polar_race.engine.common.wal.RedoLog;
import com.alibabacloud.polar_race.engine.common.utils.FileHelper;
import com.alibabacloud.polar_race.engine.common.utils.Serialization;
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
 */

public class BitCask<T> {

    private Logger logger = Logger.getLogger(BitCask.class);

    private final String dbName;  //数据库名
    private static final long DEFAULT_ACTIVE_FILE_SIZE = 4096;  //活跃文件最大大小
    private static final long DEFAULT_INDEX_FILE_SIZE = 4096;
    private static final String INDEX_DIR = "/index";
    private static final String DATA_DIR = "/data";
    private static final String LOG_DIR = "/log";
    private static final String HINT_FILE_NAME = "/index_file";  //索引文件
    private static final String LOG_FILE_PATH = "/redoLog_file";
    private static final long PERIOD = 1000;  //定时将数据刷到磁盘的频率

    private long activeFileId;  //活跃文件
    private long activeFileOffset;  //记录活跃文件的当前写入位置
    private Map<String, BitCaskIndex> _indexer;  //索引哈希表

    private Map<Long, List<RedoLog>> redoLogMap;  //重做日志表，用于意外掉电时恢复

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();  //读写锁
    private Timer timer;
    private static final AtomicLong transactionId = new AtomicLong(0);  //事务id


    public BitCask(String dbName) throws EngineException {
        this.dbName = dbName;
        this._indexer = new ConcurrentHashMap<>();
        this.redoLogMap = new ConcurrentHashMap<>();
        timer = new Timer();
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
            activeFileId = FileHelper.findActiveFileId(dbName + DATA_DIR);
            logger.info("当前活跃文件id：" + activeFileId);
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

        /*timer.schedule(new TimerTask() {
            @Override
            public void run() {
                writeToDisk(redoLogPath);
            }
        }, PERIOD, PERIOD);*/
    }

    private void loadIndex(String filePath) throws EngineException {
        List<Object> indexes = readObjectFromFile(filePath);
        if (indexes == null) return;
        for (Object index : indexes) {
            if (index == null) continue;
            _indexer.put(((BitCaskIndex)index).getKey(), (BitCaskIndex)index);
        }
    }

    /**
     * 系统意外停止恢复数据
     * 将同一个transactionId的操作，存入一个map中
     */
    private void checkRedoLog(String filePath) throws EngineException {
        File file = new File(filePath);
        if (file.length() == 0) return;
        List<Object> logs = readObjectFromFile(filePath);
        for (Object log : logs) {
            long Id = ((RedoLog) log).getTransactionId();
            if (!redoLogMap.containsKey(Id)) {
                redoLogMap.put(Id, new ArrayList<>());
            }
            redoLogMap.get(Id).add((RedoLog) log);
        }
        for (long Id : redoLogMap.keySet()) {
            Collections.sort(redoLogMap.get(Id), (a, b) -> (int) (b.getTimestamp() - a.getTimestamp()));
        }
        for (long Id : redoLogMap.keySet()) {
            boolean isCommit = false;
            RedoLog lastLog = redoLogMap.get(Id).get(0);
            put(lastLog.getKey(), (T) lastLog.getNewValue());
        }
    }

    /*private void writeToDisk(String logPath) {
        String redoLogPath = dbName + LOG_DIR + LOG_FILE_PATH;
        List<Object> logs = readObjectFromFile(redoLogPath);
        String dataFileDir = dbName + DATA_DIR;
        for (Object log : logs) {
            String key = ((RedoLog) log).getKey();
            byte[] value = (byte[]) (((RedoLog) log).getNewValue());
            try {
                writeBytesToFile(key, value, dataFileDir);
                ((RedoLog) log).setCommit(true);
            } catch (Exception e) {
                logger.error("异步写物理文件出错" + e);
            }
        }
        clearRedoLog(logPath);
    }*/

    private void clearRedoLog(RedoLog redoLog) {

    }

    private void clearAllRedoLog(String logPath) {
        File file = new File(logPath);
        try {
            if (! file.delete())
                logger.error("删除redoLog文件失败！");
        } catch (Exception e) {
            logger.error("删除redoLog文件失败！");
        }
    }

    public T get(String key) throws EngineException {
        if (!_indexer.containsKey(key) || !_indexer.get(key).isValid())
            return null;
        return  readBytesFromFile(_indexer.get(key));
    }

    /**
     * todo: put操作写log文件写了几次啊，之后得优化
     */
    public int put(String key, T value) throws EngineException {
        byte[] bytes = null;
        try {
            if (value instanceof byte[]) {
                bytes = (byte[]) value;
            } else {
                bytes = Serialization.serialize(value);
            }
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "序列化出错!");
        }
        RedoLog<byte[]> curLog = new RedoLog<>();
        curLog.setTransactionId(transactionId.getAndIncrement());
        curLog.setBegin(true);
        curLog.setKey(key);
        curLog.setOldValue((byte[]) get(key));
        curLog.setNewValue((byte[]) value);
        curLog.setTimestamp(new Date().getTime());
        writeToRedoLog(curLog);

        updateIndex(key, bytes, curLog);
        writeBytesToFile(key, bytes, dbName + DATA_DIR);

        curLog.setCommit(true);
        curLog.setTimestamp(new Date().getTime());
        writeToRedoLog(curLog);
        return 0;
    }

    public int delete(String key) throws EngineException {
        if (!_indexer.containsKey(key))
            throw new EngineException(RetCodeEnum.INVALID_ARGUMENT, "要查找的key不存在!");
        BitCaskIndex index = _indexer.get(key);
        if (!index.isValid())
            throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "该key已经被删除!");
        index.setValid(false);
        index.setTimestamp(new Date().getTime());
        return 0;
    }

    /**
     * 根据索引从物理数据文件中读取数据
     */
    private T readBytesFromFile(BitCaskIndex index) throws EngineException {
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
            logger.info("从数据文件" + activeFileId + "读出长度为value=" + new String(bytes) + "的数据");
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
        return (T) bytes;
    }

    private void writeToRedoLog(RedoLog redoLog) {
        String redoLogPath = dbName + LOG_DIR + LOG_FILE_PATH;
        try {
            writeObjectToFile(redoLog, redoLogPath);
            logger.info("操作写入redoLog: RedoLog=" + redoLog.toString());
        } catch (Exception e) {
            logger.error("写入redoLog出错!");
        }
    }

    /**
     * 将数据写入物理文件
     */
    private void writeBytesToFile(String key, byte[] bytes, String dir) throws EngineException {
        RandomAccessFile file = null;
        String filePath = dir + "/" + String.valueOf(activeFileId) + ".data";
        if (!FileHelper.fileExists(filePath)) {
            try {
                FileHelper.createFile(filePath);
            } catch (Exception e) {
                throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "创建新物理数据文件失败!");
            }
        }
        try {
            file = new RandomAccessFile(filePath, "rw");
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "物理数据文件不存在!");
        }
        //如果当前活跃文件大小超过限制，则创建新的活跃文件；现在的活跃文件变为old
        long curFileLen;
        try {
            curFileLen = file.length();
            logger.info("当前活跃文件长度：" + curFileLen);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "读物理数据文件长度出错!");
        }
        if (curFileLen >= DEFAULT_ACTIVE_FILE_SIZE) {
            try {
                file.close();
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "关闭物理文件出错!");
            }
            activeFileId += 1;
            writeBytesToFile(key, bytes, dir);
        } else {
            rwLock.writeLock().lock();
            try {
                long offset = file.length();
                file.seek(offset);
                file.write(bytes);
                this.activeFileOffset = offset;
                logger.info("向数据文件" +  activeFileId + "写入key=" + key +", value=" + new String(bytes) + "的数据");
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "写物理数据文件出错!");
            } finally {
                try {
                    file.close();
                    rwLock.writeLock().unlock();
                } catch (IOException e) {
                    logger.error("关闭文件出错!");
                }
            }
        }
    }

    private void updateIndex(String key, byte[] bytes, RedoLog curLog) throws EngineException {
        BitCaskIndex index = new BitCaskIndex(key, this.activeFileId,
                bytes.length, this.activeFileOffset, new Date().getTime(), true);
        this._indexer.put(key, index);
        String filePath = dbName + INDEX_DIR + HINT_FILE_NAME;
        writeObjectToFile(index, filePath);
    }

    private List<Object> readObjectFromFile(String filePath) {
        List<Object> objects = new ArrayList<Object>();
        File file = new File(filePath);
        ObjectInputStream ois = null;
        rwLock.readLock().lock();
        try {
            ois = new ObjectInputStream(new FileInputStream(file));
            Object object = ois.readObject();
            while (object != null) {
                objects.add(object);
                try {
                    object = ois.readObject();
                } catch (EOFException e) {
                    break;
                }
            }
            logger.info("从文件读出对象流List.size()=" + objects.size());
        } catch (Exception e) {
            logger.error("读取对象输出流出错", e);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                    rwLock.readLock().unlock();
                } catch (IOException e) {
                    logger.error("关闭对象输出流出错", e);
                }
            }
        }
        return objects;
    }

    private void writeObjectToFile(Object obj, String filePath) {
        ObjectOutputStream oos = null;
        rwLock.writeLock().lock();
        try {
            oos = NewObjectOutputStream.newInstance(filePath);
            oos.writeObject(obj);
            oos.flush();
            logger.info("向文件写入对象流");
        } catch (Exception e) {
            logger.error("将对象序列化进文件出错", e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                    rwLock.writeLock().unlock();
                } catch (IOException e) {
                    logger.error("关闭对象输入流出错", e);
                }
            }
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
    private void Marge() {

    }
}
