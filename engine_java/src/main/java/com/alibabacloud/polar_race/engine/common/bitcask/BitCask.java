package com.alibabacloud.polar_race.engine.common.bitcask;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.FileHelper;
import com.alibabacloud.polar_race.engine.common.utils.Serialization;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;



/**
 * Bitcask模型是一种日志型键值模型。
 * 所谓日志型，是指它不直接支持随机写入，而是像日志一样支持追加操作。Bitcask模型将随机写入转化为顺序写入
 * 核心思想就是维护索引文件和数据文件
 */

public class BitCask<T> {

    private Logger logger = Logger.getLogger(BitCask.class);

    private final String dbName;  //数据库名
    private static final long DEFAULT_ACTIVE_FILE_SIZE = 4096;  //活跃文件最大大小
    private static final long DEFAULT_INDEX_FILE_SIZE = 4096;
    private static final String INDEX_DIR = "/index";
    private static final String DATA_DIR = "/data";
    private static final String HINT_FILE_NAME = "index_file";  //索引文件

    private long activeFileId;  //活跃文件
    private long activeFileOffset;  //记录活跃文件的当前写入位置
    private Map<String, BitCaskIndex> _indexer;  //索引哈希表


    public BitCask(String dbName) throws EngineException{
        this.dbName = dbName;
        this._indexer = new HashMap<String, BitCaskIndex>();
        Init();
    }

    /**
     * 初始化工作
     * 0.将索引文件加载进内存
     * 1.找到当前目录下ID最大的文件，即当前活跃文件
     */
    private void Init() throws EngineException {
        if (!FileHelper.fileExists(dbName)) {
            FileHelper.createDir(dbName);
        }
        if (!FileHelper.fileExists(dbName + INDEX_DIR)) {
            FileHelper.createDir(dbName + INDEX_DIR);
        }

        String hintFilePath = dbName + INDEX_DIR + "/" + HINT_FILE_NAME;
        if (!FileHelper.fileExists(hintFilePath)) {
            try {
                FileHelper.createFile(hintFilePath);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        loadIndex(hintFilePath);
        if (!FileHelper.fileExists(dbName + DATA_DIR)) {
            FileHelper.createDir(dbName + DATA_DIR);
        } else {
            activeFileId = FileHelper.findActiveFileId(dbName + DATA_DIR);
            logger.info("当前活跃文件id：" + activeFileId);
        }
    }

    /**
     * 读索引文件byte，将索引加载到内存
     * @param filePath
     */
    private void loadIndex(String filePath) throws EngineException {
        File file = new File(filePath);
        InputStreamReader reader = null;
        BufferedReader br = null;
        try {
            reader = new InputStreamReader(new FileInputStream(file));
            br = new BufferedReader(reader);
            String line;
            try {
                while ((line = br.readLine()) != null) {
                    BitCaskIndex index = (BitCaskIndex) (Serialization.deserializeFromStr(line));
                    _indexer.put(index.getKey(), index);
                }
            } catch (Exception e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "初始化读取索引数据文件出错!" + e);
            }
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "初始化索引文件不存在!");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    reader.close();
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.IO_ERROR, "初始化关闭文件出错!");
                }
            }
        }
    }

    /**
     * 取数据
     * @param key
     * @return
     */
    public T get(String key) throws EngineException {
        T value = null;
        if (!_indexer.containsKey(key) || !_indexer.get(key).isValid())
            throw new EngineException(RetCodeEnum.INVALID_ARGUMENT, "要查找的key不存在!");
        value = readFromFile(_indexer.get(key));
        if (value == null)
            throw new EngineException(RetCodeEnum.IO_ERROR, "查找key出错!");
        return value;
    }

    /**
     * 存数据
     * 写入的记录直接追加到活动文件，因此活动文件会越来越大，当到达一定大小时，Bitcask会冻结活动文件，新建一个活动文件用于写入，
     * 而之前的活动文件则变为了older data file。写入记录的同时还要在索引哈希表中添加索引记录。
     * @param key
     * @param value
     * @return
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
        writeToFile(key, bytes, dbName + DATA_DIR);
        updateIndex(key, bytes);
        return 0;
    }

    /**
     * Bitcask不直接删除记录，而是新增一条相同key的记录，把value值设置一个删除的标记。
     * 原有记录依然存在于数据文件中，然后更新索引哈希表。
     * @param key
     * @return
     */
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
     * @param index
     * @return
     * @throws EngineException
     */
    private T readFromFile(BitCaskIndex index) throws EngineException {
        byte[] bytes = new byte[index.getValueSize()];
        RandomAccessFile file = null;
        String filePath = dbName + DATA_DIR + "/" + String.valueOf(index.getFileId()) + ".data";
        try {
            file = new RandomAccessFile(filePath, "rw");
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "物理数据文件不存在!");
        }
        try {
            file.seek(index.getValueOffset());
            file.read(bytes);
            logger.info("从数据文件" + activeFileId + "读出长度为bytes.length=" + bytes.length + "的数据");
        } catch (IOException e) {
            bytes = null;
            throw new EngineException(RetCodeEnum.IO_ERROR, "读取物理数据文件出错!");
        } finally {
            try {
                file.close();
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "关闭文件出错!");
            }
        }
        return (T) bytes;
    }

    /**
     * 将数据写入物理文件
     * @param key
     * @param bytes
     */
    private void writeToFile(String key, byte[] bytes, String dir) throws EngineException {
        RandomAccessFile file = null;
        String filePath = dir + "/" + String.valueOf(activeFileId) + ".data";
        try {
            file = new RandomAccessFile(filePath, "rw");
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "物理数据文件不存在!");
        }
        //如果当前活跃文件大小超过限制，则创建新的活跃文件；现在的活跃文件变为old
        long curFileLen;
        try {
            curFileLen = file.length();
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
            writeToFile(key, bytes, dir);
        } else {
            try {
                long offset = file.length();
                file.seek(offset);
                file.write(bytes);
                this.activeFileOffset = offset;
                logger.info("向数据文件" +  activeFileId + "写入长度为bytes.length=" + bytes.length + "的数据");
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "写物理数据文件出错!");
            } finally {
                try {
                    file.close();
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.IO_ERROR, "关闭文件出错!");
                }
            }
        }
    }

    private void updateIndex(String key, byte[] bytes) throws EngineException {
        BitCaskIndex index = new BitCaskIndex(key, this.activeFileId,
                bytes.length, this.activeFileOffset, new Date().getTime(), true);
        this._indexer.put(key, index);
        //追加到索引文件末尾
        String filePath = dbName + INDEX_DIR + "/" + HINT_FILE_NAME;
        BufferedWriter bw = null;
        File file = new File(filePath);
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
            String data = Serialization.serializeToStr(index) + "\n";
            bw.write(data);
        } catch (Exception e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "写索引文件出错!");
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.IO_ERROR, "关闭文件出错!");
                }
            }
        }
    }

    public void close() {

    }

    /**
     * 定时合并文件操作
     */
    private void Marge() {

    }
}
