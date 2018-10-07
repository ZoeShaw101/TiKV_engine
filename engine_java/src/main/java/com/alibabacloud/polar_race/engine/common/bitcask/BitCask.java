package com.alibabacloud.polar_race.engine.common.bitcask;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.FileHelper;
import com.alibabacloud.polar_race.engine.common.utils.Serialization;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bitcask模型是一种日志型键值模型。
 * 所谓日志型，是指它不直接支持随机写入，而是像日志一样支持追加操作。Bitcask模型将随机写入转化为顺序写入
 * 核心思想就是维护索引文件和数据文件
 */

public class BitCask<T> {

    private final String dbName;  //数据库名
    private static final long DEFAULT_ACTIVE_FILE_SIZE = 4096;  //活跃文件最大大小
    private static final long DEFAULT_INDEX_FILE_SIZE = 4096;
    private static final String INDEX_DIR = "/index";
    private static final String DATA_DIR = "/data";


    private long activeFileId;  //活跃文件
    private long activeFileOffset;  //记录活跃文件的当前写入位置
    private long hintFileId;  //线索文件
    private long hintFileOffset;
    private Map<String, BitCaskIndex> _indexer;  //索引哈希表


    public BitCask(String dbName) {
        this.dbName = dbName;
        this._indexer = new HashMap<String, BitCaskIndex>();
        Init();
    }

    /**
     * 初始化工作
     * 0.将索引文件加载进内存
     * 1.找到当前目录下ID最大的文件，即当前活跃文件
     */
    private void Init() {
        if (!FileHelper.fileExists(dbName)) {
            FileHelper.createDir(dbName);
        }
        if (!FileHelper.fileExists(dbName + INDEX_DIR)) {
            FileHelper.createDir(dbName + INDEX_DIR);
        }
        List<String> indexFiles = FileHelper.getDirFiles(dbName + INDEX_DIR);
        for (String file : indexFiles) {
            try {
                loadIndex(file);
            } catch (EngineException e) {
                e.printStackTrace();
            }
        }
        if (!FileHelper.fileExists(dbName + DATA_DIR)) {
            FileHelper.createDir(dbName + DATA_DIR);
        } else {
            activeFileId = FileHelper.findActiveFileId(dbName + DATA_DIR);
        }
    }

    /**
     * 读索引文件byte，将索引加载到内存
     * @param fileName
     */
    private void loadIndex(String fileName) throws EngineException{
        String filePath = dbName + INDEX_DIR + "/" + fileName;
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
                throw new EngineException(RetCodeEnum.IO_ERROR, "初始化读取索引数据文件出错!");
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
    public T get(String key) throws EngineException{
        T value = null;
        if (!_indexer.containsKey(key))
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
            bytes = Serialization.serialize(value);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "序列化出错!");
        }
        writeToFile(key, bytes, dbName + DATA_DIR);
        updateIndex(key, bytes);
        return 0;
    }

    /**
     *
     * @param key
     * @return
     */
    public int delete(String key) {
        return 0;
    }

    /**
     * 根据索引从物理数据文件中读取数据
     * @param index
     * @return
     * @throws EngineException
     */
    private T readFromFile(BitCaskIndex index) throws EngineException{
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
        Object value = null;
        try {
            value = Serialization.deserialize(bytes);
        } catch (Exception e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "反序列化出错!");
        }
        return value != null ? (T)value : null;
    }

    /**
     * 将数据写入物理文件
     * @param key
     * @param bytes
     */
    private void writeToFile(String key, byte[] bytes, String dir) throws EngineException{
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

    private void updateIndex(String key, byte[] bytes) throws EngineException{
        BitCaskIndex index = new BitCaskIndex(key, this.activeFileId,
                bytes.length, this.activeFileOffset, new Date().getTime());
        this._indexer.put(key, index);

        //todo: 持久化到索引文件
        /*RandomAccessFile file = null;
        String filePath = dbName + INDEX_DIR + "/" + hintFileId + ".index";
        try {
            file = new RandomAccessFile(filePath, "rw");
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "索引数据文件不存在!");
        }
        long curFileLen;
        try {
            curFileLen = file.length();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "读物理数据文件长度出错!");
        }
        if (curFileLen >= DEFAULT_INDEX_FILE_SIZE) {
            try {
                file.close();
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "关闭物理文件出错!");
            }
            hintFileId += 1;
            updateIndex(key, bytes);
        } else {
            long offset = 0;
            try {
                offset = file.length();
                file.seek(offset);

            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "写索引文件出错!");
        }*/
    }

    public void close() {

    }
}
