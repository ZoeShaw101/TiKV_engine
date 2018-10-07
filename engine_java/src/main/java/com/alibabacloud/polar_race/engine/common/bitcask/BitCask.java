package com.alibabacloud.polar_race.engine.common.bitcask;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.Serialization;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * Bitcask模型是一种日志型键值模型。所谓日志型，是指它不直接支持随机写入，而是像日志一样支持追加操作。Bitcask模型将随机写入转化为顺序写入
 */

public class BitCask<T> {

    private final String dbName;  //数据库名
    private String activeFileId;  //活跃文件名
    private static final int DEFAULT_ACTIVE_FILE_SIZE = 4096;  //活跃文件最大大小
    private Map<String, BitCaskIndex> _indexer;  //索引哈希表

    public BitCask(String dbName) {
        this.dbName = dbName;
        this._indexer = new HashMap<String, BitCaskIndex>();
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
    public int put(String key, T value) throws EngineException{
        byte[] bytes = null;
        try {
            bytes = Serialization.serialize(value);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "序列化出错!");
        }
        writeToFile(key, bytes);
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
        try {
            file = new RandomAccessFile(index.getFileId(), "rw");
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
            throw new EngineException(RetCodeEnum.IO_ERROR, "解序列化出错!");
        }
        return value != null ? (T)value : null;
    }


    private void writeToFile(String key, byte[] bytes) {

    }

    private void updateIndex(String key, byte[] bytes) {

    }
}
