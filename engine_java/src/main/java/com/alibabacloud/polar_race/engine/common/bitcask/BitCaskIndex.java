package com.alibabacloud.polar_race.engine.common.bitcask;

import java.io.Serializable;
import java.util.Date;

/**
 * 位于内存中的索引
 */
public class BitCaskIndex implements Serializable {
    private final String key;
    private final long fileId;  //所在物理文件名
    private final int valueSize;  //value大小
    private final long valueOffset;  //value所在物理文件的位置
    private final long timestamp;  //时间戳

    private static final long serialVersionUID = -6849794470754667710L;

    public BitCaskIndex(String key, long fileId, int valueSize, long valueOffset, long timestamp) {
        this.key = key;
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public long getFileId() {
        return fileId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getValueOffset() {
        return valueOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "BitCaskIndex{" +
                "key='" + key + '\'' +
                ", fileId=" + fileId +
                ", valueSize=" + valueSize +
                ", valueOffset=" + valueOffset +
                ", timestamp=" + timestamp +
                '}';
    }
}
