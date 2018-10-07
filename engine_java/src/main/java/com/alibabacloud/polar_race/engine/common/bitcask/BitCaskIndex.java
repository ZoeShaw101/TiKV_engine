package com.alibabacloud.polar_race.engine.common.bitcask;

import java.io.Serializable;
import java.util.Date;

/**
 * 位于内存中的索引
 */
public class BitCaskIndex implements Serializable{
    private final String key;
    private final String fileId;  //所在物理文件名
    private final int valueSize;  //value大小
    private final long valueOffset;  //value所在物理文件的位置
    private final Date timestamp;  //时间戳

    public BitCaskIndex(String key, String fileId, int valueSize, long valueOffset, Date timestamp) {
        this.key = key;
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public String getFileId() {
        return fileId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getValueOffset() {
        return valueOffset;
    }

    public Date getTimestamp() {
        return timestamp;
    }
}
