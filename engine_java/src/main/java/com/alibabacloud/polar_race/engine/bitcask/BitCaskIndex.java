package com.alibabacloud.polar_race.engine.bitcask;

import java.io.Serializable;

/**
 * 内存中的索引
 */
public class BitCaskIndex implements Serializable {
    private final byte[] key;
    private final long fileId;  //所在物理文件名
    private final int valueSize;  //value大小
    private final long valueOffset;  //value所在物理文件的位置
    private long timestamp;  //时间戳
    private boolean valid;  //是否有效

    private static final long serialVersionUID = -6849794470754667710L;

    public BitCaskIndex(byte[] key, long fileId, int valueSize, long valueOffset, long timestamp, boolean valid) {
        this.key = key;
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.timestamp = timestamp;
        this.valid = valid;
    }

    public byte[] getKey() {
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

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "BitCaskIndex{" +
                "key='" + new String(key) + '\'' +
                ", fileId=" + fileId +
                ", valueSize=" + valueSize +
                ", valueOffset=" + valueOffset +
                ", timestamp=" + timestamp +
                ", valid=" + valid +
                '}';
    }
}
