package com.alibabacloud.polar_race.engine.wal;


import java.io.Serializable;

/**
 * 重做日志：实现数据库的事务一致性
 * WAL(Write Ahead Log)机制，写入先写Log，再刷盘
 *
 * 实现原理：
 * 正常关闭的操作，数据库redoLog是空的（正常操作提交后会清空redoLog）；
 * 如果是突然系统意外停止，则会产生redoLog，那么下次打开数据库时，如果有redoLog，则需要将redoLog中的操作Commit或者Rollback
 *
 * 一个完整的事务流程，只要记录写没写磁盘成功：
 * 1. {key, oldValue, newValue, commitStart} 开始写磁盘
 * 2. {key, oldValue, newValue, commitEnd} 成功写磁盘，事务成功
 */

public class RedoLog implements Serializable {
    private boolean isBegin;
    private byte[] key;
    private byte[] newValue;
    private boolean isCommit;
    private long timestamp;

    public RedoLog() {
        this.isBegin = false;
        this.key = null;
        this.newValue = null;
        this.isCommit = false;
        this.timestamp = -1;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getNewValue() {
        return newValue;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setNewValue(byte[] newValue) {
        this.newValue = newValue;
    }

    public boolean isBegin() {
        return isBegin;
    }

    public void setBegin(boolean begin) {
        isBegin = begin;
    }

    public boolean isCommit() {
        return isCommit;
    }

    public void setCommit(boolean commit) {
        isCommit = commit;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "RedoLog{" +
                "isBegin=" + isBegin +
                ", key='" + new String(key) + '\'' +
                ", newValue=" + new String(newValue) +
                ", isCommit=" + isCommit +
                ", timestamp=" + timestamp +
                '}';
    }
}
