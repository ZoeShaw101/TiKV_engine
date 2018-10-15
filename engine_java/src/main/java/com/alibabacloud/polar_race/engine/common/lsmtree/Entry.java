package com.alibabacloud.polar_race.engine.common.lsmtree;


public class Entry {
    private byte[] key;  //8B
    private byte[] value;  //4KB
    static final int ENTRY_BYTE_SIZE = 4104;  //8 + 4096

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
