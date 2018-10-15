package com.alibabacloud.polar_race.engine.common.utils;


public class KVEntry {
    private byte[] key;  //8B
    private byte[] value;  //4KB
    private int keySize;
    private int valueSize;
    static final int ENTRY_BYTE_SIZE = 4104;  //8 + 4096

    public KVEntry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        this.keySize = key.length;
        this.valueSize = value.length;
    }

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

    public int getKeySize() {
        return keySize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[keySize + valueSize];
        for (int i = 0; i < bytes.length; i++) {

        }
        return bytes;
    }
}
