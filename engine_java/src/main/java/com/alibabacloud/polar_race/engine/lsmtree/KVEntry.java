package com.alibabacloud.polar_race.engine.lsmtree;


import java.io.Serializable;
import java.util.Arrays;

public class KVEntry implements Serializable {
    private byte[] key;  //8B
    private byte[] value;  //4KB

    public KVEntry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
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

    public byte[] toBytes() {
        byte[] arr = new byte[key.length + value.length];
        System.arraycopy(key, 0, arr, 0, key.length);
        System.arraycopy(value, 0, arr, key.length, value.length);
        return arr;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof KVEntry) {
            KVEntry e = (KVEntry) obj;
            return Arrays.equals(e.getKey(), this.getKey());
        }
        return false;
    }
}
