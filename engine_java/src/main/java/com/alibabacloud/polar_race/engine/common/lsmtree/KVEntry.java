package com.alibabacloud.polar_race.engine.common.lsmtree;


import com.alibabacloud.polar_race.engine.common.lsmtree.LSMTree;

import java.util.Arrays;

public class KVEntry {
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

    public byte[] getBytes() {
        byte[] bytes = new byte[LSMTree.KEY_BYTE_SIZE + LSMTree.VALUE_BYTE_SIZE];
        for (int i = 0; i < bytes.length; i++) {

        }
        return bytes;
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
