package com.alibabacloud.polar_race.engine.bitcask;

import java.util.Arrays;

public class KeyWapper {
    private byte[] key;

    public KeyWapper(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof KeyWapper) {
            KeyWapper k = (KeyWapper) obj;
            return Arrays.equals(this.key, k.key);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
