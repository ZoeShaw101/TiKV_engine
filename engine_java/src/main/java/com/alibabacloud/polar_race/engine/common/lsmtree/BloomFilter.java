package com.alibabacloud.polar_race.engine.common.lsmtree;

import java.util.BitSet;

/**
 * 判断元素是否在集合中的高效做法
 * 缺点：存在误判率 (但是微乎其微, 也可以用白名单方法解决)
 */

public class BloomFilter {
    private long length;
    private BitSet bitSet;

    public BloomFilter(long length) {
        this.length = length;
        bitSet = new BitSet((int) length);
    }

    public void set(byte[] key) {
        bitSet.set(Hash1(key));
        bitSet.set(Hash2(key));
        bitSet.set(Hash3(key));
    }

    public boolean isSet(byte[] key) {
        return bitSet.get(Hash1(key)) && bitSet.get(Hash1(key)) && bitSet.get(Hash1(key));
    }

    private int Hash1(byte[] key) {

        return -1;
    }

    private int Hash2(byte[] key) {
        return -1;
    }

    private int Hash3(byte[] key) {
        return -1;
    }


}
