package com.alibabacloud.polar_race.engine.common.lsmtree;

import java.io.Serializable;
import java.util.BitSet;

/**
 * 判断元素是否在大数据量的集合中的高效做法
 * 缺点：存在误判率 (但是微乎其微, 也可以用白名单方法解决)
 *
 * 为了获得最优的准确率，当k = ln2 * (m/n)时，布隆过滤器获得最优的准确性； k:哈希函数的个数 m:布隆过滤器位数组的容量 n:布隆过滤器插入的数据数量
 * 在哈希函数的个数取到最优时，要让错误率不超过є，m至少需要取到最小值的1.44倍；
 */

public class BloomFilter implements Serializable {
    private BitSet bitSet;

    public BloomFilter(long length) {
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
