package com.alibabacloud.polar_race.engine.lsmtree;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.Serializable;

public class GuavaBloomFilter implements Serializable {
    private BloomFilter<byte[]> bloomFilter;

    public GuavaBloomFilter(long expectedInsertions) {
        bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, LSMTree.FALSE_POSITIVE_PROBABILITY);
    }

    public void set(byte[] key) {
        bloomFilter.put(key);
    }

    public boolean isSet(byte[] key) {
        return bloomFilter.mightContain(key);
    }
}
