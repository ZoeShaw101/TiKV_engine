package com.alibabacloud.polar_race.engine.common.lsmtree;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.io.Serializable;

public class GuavaBloomFilter implements Serializable {
    private BloomFilter<String> bloomFilter;

    public GuavaBloomFilter() {
        bloomFilter = BloomFilter.create(new Funnel<String>() {
            @Override
            public void funnel(String s, PrimitiveSink primitiveSink) {
                primitiveSink.putString(s, Charsets.UTF_8);
            }
        }, 1000000, 0.001);
    }

    public void set(byte[] key) {
        bloomFilter.put(new String(key));
    }

    public boolean isSet(byte[] key) {
        return bloomFilter.mightContain(new String(key));
    }
}
