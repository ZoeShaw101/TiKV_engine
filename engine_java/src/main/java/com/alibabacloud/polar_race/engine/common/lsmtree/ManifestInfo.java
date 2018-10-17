package com.alibabacloud.polar_race.engine.common.lsmtree;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManifestInfo implements Serializable {

    private Map<Integer, List<byte[]>> fencePointerInfos;  //key : level_idx * fanout + table_idx
    private Map<Integer, GuavaBloomFilter> bloomFilterInfos;

    public ManifestInfo() {
        fencePointerInfos = new HashMap<>();
        bloomFilterInfos = new HashMap<>();
    }

    public Map<Integer, List<byte[]>> getFencePointerInfos() {
        return fencePointerInfos;
    }

    public void setFencePointerInfos(Map<Integer, List<byte[]>> fencePointerInfos) {
        this.fencePointerInfos = fencePointerInfos;
    }

    public Map<Integer, GuavaBloomFilter> getBloomFilterInfos() {
        return bloomFilterInfos;
    }

    public void setBloomFilterInfos(Map<Integer, GuavaBloomFilter> bloomFilterInfos) {
        this.bloomFilterInfos = bloomFilterInfos;
    }
}
