package com.alibabacloud.polar_race.engine.common.lsmtree;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Level逻辑结构
 * 每个Level中包含一些Run，这些Run的以时间顺序组织：最近使用的Run都在level的最前面
 */
public class Level {
    private static final String MANIFEST_FILE_PATH = "/lsmdb/data/manifest"; //manifest文件记录了当前所有sstable 的信息, 包括文件名, 所处level, key的范围(minKey-maxKey)等等
    private int maxRuns;
    private long maxRunSize;
    private Deque<SSTable> runs;  //最近时间的插入到最前面

    public Level(int maxRuns, long maxRunSize) {
        this.maxRuns = maxRuns;
        this.maxRunSize = maxRunSize;
        runs = new ArrayDeque<>();
    }

    public Deque<SSTable> getRuns() {
        return runs;
    }

    public long getMaxRunSize() {
        return maxRunSize;
    }

    public int getRemaining() {return maxRuns - runs.size();}

    /**
     * 二分搜索找到对应的SSTable，根据每个table的key的范围(minKey-maxKey)
     */
    public SSTable findKeyInTables(byte[] key) {

        return null;
    }
}
