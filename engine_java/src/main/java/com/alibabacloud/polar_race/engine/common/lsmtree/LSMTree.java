package com.alibabacloud.polar_race.engine.common.lsmtree;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Log Structured Merge Tree
 * 核心思想：将随机写转换为顺序写来大幅提高写入操作的性能
 *
 *  当内存中的MemTable满了的时候，需要将MemTable和磁盘的第一个level作归并排序
 *  SSTable: 内存中的数据结构，包括磁盘中排序文件的指针、一个固定大小的布隆过滤器和一个范围指针
 *
 *  Merge策略：K路归并排序
 *
 */

public class LSMTree {
    private Logger logger = Logger.getLogger(LSMTree.class);

    private static final double BF_BITS_PER_ENTRY = SysParam.BF_BITS_PER_ENTRY.getParamVal();
    private static final int TREE_DEPTH = (int) SysParam.TREE_DEPTH.getParamVal();
    private static final int TREE_FANOUT = (int) SysParam.TREE_FANOUT.getParamVal();
    private static final int BUFFER_MAX_ENTRIES =
            (int) SysParam.BUFFER_NUM_PAGES.getParamVal() * 4096 / 4104;  //最大页数 * 每页大小 ／ Entry大小

    private MemTable memTable;
    private List<Level> levels;
    private Map<byte[], String> thinIndex;    //稀疏索引:用于在Run中查找特定的Key, value值为该key在哪个level的哪个Run，在Run中可以使用二分查找
    private ExecutorService pool = Executors.newFixedThreadPool((int) SysParam.THREAD_COUNT.getParamVal());


    public LSMTree() {
        memTable = new MemTable(BUFFER_MAX_ENTRIES);
        levels = new ArrayList<>();
        //thinIndex = new HashMap<>();
        long maxRunSize = BUFFER_MAX_ENTRIES;
        int depth = TREE_DEPTH;
        while ((depth--) > 0) {
            levels.add(new Level(TREE_FANOUT, maxRunSize));
            maxRunSize *= BUFFER_MAX_ENTRIES;
        }
    }


    public void write(byte[] key, byte[] value) {
        //0.先看能不能插入buffer
        if (memTable.write(key, value)) {
            return;
        }
        //1.如果不能插入buffer，说明buffer已满，则看能不能将buffer刷到level 0上，先看需不需要进行归并操作
        mergeDown(0);
        //2.buffer刷到level 0上
        levels.get(0).getRuns().addFirst(new SSTable(levels.get(0).getMaxRunSize(), BF_BITS_PER_ENTRY));
        for (Map.Entry<byte[], byte[]> entry : memTable.getEntries().entrySet()) {
            levels.get(0).getRuns().getFirst().write(entry.getKey(), entry.getValue());
        }
        //3.清空buffer，并重新插入
        memTable.clear();
        assert memTable.write(key, value);
    }

    public byte[] read(final byte[] key) {
        byte[] latestVal;
        //0.先在buffer中找
        if ((latestVal = memTable.read(key)) != null)
            return latestVal;
        //1.buffer中不存在，则在runs中查找，todo:这里可以多线程并发地找
        for (Level level : levels) {
            for (SSTable run : level.getRuns()) {
                if ((latestVal = run.read(key)) != null)
                    return latestVal;
            }
        }
        /*AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger latestRun = new AtomicInteger(-1);
        ReentrantLock lock = new ReentrantLock();
        try {
            pool.execute(() -> {
                int currentRun = counter.getAndIncrement();
                Run run = getRun(currentRun);
                byte[] currentVal = run.read(key);
                if (currentVal != null);

            });
            pool.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("在runs中查找key出错", e);
        }*/
        return latestVal;
    }

    private SSTable getRun(int index) {
        return null;
    }

    /**
     * 自上而下进行merge操作
     */
    private void mergeDown(int currentLevel) {
        int nextLevel;
        if (levels.get(currentLevel).getRemaining() > 0) {
            return;
        } else if (currentLevel >= levels.size()) {
            logger.info("已经到达最后一层，没有多余的空间了");
            return;
        } else {
            nextLevel = currentLevel + 1;
        }
        //如果下一层的还有没有剩余空间，那么还要递归下一层
        if (levels.get(nextLevel).getRemaining() == 0) {
            mergeDown(nextLevel);
            assert levels.get(nextLevel).getRemaining() > 0;
        }
        //todo:找到下一层是空闲的，则在把当前层所有的runs都归并，并放入下一层的第一个run，然后清空当前层

    }



}
