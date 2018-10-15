package com.alibabacloud.polar_race.engine.common.lsmtree;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Log Structured Merge Tree
 * 核心思想：将随机写转换为顺序写来大幅提高写入操作的性能
 *
 * 设计模块：
 * 0.内存中的buffer（相当于MemTable）
 * 1.Level design：当Buffer满了的时候，需要将buffer和次噢案中的第一个level作归并排序
 *   1.0 Run: 内存中的数据结构，包括磁盘中排序文件的指针、一个固定大小的布隆过滤器和一个范围指针
 *
 */

public class LSMTree {
    private static final double BF_BITS_PER_ENTRY = SysParam.BF_BITS_PER_ENTRY.getParamVal();
    private static final int TREE_DEPTH = (int) SysParam.TREE_DEPTH.getParamVal();
    private static final int TREE_FANOUT = (int) SysParam.TREE_FANOUT.getParamVal();
    private static final int BUFFER_MAX_ENTRIES =
            (int) SysParam.BUFFER_NUM_PAGES.getParamVal() * 4096 / Entry.ENTRY_BYTE_SIZE;  //最大页数 * 每页大小 ／ Entry大小

    private Buffer buffer;
    private List<Level> levels;
    private ExecutorService pool = Executors.newFixedThreadPool((int) SysParam.THREAD_COUNT.getParamVal());


    public LSMTree() {
        buffer = new Buffer(BUFFER_MAX_ENTRIES);
        levels = new ArrayList<>();
        long maxRunSize = BUFFER_MAX_ENTRIES;
        int depth = TREE_DEPTH;
        while ((depth--) > 0) {
            levels.add(new Level(TREE_FANOUT, maxRunSize));
            maxRunSize *= BUFFER_MAX_ENTRIES;
        }
    }


    public void write(byte[] key, byte[] value) {
        //先看能不能插入buffer
        if (buffer.write(key, value)) {
            return;
        }
        //如果不能插入buffer，说明buffer已满，则看能不能将buffer刷到level 0上，先看需不需要进行归并操作
        mergeDown(levels.iterator());
        //buffer刷到level 0上
        levels.get(0).getRuns().addFirst(new Run(levels.get(0).getMaxRunSize(), BF_BITS_PER_ENTRY));
        for (Entry entry : buffer.getEntries()) {
            levels.get(0).getRuns().getFirst().write(entry);
        }
        //清空buffer，并重新插入
        buffer.clear();
        assert buffer.write(key, value);
    }

    public void read() {
        //先在buffer中找

        //buffer中不存在，则在run中查找
    }



    private Run getRun(int index) {
        return null;
    }

    private void mergeDown(Iterator<Level> iterator) {

    }



}
