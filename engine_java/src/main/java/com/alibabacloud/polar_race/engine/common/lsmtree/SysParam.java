package com.alibabacloud.polar_race.engine.common.lsmtree;

/**
 * 系统参数
 * 可用于调节性能
 */
public enum SysParam {
    BUFFER_NUM_PAGES(1000),   //缓存的页数
    BF_BITS_PER_ENTRY(0.5),   //每个entry的布隆过滤器的位数
    TREE_DEPTH(5),            //磁盘中level的深度，即有几层
    TREE_FANOUT(10),          //每个level所包含的run的树木，即level的扇出
    THREAD_COUNT(4);          //线程数

    private double paramVal;

    SysParam(double paramVal) {
        this.paramVal = paramVal;
    }

    public double getParamVal() {
        return paramVal;
    }
}
