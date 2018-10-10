package com.alibabacloud.polar_race.engine.common.wal;

import java.io.IOException;

/**
 * Write Ahead Log 机制实现
 * 多生产者，单消费者
 */

public interface WAL {

    /**
     * 在当前线程中同步写文件
     * @param key 记录的key
     * @param log 这一次更改的log
     * @return 返回TransactionId
     */
    long append(String key, RedoLog log);

    /**
     * 异步提交到线程池，将数据异步、批量刷回磁盘
     * @throws IOException
     */
    void sync() throws IOException;

}
