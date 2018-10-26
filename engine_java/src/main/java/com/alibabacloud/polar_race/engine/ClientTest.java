package com.alibabacloud.polar_race.engine;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.utils.TestUtil;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ClientTest {
    private static Logger logger = Logger.getLogger(ClientTest.class);

    private final static String DB_PATH = "/Users/shaw/shawdb";  //数据库目录
    private final static int THREAD_NUM = Runtime.getRuntime().availableProcessors();  //8
    private final static int ENTRY_NUM = 100;

    private static Map<byte[], byte[]> kvs = new ConcurrentHashMap<>();
    private static EngineRace engineRace = new EngineRace();
    private static ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
    private static AtomicLong byteNum = new AtomicLong(0);


    public static void main(String[] args) {
        long start = System.nanoTime();
        try {
            engineRace.open(DB_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            engineRace.write(TestUtil.randomString(8).getBytes(), TestUtil.randomString(4000).getBytes());
            engineRace.write(TestUtil.randomString(8).getBytes(), TestUtil.randomString(4000).getBytes());
        } catch (Exception e) {
            logger.info("写出错" + e);
        }
        engineRace.close();

    }
}
