package com.alibabacloud.polar_race.engine.common.utils;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 程序正确性验证与性能测试
 * 0.Recover正确性评测
 * 1.性能评测
 */
public class BenchMark {
    private static Logger logger = Logger.getLogger(BenchMark.class);

    private final static String DB_PATH = "/Users/shaw/db";  //数据库目录

    private final static int THREAD_NUM = 8;
    private final static int KEY_NUM = 10000;

    private static ExecutorService pool = Executors.newCachedThreadPool();
    private static CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);

    private static Random random = new Random();

    public static Pair<byte[], byte[]> keyValueGenerator() {
        byte[] key = new byte[8];
        byte[] value = new byte[4000];
        random.nextBytes(key);
        random.nextBytes(value);
        return new Pair<>(key, value);
    }

    public static void SimpleTest() {
        EngineRace engineRace = new EngineRace();
        try {
            engineRace.open(DB_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        Pair<byte[], byte[]> keyValue = keyValueGenerator();
//        byte[] key = keyValue.getKey();
//        byte[] v = keyValue.getValue();
        byte[] key = String.valueOf(2144).getBytes();  //749 not: 209 128 151 470 133 779
        byte[] v = "today is a good day!".getBytes();
        try {
            engineRace.write(key, v);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            byte[] value = engineRace.read(key);
            logger.info("key=" + new String(key) + " , value=" + new String(value));
        } catch (Exception e) {
            e.printStackTrace();
        }
        engineRace.close();
    }

    public static void SysBenchMark() throws InterruptedException, EngineException {
        final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
        EngineRace engineRace = new EngineRace();
        engineRace.open(DB_PATH);
        AtomicLong byteNum = new AtomicLong(0);
        long start = System.currentTimeMillis();
        for (int i = 0; i < THREAD_NUM; i++) {
            pool.execute(() -> {
                try {
                    for (int j = 0; j < KEY_NUM; j++) {
                        byte[] key =  String.valueOf(random.nextInt(1000)).getBytes();
                        byte[] value = String.valueOf(random.nextGaussian()).getBytes();
                        byteNum.getAndAdd(key.length + value.length);
                        engineRace.write(key, value);
                    }
                } catch (Exception e) {
                    logger.error(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        pool.shutdown();
        engineRace.close();
        long cost = System.currentTimeMillis() - start;
        System.out.println("=====================================");
        System.out.println("cost=" + cost + "ms, iops=" + (1000 * THREAD_NUM * KEY_NUM) / cost + ", 吞吐量=" + 1000 * byteNum.get() / cost );
        System.out.println("=====================================");
    }


    public static void RecoveryTest() throws Exception {
        final EngineRace engineRace = new EngineRace();
        try {
            engineRace.open(DB_PATH);
            for (int i = 0; i < THREAD_NUM; i++) {
                pool.submit(new Runnable() {
                    public void run() {
                        try {
                            for (int j = 0; j < KEY_NUM; j++) {
                                int key =  random.nextInt(1000);
                                String value = String.valueOf(random.nextGaussian());
                                engineRace.write(String.valueOf(key).getBytes(), value.getBytes());
                                logger.info("线程" + Thread.currentThread().getName() + "执行了写操作");
                            }
                        } catch (Exception e) {
                            logger.error(e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
            }
        } catch (Exception e) {
            logger.error(e);
        } finally {
            pool.shutdown();
        }
        countDownLatch.await();  //阻塞主线程直到所有线程都执行完毕，关闭系统
        engineRace.close();
    }

    public static void ConcurrentTest1() throws Exception {
        long start = System.currentTimeMillis();
        final EngineRace engineRace = new EngineRace();
        try {
            engineRace.open(DB_PATH);
            for (int i = 0; i < THREAD_NUM; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        try {
                            int key =  random.nextInt(1000);
                            String value = String.valueOf(random.nextGaussian());
                            engineRace.write(String.valueOf(key).getBytes(), value.getBytes());
                        } catch (Exception e) {
                            logger.error(e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
            }
        } catch (Exception e) {
            logger.error(e);
        } finally {
            pool.shutdown();
        }
        countDownLatch.await();  //阻塞主线程直到所有线程都执行完毕，关闭系统
        engineRace.close();
        long cost = System.currentTimeMillis() - start;
    }

    public static void main(String[] args) throws Exception {
        //SimpleTest();

        //ConcurrentTest1();

        //RecoveryTest();  //运行的时候手动kill -9

        SysBenchMark();
    }
}
