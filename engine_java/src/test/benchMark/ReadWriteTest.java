package benchMark;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.lsmtree.LSMTree;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


public class ReadWriteTest {
    private static Logger logger = Logger.getLogger(BenchMark.class);

    private final static String DB_PATH = "/Users/shaw/shawdb";  //数据库目录
    private final static int THREAD_NUM = Runtime.getRuntime().availableProcessors();  //8
//    private final static int THREAD_NUM = 16;
    private final static int ENTRY_NUM = 10000;

    private static Map<byte[], byte[]> kvs = new ConcurrentHashMap<>();
    private static EngineRace engineRace = new EngineRace();
    private static ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
    private static AtomicLong byteNum = new AtomicLong(0);

    private static void concurrentWrite() {
        for (int i = 0; i < THREAD_NUM; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < ENTRY_NUM; j++) {
//                            byte[] key = TestUtil.randomString(8).getBytes();
//                            byte[] value = TestUtil.randomString(4000).getBytes();
//                            kvs.put(key, value);
                            engineRace.write(TestUtil.randomString(8).getBytes(), TestUtil.randomString(4096).getBytes());
//                            key = null;
//                            value = null;
                            byteNum.getAndAdd(4008);
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            });
        }
        try {
            pool.shutdown();
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    private static void write() {
        try {
            for (byte[] key : kvs.keySet()) {
                engineRace.write(key, kvs.get(key));
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private static void read() {
        int cnt = 0;
        for (byte[] key : kvs.keySet()) {
            try {
                byte[] readVal = engineRace.read(key);
                if (readVal == null || readVal.length == 0) {
                    logger.error("没找到key=" + new String(key));
                    cnt++;
                } else if (!Arrays.equals(readVal, kvs.get(key))) {
                    logger.error("查找出的value值错误");
                    cnt++;
                } else {
                    //continue;
                    logger.info("找到key=" + new String(key));
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e);
            }
        }
        logger.info("读写测试：没找到或值错误的value的个数：" + cnt);
    }

    public static void main(String[] args) {
        long start = System.nanoTime();
        try {
            engineRace.open(DB_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        for (int i = 0; i < ENTRY_NUM * THREAD_NUM; i++) {
//            kvs.put(TestUtil.randomString(8).getBytes(), TestUtil.randomString(4000).getBytes());
//        }
//        write();

        concurrentWrite();
        engineRace.close();

//        long cost = System.nanoTime() - start;
//        System.out.println("=====================================");
//        System.out.println("cost=" + cost + "ms, iops=" + (1000000000 * THREAD_NUM * ENTRY_NUM) / cost +
//                ", 吞吐量=" + 1000000000 * byteNum.get() / cost );
//        System.out.println("=====================================");
//
//
//        try {
//            engineRace.open(DB_PATH);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        read();
//        engineRace.close();
    }
}
