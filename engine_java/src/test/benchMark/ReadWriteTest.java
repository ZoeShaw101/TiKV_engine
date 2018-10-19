package benchMark;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ReadWriteTest {
    private static Logger logger = Logger.getLogger(BenchMark.class);

    private final static String DB_PATH = "/Users/shaw/lsmdb";  //数据库目录
    private final static int THREAD_NUM = 8;
    private final static int ENTRY_NUM = 500;

    private static Map<byte[], byte[]> kvs;

    private static void write(EngineRace engineRace) {
        for (byte[] key : kvs.keySet()) {
            try {
                engineRace.write(key, kvs.get(key));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void read(EngineRace engineRace) {
        int cnt = 0;
        for (byte[] key : kvs.keySet()) {
            try {
                byte[] readVal = engineRace.read(key);
                if (readVal == null) {
                    logger.info("没找到value");
                    cnt++;
                } else if (!Arrays.equals(readVal, kvs.get(key))) {
                    logger.info("查找出的value值错误");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        logger.info("没找到value的个数：" + cnt);
    }

    public static void main(String[] args) {
        EngineRace engineRace = new EngineRace();
        try {
            engineRace.open(DB_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
        kvs = new HashMap<>();
        for (int i = 0; i < ENTRY_NUM; i++) {
            kvs.put(TestUtil.randomString(8).getBytes(), TestUtil.randomString(4000).getBytes());
        }
        write(engineRace);
        engineRace.close();
        try {
            engineRace.open(DB_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
        read(engineRace);
        engineRace.close();
    }
}
