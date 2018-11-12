package com.alibabacloud.polar_race.engine.db;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import com.alibabacloud.polar_race.engine.utils.NewFileManager;
import com.carrotsearch.hppc.LongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BucketDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketDB.class);

    //key长度
    private static int KEY_LEN = 8;
    //value长度
    private static int VALUE_LEN = 4 * 1024;
    //文件偏移量，按条数进行偏移,长度为int，即偏移量文件只存4字节
    private static int POS_SIZE = 4;  //高1个字节存文件位置，低3个字节存在文件中的偏移

    private static int KEY_POS_SIZE = KEY_LEN + POS_SIZE;

    //总文件数
    private static int FILE_COUNT = 64;

    //每条数据索引所占用的空间=key长度+偏移量长度
    private static final int INDEX_DATA_SIZE = KEY_LEN + POS_SIZE;

    private RandomAccessFile[] dataFiles = new RandomAccessFile[FILE_COUNT];

    private ByteBuffer[] byteBuffers = new ByteBuffer[FILE_COUNT];
    private ByteBuffer[] valueByteBuffers = new ByteBuffer[FILE_COUNT];

    private RandomAccessFile indexRandomAccessFile;
    private MappedByteBuffer indexFileMappedByteBuffer;

    public static List<LongIntHashMap> indexMapList = new ArrayList<>(FILE_COUNT);
    public static Map<Integer, Byte> threadFileMap = new ConcurrentHashMap<>();

    //索引自增器
    private AtomicLong indexAtomicLong;

    private int readCount = 0;
    private int readFileCount = 0;
    private int readErrorCount = 0;
    private int readNotFoundCount = 0;
    private int writeCount = 0;
    private int writeErrorCount = 0;

    public BucketDB() {

    }

    public void open(String path) throws EngineException {
        long beginTime = System.currentTimeMillis();
        LOGGER.info("The open method is begining...path:" + path);
        File directFile = new File(path);
        if (!directFile.exists()) {
            directFile.mkdir();
        }

        try {
            File dataDirFile = new File(path + "/data/");
            File indexDirFile = new File(path + "/index/");

            if (!dataDirFile.exists()) {
                dataDirFile.mkdir();
            }

            if (!indexDirFile.exists()) {
                indexDirFile.mkdir();
            }

            indexAtomicLong = new AtomicLong();
            //索引文件
            String indexFilePath = path + "/index/allIdx.idx";
            File indexFile = new File(indexFilePath);
            if (!indexFile.exists()) {
                indexFile.createNewFile();
            }

            if (null == indexRandomAccessFile) {
                indexRandomAccessFile = new RandomAccessFile(indexFilePath, "rw");
                indexRandomAccessFile.getChannel().position(indexRandomAccessFile.length());
            }
        } catch (Exception e) {
            LOGGER.error("indexMappedByteBuffer init error!", e);
        }


        byte fileNum = 0;
        while (fileNum < FILE_COUNT) {
            //初始化索引map
            indexMapList.add(new LongIntHashMap());

            byteBuffers[fileNum] = ByteBuffer.allocate(KEY_POS_SIZE);
            valueByteBuffers[fileNum] = ByteBuffer.allocate(VALUE_LEN);
            try {
                String dataFilePath = path + "/data/" + fileNum + ".dat";

                //数据文件
                File dataFile = new File(dataFilePath);
                if (!dataFile.exists()) {
                    dataFile.createNewFile();
                }

                dataFiles[fileNum] = new RandomAccessFile(dataFilePath, "rw");
                dataFiles[fileNum].seek(dataFiles[fileNum].length());

                LOGGER.info("File[" + fileNum + "] data size:" + dataFiles[fileNum].length());

            } catch (Exception e) {
                LOGGER.error("open method error!", e);
            }
            fileNum++;
        }
        long processTime = System.currentTimeMillis() - beginTime;
        LOGGER.error("=================Files initail times:" + processTime);

        //构造索引map
        try {
            if (indexRandomAccessFile.length() > 0) {
                newIndexMapInit();
            }
            int f = 0;
            while (f < FILE_COUNT) {
                //初始化索引自增器
                indexAtomicLong.addAndGet(indexMapList.get(f).size());
                f++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        NewFileManager.init(path);

        long allProcessTime = System.currentTimeMillis() - beginTime;
        LOGGER.info("All initail times:" + allProcessTime + ", indexAtomicLong:" + indexAtomicLong.get());
    }

    public void newIndexMapInit() {
        FileChannel fileChannel = indexRandomAccessFile.getChannel();
        boolean flag = true;
        int step = 60 * 1024 * 1024;
        AtomicInteger atomicInteger = new AtomicInteger();
        while (flag) {
            try {
                indexFileMappedByteBuffer = fileChannel.map(FileChannel.MapMode.PRIVATE, atomicInteger.getAndIncrement() * step, step);
                for (int i = 0; i < step / 12; i++) {  // 8 + 4 = 12
                    long keyLong = indexFileMappedByteBuffer.getLong();
                    int valuePos = indexFileMappedByteBuffer.getInt();
                    if (keyLong != 0) {
                        //放入map
                        int keyHashFileNum = (int) (keyLong & (FILE_COUNT - 1));//取模
                        indexMapList.get(keyHashFileNum).put(keyLong, valuePos);
                    } else {
                        flag = false;
                        break;
                    }
                }
                indexFileMappedByteBuffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    public void initIndexMapOld() throws IOException {
        indexRandomAccessFile.seek(0);
        //将position恢复到数据上次数据写入位置
        while ((indexRandomAccessFile.length() - indexRandomAccessFile.getChannel().position()) >= INDEX_DATA_SIZE) {

            byte[] keybytes = new byte[KEY_LEN];
            indexRandomAccessFile.read(keybytes);

            if (isEmptyBytes(keybytes)) {
                indexRandomAccessFile.getChannel().position(indexRandomAccessFile.getChannel().position() - KEY_LEN);
                break;
            }

            String key = new String(keybytes);

            long keyLong = BytesUtil.bytes2long(keybytes, 0);
            int keyHashFileNum = Math.abs((int) keyLong % FILE_COUNT);
            //取出位置及内容文件号byte数组
            int valuePos = indexRandomAccessFile.readInt();
//            System.out.println("initIndexMap key:" + keyLong + ",map:" + keyHashFileNum + ",valuePosBytes:" + Bytes.foreachBytes(intToByteArray(valuePos)));
            indexMapList.get(keyHashFileNum).put(keyLong, valuePos);
        }


    }

    public void initIndexMap() throws IOException {
        FileChannel fileChannel = indexRandomAccessFile.getChannel();
        fileChannel.position(0);
        //一次读取6000条
        int blockSize = 6000 * 12;
        boolean stopRead = false;
        while (fileChannel.position() < indexRandomAccessFile.length()) {

            long len = indexRandomAccessFile.length() - fileChannel.position();
            if (len < blockSize) {
                blockSize = (int) len;
            }

            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(blockSize);
            //从文件读取到byteBuffer
            fileChannel.read(byteBuffer);
            byteBuffer.position(0);

            while ((byteBuffer.capacity() - byteBuffer.position()) >= KEY_POS_SIZE) {
                byte[] keybytes = new byte[KEY_LEN];
                byteBuffer.get(keybytes);
                if (isEmptyBytes(keybytes)) {
                    fileChannel.position(fileChannel.position() - KEY_POS_SIZE);
                    stopRead = true;
                    break;
                }
                long keyLong = BytesUtil.bytes2long(keybytes, 0);
                int keyMapNum = (int) (keyLong & (FILE_COUNT - 1));
                //取出位置及内容文件号byte数组
                int temValue = byteBuffer.getInt();
//                System.out.println("initIndexMap key:"+keyLong+ ",map:"+keyHashFileNum+",valuePosBytes:"+Bytes.foreachBytes(intToByteArray(valuePos)));
                indexMapList.get(keyMapNum).put(keyLong, temValue);
            }
            if (stopRead) {
                break;
            }
        }

    }

    public boolean isEmptyBytes(byte[] keyBytes) {
        if (null == keyBytes || keyBytes.length == 0) {
            return true;
        }
        int count = 0;
        for (int i = 0; i < keyBytes.length; i++) {
            if (keyBytes[i] == 0) {
                count++;
            }
        }
        if (count == keyBytes.length) {
            return true;
        }
        return false;
    }

    public static String foreachBytes(byte[] bytes) {
        int num;
        if (bytes.length > 128) {
            num = 64;
        } else {
            num = bytes.length;
        }
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < num; i++) {
            sb.append(bytes[i]).append(",");
        }
        return sb.toString();
    }

    public void write(byte[] keyByte, byte[] value) throws EngineException {
        try {
            writeCount++;
            flush(keyByte, value);

        } catch (Exception e) {
            writeErrorCount++;
            if (writeErrorCount < 10) {
                LOGGER.error(Thread.currentThread().getName() + " write error!key:" + foreachBytes(keyByte) + ",threadName:" + Thread.currentThread().getName(), e);
            }
        }
    }


    private void flush(byte[] keyByte, byte[] value) {
        long keyLong = BytesUtil.bytes2long(keyByte, 0);

        //int fileNum = (int) (keyLong & (FILE_COUNT - 1));  //

        int fileNum = NewFileManager.get(Thread.currentThread().getName());

        /*if (fileNum == 1 && writeCount < 20) {
            System.out.println(Thread.currentThread().getName() + ",fileNum:" + fileNum);
        }*/

        RandomAccessFile dataFile = dataFiles[fileNum];
        ByteBuffer byteBuffer = byteBuffers[fileNum];
        try {
            if (keyByte.length != KEY_LEN) {
                return;
            }

            //########方案1
            //持久化到数据文件
            dataFile.write(value);
            //文件号与数据偏移块号合并为一个int,高一个字节存文件名，低三个字节存文件中的偏移
            //long vPos = dataFile.length() / VALUE_LEN - 1;
            long vPos = dataFile.length() / VALUE_LEN;
            int valuePos = (int) vPos;
            int temValue = (fileNum << 24) | valuePos;

            //按buffer写索引
            byteBuffer.putLong(keyLong);
            byteBuffer.putInt(temValue);
            byteBuffer.flip();
            long indexPos = indexAtomicLong.getAndIncrement();
            indexRandomAccessFile.getChannel().write(byteBuffer, indexPos * INDEX_DATA_SIZE);
            byteBuffer.clear();

            //放入内存
            indexMapList.get(fileNum).put(keyLong, temValue);

            //########方案2
//                int valuePos;
//                int temValue = indexMapList.get(fileNum).getOrDefault(keyLong, -1);
//                if (temValue > 0) {
//                    int valueFileNum = temValue >> 24;
//                    valuePos = valueFileNum;
//                    dataFile.seek(valuePos);
//                    dataFile.write(value);
//                } else {
//                    long indexPos = indexAtomicLong.getAndIncrement();
//
//                    dataFile.seek(dataFile.length());
//                    dataFile.write(value);
//                    long vPos = dataFile.length() / VALUE_LEN - 1;
//                    valuePos = (int) vPos;
//                    temValue = (fileNum << 24) | valuePos;
//                    byteBuffer.putLong(keyLong);
//                    byteBuffer.putInt(temValue);
//                    byteBuffer.flip();
//                    indexRandomAccessFile.getChannel().write(byteBuffer, indexPos * INDEX_DATA_SIZE);
//                    byteBuffer.clear();
//
//                    //放入内存
//                    indexMapList.get(fileNum).put(keyLong, temValue);

        } catch (Exception e) {
                writeErrorCount++;
                if (writeErrorCount < 20) {
                    LOGGER.error(Thread.currentThread().getName() + " flush error!fileNum:" + fileNum, e);
                }
            }
        //}
    }


    public byte[] read(byte[] keyByte) throws EngineException {
        byte[] value = null;
        try {
            readCount++;

            value = readFromDisk(keyByte);
        } catch (Exception e) {
            readErrorCount++;
            if (readErrorCount < 10) {
                LOGGER.error("read error!key:" + foreachBytes(keyByte) + ",threadName:" + Thread.currentThread().getName(), e);
            }
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found!");
        }

        if (null == value || isEmptyBytes(value)) {
            readNotFoundCount++;
            if (readNotFoundCount < 10) {
                LOGGER.error("NOT_FOUND!key:" + foreachBytes(keyByte) + ",threadName:" + Thread.currentThread().getName());
            }
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found!");
        }
        return value;
    }

    private byte[] readFromDisk(byte[] keyByte) {
        byte[] value = null;
        if (keyByte.length != KEY_LEN) {
            return value;
        }
        long keyLong = BytesUtil.bytes2long(keyByte, 0);

        int keyMapNum = (int) (keyLong & (FILE_COUNT - 1));

        LongIntHashMap indexMap = indexMapList.get(keyMapNum);

        int temValue = indexMap.getOrDefault(keyLong, -1);

        if (temValue < 0) {
            return value;
        }

        //read data
        //重新建一份新的数组，防止修改后内存map中的数据被修改
        int valueFileNum = temValue >> 24;
        value = new byte[VALUE_LEN];
        //从数据文件读取
        try {

            //int posVal = temValue & 0x00ffffff;
            //long pos = posVal - 1; //注意：整型转为long再进行位置计算，否则整型乘以整型后越界为负值
            long pos = (long) (temValue & 0x00ffffff) - 1;

            if (valueFileNum == 1) {
                if (readCount > 60000000 && readFileCount % 10000 == 0 && readErrorCount < 100) {
                    readErrorCount++;
                    LOGGER.error("[readFileCount:" + readFileCount + ",allRead:" + readCount + "]read key:" +
                            foreachBytes(keyByte) + ",pos:" + pos);
                }
                readFileCount++;
            }

            if (readCount > 60000000 && readCount % 100000 == 0) {
                LOGGER.error("[allRead:" + readCount + ",readNotFoundCount:" + readNotFoundCount + "]read key:" +
                        foreachBytes(keyByte) + ",valueFileNum:" + valueFileNum + ",pos:" + pos);
            }

            dataFiles[valueFileNum].seek(pos * VALUE_LEN);
            dataFiles[valueFileNum].read(value);


        } catch (Exception e) {
            readErrorCount++;
            if (readErrorCount < 10) {
                String threadName = Thread.currentThread().getName();
                LOGGER.error("readFromDisk error!threadName:" + threadName + ",threadFileMap:" + threadFileMap.size(), e);
            }
        }

        return value;
    }

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    public void close() {
        LOGGER.info("end=========================>all read:" + readCount);
    }

}