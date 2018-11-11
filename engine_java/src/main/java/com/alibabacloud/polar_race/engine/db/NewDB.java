package com.alibabacloud.polar_race.engine.db;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import com.alibabacloud.polar_race.engine.utils.FileUtil;
import com.alibabacloud.polar_race.engine.utils.KVUtil;
import com.carrotsearch.hppc.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class NewDB {
    private static final Logger logger = LoggerFactory.getLogger(NewDB.class);
    private String relativePath;
    private String indexFilePath;
    private String dataFilePath;

//	private long indexFileSize = 490 * 1024 * 1024;


    private MappedByteBuffer indexFileMappedByteBuffer;
    private LongBuffer indexFileLongBuffer;
    /**
     * 索引文件中存储的有效的key的数量
     */
//	private long keyNum;
    /**
     * 当前写的数据文件的MappedByteBuffer，每次从当前有效数据所在的位置开始向后映射1024MB(可调整优化)
     */
//	private MappedByteBuffer currentWriteDataFileMappedByteBuffer;

    /**
     * size : 16m map(647000000,0.99) read time: 127.65s write time: 131.77s sum:746.26s
     * size : 16m map(83886080,0.77)  oom
     * size : 24m map(647000000,0.99)  read time: 186.12s write time: 427.75s sum:620.49s
     * size : 128M read time: 255.87s write time: 406.35s sum:669.16s
     */
    private long writeDataFileBlockSize = 24 * 1024 * 1024;

    private long _4KB = 4 * 1024;

//	private RandomAccessFile randomAccessDataFile;
//	private FileChannel dataFileFc;

    private RandomAccessFile randomAccessIndexFile= null;
//	private FileChannel indexFileFc = null;

    /**
     * 每个线程在读取DataFile中的Data时使用自己的RandomAccessFile句柄，防止频繁获取文件句柄耗时
     */
    private ThreadLocal<RandomAccessFile> threadRandomAccessDataFile = new ThreadLocal<RandomAccessFile>();
    private ThreadLocal<RandomAccessFile> threadRandomReadDataFile = new ThreadLocal<RandomAccessFile>();
    private ThreadLocal<RandomAccessFile> threadRandomAccessIndexFile = new ThreadLocal<RandomAccessFile>();
    private ThreadLocal<byte[]> threadReadValue = new ThreadLocal<byte[]>();
    private LongLongHashMap indexHashMap;

    private ThreadLocal<MappedByteBuffer> currentIndexFileMappedByteBuffer = new ThreadLocal<MappedByteBuffer>();
    private ThreadLocal<LongBuffer> currentIndexFileLongBuffer = new ThreadLocal<LongBuffer>();
    private ThreadLocal<Long> theadkeyNum = new ThreadLocal<Long>();
    private ThreadLocal<MappedByteBuffer> currentWriteDataFileMappedByteBuffer = new ThreadLocal<MappedByteBuffer>();
    private long startTime;

    private AtomicInteger fileCount = new AtomicInteger(0);
    private ThreadLocal<Byte> threadIndex = new ThreadLocal<Byte>();

    public void open(String path) throws EngineException {
        // TODO:打开test_directory下索引文件的存储目录，构建所有的bloomFilter
        logger.info("EngineRace.open() path:" + path);
        this.relativePath = path + "/";
        indexFilePath = relativePath + "indexFile";
        dataFilePath = relativePath + "dataFile";

        FileUtil.judgeDirExists(relativePath);
        startTime = System.nanoTime();

        try {
            if (!new File(indexFilePath+0).exists()){
                indexHashMap = null;
                return;
            }else {
                //83886080,0.77
                //64700000,0.99
                indexHashMap = new LongLongHashMap(64700000,0.99);
            }
            for (int i=0;i<64;i++){
                //如果第一个文件都不存在，即第一次打开引擎，不做任何操作直接结束方法
                if (!new File(indexFilePath+i).exists()){
                    return;
                }
                //开始读文件
                randomAccessIndexFile = new RandomAccessFile(indexFilePath+i, "r");
                indexFileMappedByteBuffer = randomAccessIndexFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, (8+8)*1000000 + 8);
                indexFileLongBuffer = indexFileMappedByteBuffer.asLongBuffer();
                int keyNum = (int) indexFileLongBuffer.get(0); // 实际存储的key的数量
                if (keyNum < 0) {
                    keyNum = 0;
                } else if (keyNum > 0) {
                    long tempKey, tempPosition, tempValue, temp,readValue;
                    long tempTimestamp;
                    byte tempIndexcode;
                    for (long j = 1; j <= keyNum; ) {
                        tempKey = indexFileLongBuffer.get((int) j++);
                        //获得indexcode和timestamp组成的key
                        readValue = indexFileLongBuffer.get((int)(j++));
                        tempTimestamp = KVUtil.getTimeStamp(readValue);

                        //如果该键已存在
                        if ((temp = indexHashMap.get(tempKey)) != 0) {
                            long oldstamp = KVUtil.getTimeStamp(temp);
                            //如果当前时间戳大，则为后更新的，插入记录
                            if (tempTimestamp > oldstamp) {
                                indexHashMap.put(tempKey, readValue);
                            }
                        } else {
                            //如果该键不存在，直接插入
                            indexHashMap.put(tempKey, readValue);
                        }
                    }
                }
                FileUtil.unmap(indexFileMappedByteBuffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("EngineRace.open() read index File Error: ", e);
            throw new EngineException(RetCodeEnum.IO_ERROR, "EngineRace.open() read index File Error");
        }

    }

    public void write(byte[] key, byte[] value) throws EngineException {
        long currentEndTime =  System.nanoTime();
        long timestamp = (currentEndTime-startTime);
        //init keyNum
        byte indexCode = 0;
        if((threadIndex.get())==null){
            indexCode = (byte) fileCount.getAndIncrement();
            threadIndex.set(indexCode);
        }else {
            indexCode = threadIndex.get();
        }
        //获得该线程自己的写索引对象
        RandomAccessFile indexFile;
        if ((indexFile = threadRandomAccessIndexFile.get()) == null) {
            try {
                indexFile = new RandomAccessFile(indexFilePath+indexCode, "rw");
                threadRandomAccessIndexFile.set(indexFile);
            } catch (FileNotFoundException e) {
                logger.error("EngineRace.write() create threadRandomAccessIndexFile Error: ", e);
                throw new EngineException(RetCodeEnum.IO_ERROR, "EngineRace.write() create threadRandomAccessIndexFile Error");
            }
        }
        //init mapbuffer
        MappedByteBuffer indexMapBuffer;
        if ((indexMapBuffer = currentIndexFileMappedByteBuffer.get()) == null) {
            try {
                indexMapBuffer = indexFile.getChannel().map(FileChannel.MapMode.READ_WRITE,0,(8+8)*1000000 + 8);
                currentIndexFileMappedByteBuffer.set(indexMapBuffer);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        //init longbuffer
        LongBuffer indexLongBuffer;
        if ((indexLongBuffer = currentIndexFileLongBuffer.get()) == null) {
            indexLongBuffer = indexMapBuffer.asLongBuffer();
            currentIndexFileLongBuffer.set(indexLongBuffer);
        }
        //init keyNum
        long keyNum = 0;
        if((theadkeyNum.get())==null){
            theadkeyNum.set(keyNum);
        }else {
            keyNum = theadkeyNum.get();
        }

        //获得该线程自己的写文件对象
        RandomAccessFile dataFile;
        if ((dataFile = threadRandomAccessDataFile.get()) == null) {
            try {
                dataFile = new RandomAccessFile(dataFilePath+indexCode, "rw");
                threadRandomAccessDataFile.set(dataFile);
            } catch (FileNotFoundException e) {
                logger.error("EngineRace.write() create threadRandomAccessDataFile Error: ", e);
                throw new EngineException(RetCodeEnum.IO_ERROR, "EngineRace.write() create threadRandomAccessDataFile Error");
            }
        }
        // init data write map buffer
        MappedByteBuffer dataMappedBuffer;
        if ((dataMappedBuffer = currentWriteDataFileMappedByteBuffer.get())==null){
            try {
                dataMappedBuffer = dataFile.getChannel().map(FileChannel.MapMode.READ_WRITE,(keyNum/2 + 1) * _4KB,writeDataFileBlockSize);
                currentWriteDataFileMappedByteBuffer.set(dataMappedBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //write value into datamapbuffer
        dataMappedBuffer.put(value);

        long exactKey = BytesUtil.byteToLong(key);
        //write key into keymapbuffer
        indexLongBuffer.put((int) ++keyNum, exactKey);
        //构建索引时需要timestamp，因此用这两个值来产生一个value
        indexLongBuffer.put((int) ++keyNum, KVUtil.generateValue(indexCode,timestamp, (int) (keyNum/2)));
        theadkeyNum.set(keyNum);
        indexLongBuffer.put(0,keyNum);

//		 4、如果当前数据文件块被写满了，则解除文件映射并将其更新到磁盘
        if (dataMappedBuffer.position() == dataMappedBuffer.limit()) {
            // 解除映射关系，防止进程mmap占用内存过多被cggroup杀掉
            FileUtil.unmap(dataMappedBuffer);
            currentWriteDataFileMappedByteBuffer.remove();
        }

    }

    /**
     * 这里的read操作假设是在并发写操作完成之后，清空pageCache，重启engine的情况下进行的
     * @param key
     * @return
     * @throws EngineException
     */
    public byte[] read(byte[] key) throws EngineException {
        long exactKey = BytesUtil.byteToLong(key);

        long tempValue = indexHashMap.get(exactKey);
        long dataFilePosition = KVUtil.getPosition(tempValue)*_4KB;
        byte indexCode = KVUtil.getIndexCode(tempValue);

        // dataFilePosition为0代表要查找的key不存在
        if (dataFilePosition <= 0) {
            return null;
        }
//		RandomAccessFile dataFile = null;
//		try {
//			dataFile = new RandomAccessFile(dataFilePath+indexCode, "r");
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
        RandomAccessFile dataFile;
        if ((dataFile = threadRandomReadDataFile.get()) == null) {
            try {
                dataFile = new RandomAccessFile(dataFilePath +  indexCode, "r");
                threadRandomReadDataFile.set(dataFile);
            } catch (FileNotFoundException e) {
                logger.error("EngineRace.read() create threadRandomAccessDataFile Error: ", e);
                throw new EngineException(RetCodeEnum.IO_ERROR, "EngineRace.read() create threadRandomAccessDataFile Error");
            }
        }

        byte[] value;
        if ((value = threadReadValue.get()) == null) {
            value = new byte[(int)_4KB];
            threadReadValue.set(value);
        }
        int readNum;
        try {
            dataFile.seek(dataFilePosition);
            readNum = dataFile.read(value);
//			dataFile.close();
        } catch (IOException e) {
            logger.error("EngineRace.read() seek threadRandomReadDataFile Error: ", e);
            System.out.println("读异常时 datafiletion : "+dataFilePosition);
            throw new EngineException(RetCodeEnum.IO_ERROR, "EngineRace.read() seek threadRandomReadDataFile Error");
        }
        if (readNum != _4KB) {
            logger.error("EngineRace.read() read value Error");
            throw new EngineException(RetCodeEnum.NOT_FOUND, "EngineRace.read() read value Error");
        }
        return value;
    }

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        logger.info("EngineRace.range() is invoked!");
    }

    public void close() {
        logger.info("close is invoked!");
        if (indexFileMappedByteBuffer != null) {
            FileUtil.unmap(indexFileMappedByteBuffer);
        }
    }
}
