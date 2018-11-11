package com.alibabacloud.polar_race.engine.utils;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yanghongfeng1@jd.com
 * JUST DO IT
 * @datc 2018/11/8 9:17
 */
public class FileManager {

    public static int DATA_FILE_NUM = 64;

    public static int[] fileNum = new int[DATA_FILE_NUM];

    public static RandomAccessFile[] fileEntries = new RandomAccessFile[DATA_FILE_NUM];

    public static ByteBuffer [] indexByteBuffers = new ByteBuffer[DATA_FILE_NUM];

    public static Map<String,Integer> fileChannelMap = new ConcurrentHashMap<>();

    public static AtomicInteger i = new AtomicInteger(0);

    public static void init(String path){
        for (byte i =0; i< DATA_FILE_NUM;i++){
            try {
                fileNum[i] = i;
                fileEntries[i] = new RandomAccessFile(path+"/data_"+i + ".txt","rw");
                fileEntries[i].seek(fileEntries[i].length());
                indexByteBuffers[i] = ByteBuffer.allocateDirect(12*1024*10);
                fileChannelMap.clear();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Integer get(String threadName){
        Integer num = fileChannelMap.get(threadName);
        if (num == null){
            num = fileNum[i.getAndIncrement()];
            fileChannelMap.put(threadName,num);
        }
        return num;
    }
}
