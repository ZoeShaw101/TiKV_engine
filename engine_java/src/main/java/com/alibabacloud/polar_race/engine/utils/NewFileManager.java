package com.alibabacloud.polar_race.engine.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class NewFileManager {
    public static int DATA_FILE_NUM = 64;

    public static int[] fileNum = new int[DATA_FILE_NUM];

    public static Map<String,Integer> fileChannelMap = new ConcurrentHashMap<>();

    public static AtomicInteger i = new AtomicInteger(0);

    public static void init(String path){
        for (byte i =0; i < DATA_FILE_NUM; i++){
            try {
                fileNum[i] = i;
                fileChannelMap.clear();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Integer get(String threadName){
        Integer num = fileChannelMap.get(threadName);
        if (num == null) {
            num = fileNum[i.getAndIncrement()];
            fileChannelMap.put(threadName, num);
        }
        return num;
    }
}
