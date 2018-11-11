package com.alibabacloud.polar_race.engine.utils;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

public class FileUtil {
    public static void judgeFileExists(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    public static void judgeDirExists(String path) {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    //解除MappedByteBuffer与pagecache之间的映射关系
    public static void unmap(MappedByteBuffer buffer) {
        Cleaner cleaner = ((DirectBuffer)buffer).cleaner();
        if(cleaner != null) {
            cleaner.clean();
        }
    }
}
