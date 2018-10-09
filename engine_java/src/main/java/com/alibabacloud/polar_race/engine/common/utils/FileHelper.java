package com.alibabacloud.polar_race.engine.common.utils;

import com.alibabacloud.polar_race.engine.common.bitcask.BitCask;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileHelper {

    private static Logger logger = Logger.getLogger(FileHelper.class);

    public static boolean fileExists(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            return true;
        } else {
            return false;
        }
    }

    public static void createFile(String filePath) throws Exception {
        File file = new File(filePath);
        if (!file.createNewFile()) {
            throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "创建文件失败");
        }
    }

    public static void createDir(String path) throws EngineException{
        File file = new File(path);
        if (!file.mkdirs()) {
            throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "创建文件失败");
        }
    }

    public static long findActiveFileId(String path) {
        long activeFileId = 0;
        File file = new File(path);
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            String filePath = path + "/0.data";
            try {
                createFile(filePath);
            } catch (Exception e) {
                logger.error(e);
            }
            return activeFileId;
        } else {
            for (int i = 0; i < files.length; i++) {
                if (files[i].isFile()) {
                    long fileId = Long.valueOf(files[i].getName().split("\\.")[0]);
                    activeFileId = Math.max(activeFileId, fileId);
                }
            }
        }
        return activeFileId;
    }

    public static List<String> getDirFiles(String path) {
        List<String> pathFiles = new ArrayList<String>();
        File file = new File(path);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            pathFiles.add(files[i].getName());
        }
        return pathFiles;
    }
}
