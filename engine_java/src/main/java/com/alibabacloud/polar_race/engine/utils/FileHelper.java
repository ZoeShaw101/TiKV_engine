package com.alibabacloud.polar_race.engine.utils;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileHelper {

    private static Logger logger = Logger.getLogger(FileHelper.class);

    private static ReadWriteLock rwLock = new ReentrantReadWriteLock();  //读写锁

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

    public static void clearFileContent(String filePath) {
        File file = new File(filePath);
        try {
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write("");
            fileWriter.flush();
            //logger.info("清空文件：" + filePath);
        } catch (Exception e) {
            logger.error("删除redoLog文件失败！");
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

    public static void closeFile(RandomAccessFile file) {
        if (file != null) {
            try {
                file.close();
            } catch (IOException e) {
                logger.error("关闭文件出错" + e);
            }
        }
    }

    public static void writeObjectToFile(Object obj, String filePath) {
        ObjectOutputStream oos = null;
        rwLock.writeLock().lock();
        try {
            oos = NewObjectOutputStream.getInstance(filePath);
            oos.writeObject(obj);
            oos.flush();
        } catch (Exception e) {
            logger.error("将对象序列化进文件出错", e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                    rwLock.writeLock().unlock();
                } catch (IOException e) {
                    logger.error("关闭对象输入流出错", e);
                }
            }
        }
    }

    public static List<Object> readObjectFromFile(String filePath) {
        List<Object> objects = new ArrayList<Object>();
        File file = new File(filePath);
        ObjectInputStream ois = null;
        rwLock.readLock().lock();
        try {
            ois = new ObjectInputStream(new FileInputStream(file));
            Object object = ois.readObject();
            while (object != null) {
                objects.add(object);
                try {
                    object = ois.readObject();
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("读取对象输出流出错", e);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                    rwLock.readLock().unlock();
                } catch (IOException e) {
                    logger.error("关闭对象输出流出错", e);
                }
            }
        }
        return objects;
    }
}
