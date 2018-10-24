package com.alibabacloud.polar_race.engine.utils;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * 文件锁: 基于进程级别
 * 读的时候加共享锁：其它线程可读但不可写
 * 写的时候加独占锁：其它线程不可读也不可写
 */
public class FileLockHelper {

    private static FileLock fileLock = null;

    public static void lock(RandomAccessFile file, boolean isShared) throws EngineException{
        FileChannel channel = file.getChannel();
        fileLock = null;
        while (true) {
            try {
                if (isShared) {
                    fileLock = channel.tryLock(0, channel.size(), true);
                } else {
                    fileLock = channel.tryLock();
                }
                break;
            } catch (Exception e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "文件加锁失败" + e);
            }
        }
    }

    public static void unLock(RandomAccessFile file) throws EngineException {
        if (fileLock == null) return;
        try {
            fileLock.release();
            fileLock.channel().close();
        } catch (Exception e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "文件解锁失败" + e);
        }
    }
}
