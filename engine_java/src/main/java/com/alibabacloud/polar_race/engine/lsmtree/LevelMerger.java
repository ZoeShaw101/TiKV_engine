package com.alibabacloud.polar_race.engine.lsmtree;

import com.alibabacloud.polar_race.engine.utils.FileHelper;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LevelMerger extends Thread {
    static final Logger logger = LoggerFactory.getLogger(LevelMerger.class);

    private static final int MAX_SLEEP_TIME = 2 * 100; // 2 seconds

    private CountDownLatch countDownLatch;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private volatile boolean stop = false;
    private List<Level> levels;
    private ManifestInfo manifestInfo;

    public LevelMerger(CountDownLatch latch, List<Level> levels, ManifestInfo manifestInfo) {
        this.countDownLatch = latch;
        this.levels = levels;
        this.manifestInfo = manifestInfo;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                mergeDown(0);
            } catch (Exception e) {
                logger.error("Merge线程" + this.getName() + "过程中出错！" + e);
            }
        }
        this.countDownLatch.countDown();
        logger.info("Merge线程" + this.getName() + "已停止");
    }

    /**
     * 自上而下进行merge操作
     */
    private void mergeDown(int currentLevel) {
        int nextLevel;
        if (levels.get(currentLevel).hasRemaining()) {
            return;
        } else if (currentLevel >= levels.size()) {
            logger.info("已经到达最后一层，没有多余的空间了");
            return;
        } else {
            nextLevel = currentLevel + 1;
        }
        //如果下一层的还有没有剩余空间，那么还要递归下一层
        if (!levels.get(nextLevel).hasRemaining()) {
            mergeDown(nextLevel);
        }
        //找到下一层是空闲的，则在把当前层所有的SSTable都归并，并放入下一层的第一个SSTable，然后清空当前层
        MergeOps mergeOps = new MergeOps();
        RandomAccessFile file = null;
        byte[] mapping = null;
        try {
            for (AbstractTable table : levels.get(currentLevel).getRuns()) {
                rwLock.readLock().lock();
                file = new RandomAccessFile(((SSTable)table).getTableFilePath(), "rw");
                mapping = new byte[(int) table.getMaxSize() * LSMTree.ENTRY_BYTE_SIZE];
                MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY,
                        0, table.getMaxSize() * LSMTree.ENTRY_BYTE_SIZE);
                buffer.get(mapping);
                mergeOps.add(mapping, table.getSize());
                rwLock.readLock().unlock();
            }
            int idx = nextLevel * LSMTree.TREE_FANOUT + levels.get(nextLevel).getRuns().size();
            levels.get(nextLevel).getWriteLock().lock();
            levels.get(nextLevel).getRuns().addFirst(new SSTable(
                    levels.get(nextLevel).getMaxRunSize(),
                    LSMTree.BF_BITS_PER_ENTRY,
                    levels.get(nextLevel).getRuns().size(),
                    nextLevel,
                    manifestInfo.getMaxKeyInfos().get(idx),
                    manifestInfo.getFencePointerInfos().get(idx),
                    manifestInfo.getBloomFilterInfos().get(idx)));
            levels.get(nextLevel).getWriteLock().unlock();
            while (!mergeOps.isDone()) {
                KVEntry entry = mergeOps.next();
                levels.get(nextLevel).getRuns().getFirst().put(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            //logger.error("merge内存映射文件错误" + e);
            e.printStackTrace();
        } finally {
            FileHelper.closeFile(file);
        }
        rwLock.writeLock().lock();
        for (AbstractTable table : levels.get(currentLevel).getRuns()) {
            FileHelper.clearFileContent(((SSTable) table).getTableFilePath());
        }
        rwLock.writeLock().unlock();
        levels.get(currentLevel).getRuns().clear();
    }

    public void setStop() {
        this.stop = true;
        logger.info("Merge线程" + this.getName() + "已停止");
    }
}
