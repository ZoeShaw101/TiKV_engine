package com.alibabacloud.polar_race.engine.core.table;

import com.alibabacloud.polar_race.engine.core.LSMDB;
import com.alibabacloud.polar_race.engine.core.utils.MMFUtil;
import com.alibabacloud.polar_race.engine.utils.BytesUtil;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HashMapTable extends AbstractMapTable {

    private AtomicBoolean immutable = new AtomicBoolean(true);

    private ConcurrentHashMap<ByteArrayWrapper, InMemIndex> hashMap;   //内存索引

    protected ThreadLocalByteBuffer localDataMappedByteBuffer;
    protected ThreadLocalByteBuffer localIndexMappedByteBuffer;

    private boolean compressionEnabled = false;

    // Create new
    public HashMapTable(String dir, int level, long createdTime)
            throws IOException {
        super(dir, level, createdTime);
        mapIndexAndDataFiles();
        initToAppendIndexAndOffset();
    }

    public HashMapTable(String dir, short shard, int level, long createdTime)
            throws IOException {
        super(dir, shard, level, createdTime);
        mapIndexAndDataFiles();   //先写入数据文件，再将索引写入索引文件内存映射
        initToAppendIndexAndOffset();  //更新offset
    }

    // Load existing
    public HashMapTable(String dir, String fileName)
            throws IOException {
        super(dir, fileName);
        mapIndexAndDataFiles();
        initToAppendIndexAndOffset();
    }

    public void setCompressionEnabled(boolean enabled) {
        this.compressionEnabled = enabled;
    }

    private void mapIndexAndDataFiles() throws IOException {
        MappedByteBuffer indexMappedByteBuffer = this.indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.indexChannel.size());
        localIndexMappedByteBuffer = new ThreadLocalByteBuffer(indexMappedByteBuffer);
        MappedByteBuffer dataMappedByteBuffer = this.dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.dataChannel.size());
        localDataMappedByteBuffer = new ThreadLocalByteBuffer(dataMappedByteBuffer);
    }

    public Set<Map.Entry<ByteArrayWrapper, InMemIndex>> getEntrySet() {
        ensureNotClosed();
        return this.hashMap.entrySet();
    }

    private void initToAppendIndexAndOffset() throws IOException {
        this.hashMap = new ConcurrentHashMap<ByteArrayWrapper, InMemIndex>(INIT_INDEX_ITEMS_PER_TABLE);
        toAppendIndex = new AtomicInteger(0);
        toAppendDataFileOffset = new AtomicLong(0);
        int index = 0;
        MMFMapEntryImpl mapEntry = new MMFMapEntryImpl(index, this.localIndexMappedByteBuffer.get(), this.localDataMappedByteBuffer.get());
        //恢复内存中的索引
        while(mapEntry.getKey().length != 0) {
            toAppendIndex.incrementAndGet();
            long nextOffset = mapEntry.getItemOffsetInDataFile() + mapEntry.getKeyLength() + mapEntry.getValueAddressLength();
            toAppendDataFileOffset.set(nextOffset);
            InMemIndex inMemIndex = new InMemIndex(index);
            hashMap.put(new ByteArrayWrapper(mapEntry.getKey()), inMemIndex);
            index++;
            mapEntry = new MMFMapEntryImpl(index, this.localIndexMappedByteBuffer.get(), this.localDataMappedByteBuffer.get());
        }
    }

    private IMapEntry appendNew(byte[] key, int keyHash, byte[] value) throws IOException {
        ensureNotClosed();

        long tempToAppendIndex;
        long tempToAppendDataFileOffset;

        appendLock.lock();
        try {

            if (toAppendIndex.get() == INIT_INDEX_ITEMS_PER_TABLE) { // index overflow
                return null;
            }
            int dataLength = key.length + value.length;
            if (toAppendDataFileOffset.get() + dataLength > INIT_DATA_FILE_SIZE) { // data overflow
                return null;
            }

            tempToAppendIndex = toAppendIndex.get();
            tempToAppendDataFileOffset = toAppendDataFileOffset.get();

            // commit/update offset & index
            toAppendDataFileOffset.addAndGet(dataLength);
            toAppendIndex.incrementAndGet();
        }
        finally {
            appendLock.unlock();
        }

        // write index metadata
        ByteBuffer tempIndexBuf = ByteBuffer.allocate(INDEX_ITEM_LENGTH);
        tempIndexBuf.putLong(IMapEntry.INDEX_ITEM_IN_DATA_FILE_OFFSET_OFFSET, tempToAppendDataFileOffset);
        tempIndexBuf.putInt(IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET, key.length);
        tempIndexBuf.putInt(IMapEntry.INDEX_ITEM_VALUE_ADDRESS_LENGTH_OFFSET, value.length);
        tempIndexBuf.putInt(IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET, keyHash);

        int offsetInIndexFile = INDEX_ITEM_LENGTH * (int)tempToAppendIndex;
        ByteBuffer localIndexBuffer = this.localIndexMappedByteBuffer.get();
        localIndexBuffer.position(offsetInIndexFile);
        //indexBuf.rewind();
        localIndexBuffer.put(tempIndexBuf);
        tempIndexBuf = null;

        // write key/value
        ByteBuffer localDataBuffer = this.localDataMappedByteBuffer.get();
        localDataBuffer.position((int)tempToAppendDataFileOffset);
        localDataBuffer.put(ByteBuffer.wrap(key));
        localDataBuffer.position((int)tempToAppendDataFileOffset + key.length);
        localDataBuffer.put(ByteBuffer.wrap(value));

        this.hashMap.put(new ByteArrayWrapper(key), new InMemIndex((int)tempToAppendIndex));

        if (LSMDB.DEBUG_ENABLE) {
            log.info("写入key=" + new String(key) + "到内存映射");
        }
        return new MMFMapEntryImpl((int)tempToAppendIndex, localIndexBuffer, localDataBuffer);
    }

    @Override
    public IMapEntry getMapEntry(int index) {
        ensureNotClosed();
        Preconditions.checkArgument(index >= 0, "index (%s) must be equal to or greater than 0", index);
        Preconditions.checkArgument(!isEmpty(), "Can't get map entry since the map is empty");
        return new MMFMapEntryImpl(index, this.localIndexMappedByteBuffer.get(), this.localDataMappedByteBuffer.get());
    }

    @Override
    public Result get(byte[] key) throws IOException {
        ensureNotClosed();
        Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
        Result result = new Result();
        InMemIndex inMemIndex = this.hashMap.get(new ByteArrayWrapper(key));
        if (inMemIndex == null) return result;

        IMapEntry mapEntry = this.getMapEntry(inMemIndex.getIndex());
        result.setValue(mapEntry.getValueAddress());
        result.setLevel(this.getLevel());
        return result;
    }

    public void markImmutable(boolean immutable) {
        this.immutable.set(immutable);
    }

    public boolean isImmutable() {
        return this.immutable.get();
    }

    public boolean put(byte[] key, long valueAddress) throws IOException {
        ensureNotClosed();
        Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");

        IMapEntry mapEntry = null;
        mapEntry = this.appendNew(key, Arrays.hashCode(key), BytesUtil.LongToBytes(valueAddress));

        if (mapEntry == null) { // no space
            return false;
        }

        mapEntry = null;

        return true;
    }

    public int getRealSize() {
        return this.hashMap.size();
    }

    @Override
    public void close() throws IOException {
        if (this.localIndexMappedByteBuffer == null) return;
        if (this.localDataMappedByteBuffer == null) return;
        MMFUtil.unmap((MappedByteBuffer)this.localIndexMappedByteBuffer.getSourceBuffer());
        this.localIndexMappedByteBuffer.remove();
        this.localIndexMappedByteBuffer = null;
        MMFUtil.unmap((MappedByteBuffer)this.localDataMappedByteBuffer.getSourceBuffer());
        this.localDataMappedByteBuffer.remove();
        this.localDataMappedByteBuffer = null;
        super.close();
        log.info("HashMapTable:" + this.fileName + "正常关闭!");
    }
}

