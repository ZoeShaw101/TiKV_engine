package com.alibabacloud.polar_race.engine.core.table;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class FCMapTable extends AbstractSortedMapTable {

    // Create new
    public FCMapTable(String dir, int level, long createdTime, int expectedInsertions)
            throws IOException, ClassNotFoundException {
        super(dir, level, createdTime, expectedInsertions);

    }

    public FCMapTable(String dir, short shard, int level, long createdTime, int expectedInsertions)
            throws IOException, ClassNotFoundException {
        super(dir, shard, level, createdTime, expectedInsertions);

    }

    // Load existing
    public FCMapTable(String dir, String fileName)
            throws IOException, ClassNotFoundException {
        super(dir, fileName);
    }

    @Override
    public IMapEntry appendNew(byte[] key, int keyHash, byte[] value) throws IOException {
        ensureNotClosed();
        Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
        Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
        Preconditions.checkArgument(this.toAppendIndex.get() < MAX_ALLOWED_NUMBER_OF_ENTRIES,
                "Exceeded max allowed number of entries(" + MAX_ALLOWED_NUMBER_OF_ENTRIES + ")!");

        appendLock.lock();
        try {
            // write index metadata
            indexBuf.clear();
            indexBuf.putLong(IMapEntry.INDEX_ITEM_IN_DATA_FILE_OFFSET_OFFSET, toAppendDataFileOffset.get());
            indexBuf.putInt(IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET, key.length);
            indexBuf.putInt(IMapEntry.INDEX_ITEM_VALUE_ADDRESS_LENGTH_OFFSET, value.length);
            indexBuf.putInt(IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET, keyHash);

            int offsetInIndexFile = INDEX_ITEM_LENGTH * toAppendIndex.get();
            this.indexMappedByteBuffer.position(offsetInIndexFile);
            //indexBuf.rewind();
            this.indexMappedByteBuffer.put(indexBuf);

            // enlarge data file
            int dataLength = key.length + value.length;
            long dataFileLength = this.dataRaf.length();
            if (toAppendDataFileOffset.get() + dataLength >= dataFileLength) {
                this.dataRaf.setLength(dataFileLength + INIT_DATA_FILE_SIZE);
            }

            // write key/value
            ByteBuffer keyBuf = ByteBuffer.wrap(key);
            this.dataChannel.write(keyBuf, toAppendDataFileOffset.get());
            ByteBuffer valueBuf = ByteBuffer.wrap(value);
            this.dataChannel.write(valueBuf, toAppendDataFileOffset.get() + key.length);

            // update guarded condition
            this.bloomFilter.put(key);

            // commit/update offset & index
            toAppendDataFileOffset.addAndGet(dataLength);
            int appendedIndex = toAppendIndex.get();
            toAppendIndex.incrementAndGet();
            return new FCMapEntryImpl(appendedIndex, this.indexMappedByteBuffer, this.dataChannel);
        }
        finally {
            appendLock.unlock();
        }
    }

    @Override
    public IMapEntry getMapEntry(int index) {
        ensureNotClosed();
        Preconditions.checkArgument(index >= 0, "index (%s) must be equal to or greater than 0", index);
        Preconditions.checkArgument(!isEmpty(), "Can't get map entry since the map is empty");
        return new FCMapEntryImpl(index, this.indexMappedByteBuffer, this.dataChannel);
    }
}

