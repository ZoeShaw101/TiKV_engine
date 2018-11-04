package com.alibabacloud.polar_race.engine.core.table;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MMFMapEntryImpl implements IMapEntry {

    private int index;

    private ByteBuffer dataMappedByteBuffer;
    private ByteBuffer indexMappedByteBuffer;

    public MMFMapEntryImpl(int index, ByteBuffer indexMappedByteBuffer, ByteBuffer dataMappedByteBuffer) {
        this.index = index;
        this.dataMappedByteBuffer = dataMappedByteBuffer;
        this.indexMappedByteBuffer = indexMappedByteBuffer;
    }

    int getKeyLength() {
        int offsetInIndexFile = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET;
        return this.indexMappedByteBuffer.getInt(offsetInIndexFile);
    }

    int getValueLength() {
        int offsetInIndexFile = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_VALUE_LENGTH_OFFSET;
        return this.indexMappedByteBuffer.getInt(offsetInIndexFile);
    }

    long getItemOffsetInDataFile() {
        int offsetInIndexFile = AbstractMapTable.INDEX_ITEM_LENGTH * index;
        return this.indexMappedByteBuffer.getLong(offsetInIndexFile);
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public byte[] getKey() throws IOException {
        int itemOffsetInDataFile = (int)this.getItemOffsetInDataFile();
        int keyLength = this.getKeyLength();
        byte[] result = new byte[keyLength];
        for(int i = 0; i <  keyLength; i++) {
            result[i] = this.dataMappedByteBuffer.get(i + itemOffsetInDataFile);
        }
        return result;
    }

    @Override
    public byte[] getValue() throws IOException {
        int itemOffsetInDataFile = (int)this.getItemOffsetInDataFile();
        int keyLength = this.getKeyLength();
        itemOffsetInDataFile += keyLength;
        int valueLength = this.getValueLength();
        byte[] result = new byte[valueLength];
        for(int i = 0; i <  valueLength; i++) {
            result[i] = this.dataMappedByteBuffer.get(i + itemOffsetInDataFile);
        }

        return result;
    }


    @Override
    public int getKeyHash() throws IOException {
        int offsetInIndexFile = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET;
        int hashCode = this.indexMappedByteBuffer.getInt(offsetInIndexFile);
        return hashCode;
    }

}
