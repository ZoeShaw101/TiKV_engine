package com.alibabacloud.polar_race.engine.core.table;

import java.io.IOException;

/**
 * LSM 的Entry只存key, 和value的offset
 */

public interface IMapEntry {
    final static int INDEX_ITEM_IN_DATA_FILE_OFFSET_OFFSET = 0;
    final static int INDEX_ITEM_KEY_LENGTH_OFFSET = 8;
    final static int INDEX_ITEM_VALUE_LENGTH_OFFSET = 12;
    final static int INDEX_ITEM_KEY_HASH_CODE_OFFSET = 16;

    int getIndex();

    byte[] getKey() throws IOException;

    byte[] getValue() throws IOException;

    int getKeyHash() throws IOException;

}
