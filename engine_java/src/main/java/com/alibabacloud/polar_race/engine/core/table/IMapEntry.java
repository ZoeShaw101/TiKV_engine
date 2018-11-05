package com.alibabacloud.polar_race.engine.core.table;

import java.io.IOException;

/**
 * LSM 的Entry只存key, 和value的offset
 */

public interface IMapEntry {
    final static int INDEX_ITEM_IN_DATA_FILE_OFFSET_OFFSET = 0;      //数据的index偏移位置
    final static int INDEX_ITEM_KEY_LENGTH_OFFSET = 8;               //key的位置
    final static int INDEX_ITEM_VALUE_ADDRESS_LENGTH_OFFSET = 12;    //注意这里不存key,而存value在value log中的address位置
    final static int INDEX_ITEM_KEY_HASH_CODE_OFFSET = 16;           //hash code的位置

    int getIndex();

    byte[] getKey() throws IOException;

    byte[] getValueAddress() throws IOException;

    int getKeyHash() throws IOException;

}
