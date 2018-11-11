package com.alibabacloud.polar_race.engine.utils;

public class CommonConfig {

        // 数据文件个数
        public static final int DATA_FILE_COUNT = 64;

        // 数据缓冲块大小
        public static final int BLOCK_SIZE = 4 * 1024;

        // value大小
        public static final int VALUE_SIZE = 4 * 1024;

        // 索引文件大小
        public static final int INDEX_FILE_SIZE = 800 * 1024 * 1024;

        // 索引Map个数
        public static final int INDEX_MAP_SIZE = 64;

        // 每个索引Map初始化容量
        public static final int RECORD_SIZE_PER_MAP = 1000000;

        // 索引Key长度
        public static final int INDEX_KEY_LENGTH = 12;

}
