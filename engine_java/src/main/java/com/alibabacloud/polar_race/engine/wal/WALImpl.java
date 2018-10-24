package com.alibabacloud.polar_race.engine.wal;

import java.io.IOException;

public class WALImpl implements WAL {

    public long append(String key, RedoLog log) {

        return 0;
    }

    public void sync() throws IOException {

    }
}
