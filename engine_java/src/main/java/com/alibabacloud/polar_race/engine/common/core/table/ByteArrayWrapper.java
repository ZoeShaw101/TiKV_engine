package com.alibabacloud.polar_race.engine.common.core.table;

import com.alibabacloud.polar_race.engine.common.utils.BytesUtil;
import com.google.common.base.Preconditions;

import java.util.Arrays;

public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
    private final byte[] data;

    public ByteArrayWrapper(byte[] data) {
        Preconditions.checkNotNull(data, "data is null");
        this.data = data;
    }

    public byte[] getData() {
        return this.data;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByteArrayWrapper)) {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper)other).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public int compareTo(ByteArrayWrapper o) {
        return BytesUtil.KeyComparator(this.data, o.data);
    }
}
