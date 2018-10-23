package com.alibabacloud.polar_race.engine.common.core.table;

import java.nio.ByteBuffer;

public class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {

    private ByteBuffer _src;

    public ThreadLocalByteBuffer(ByteBuffer src) {
        _src = src;
    }

    public ByteBuffer getSourceBuffer() {
        return _src;
    }

    @Override
    protected synchronized ByteBuffer initialValue() {
        ByteBuffer dup = _src.duplicate();
        return dup;
    }
}
