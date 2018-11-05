package com.alibabacloud.polar_race.engine.utils;

import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;

public class BytesUtil {
    public static int KeyComparator(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    public static byte[] LongToBytes(long num) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(num);
        return buffer.array();
    }

    public static byte[] IntegerToBytes(int num) {
        return Ints.toByteArray(num);
    }

    public static long BytesToLong(byte[] num) {
        return ByteBuffer.wrap(num).getLong();
    }
}
