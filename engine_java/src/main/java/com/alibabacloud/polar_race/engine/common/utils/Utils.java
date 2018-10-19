package com.alibabacloud.polar_race.engine.common.utils;

import java.util.Arrays;

public class Utils {
    public static int KeyComparator(byte[] b1, byte[] b2, int len) {
        for (int i = 0; i < len; i++) {
            if (b1[i] != b2[i]) {
                if ((b1[i] >= 0 && b2[i] >= 0) || (b1[i] < 0 && b2[i] < 0))
                    return b1[i] - b2[i];
                if (b1[i] < 0 && b2[i] >= 0)
                    return 1;
                if (b2[i] < 0 && b1[i] >=0)
                    return -1;
            }
        }
        return 0;
    }

    public static void main(String[] args) {
        byte[] k1 = new byte[8];
        byte[] k2 = new byte[8];
        Arrays.fill(k1, (byte) 0x32);
        Arrays.fill(k2, (byte) 0x34);
        if (Utils.KeyComparator(k1, k2, k1.length) < 0)
            System.out.println("=====");
    }
}
