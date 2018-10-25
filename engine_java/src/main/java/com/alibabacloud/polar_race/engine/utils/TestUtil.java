package com.alibabacloud.polar_race.engine.utils;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;

public class TestUtil {
    private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static Random rnd = new Random();

    private static final NumberFormat MEM_FMT = new DecimalFormat("##,###.##");

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }
}
