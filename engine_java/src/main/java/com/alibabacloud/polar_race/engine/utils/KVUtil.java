package com.alibabacloud.polar_race.engine.utils;

public class KVUtil {
    public static long generateValue(byte indexCode,long timestamp,int position){
        //先将long值挪到最后24位
        long value = position;
        //先转为long型，填满空缺位，再进行位操作
        value = (long)indexCode << 58| value;
        value = (long)timestamp << 20 | value;
        return value;
    }

    public static byte getIndexCode(long generatedValue){
        //将最高位移到最低位8bit，用byte截断,correct
//        return (byte) ((byte) (generatedValue >>> 58) & 0x3f);
        return (byte) (generatedValue >>> 58);
    }
    //    public static long getTimeStampOrPos(long generatedValue){
//      //左移6位清零头部indexcode，再移动回来
//        return generatedValue << 6 >>> 26;
//    }
    public static long getTimeStamp(long generatedValue){
        //左移6位清零头部indexcode，再移动回来
        return generatedValue << 6 >>> 26;
    }
    public static int getPosition(long generatedValue){
        int position = 0;
        //先用int 32bit截断，左移八位清零，再恢复
        position = (int)generatedValue << 12 >>> 12;
        return position;
    }
}
