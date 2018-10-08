package com.alibabacloud.polar_race.engine.common.utils;


import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Serialization {
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    public static String serializeToStr(Object obj) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
        objOut.writeObject(obj);
        return byteOut.toString("ISO-8859-1"); //此处只能是ISO-8859-1,但是不会影响中文使用
    }

    public static Object deserializeFromStr(String data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(data.getBytes("ISO-8859-1"));
        ObjectInputStream objIn = new ObjectInputStream(byteIn);
        return objIn.readObject();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public static void main(String[] args) {
        byte[] key = String.valueOf(1314).getBytes();
        byte[] v = "today is a good day!".getBytes();
        Map<String, String> map = new HashMap<String, String>();
        map.put("1314", "happy day");
        String s = null;
        try {
            s = serializeToStr(map);
            System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Map<String, String> omap = (Map<String, String>) deserializeFromStr(s);
            System.out.println(omap.get("1314"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
