package com.alibabacloud.polar_race.engine.utils;

import java.io.*;

/**
 * 自定义对象输出流，用于将对象序列化追加入文件末尾
 */

public class NewObjectOutputStream extends ObjectOutputStream{

    public NewObjectOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    @Override
    protected void writeStreamHeader() throws IOException {
        super.reset();
    }

    public static ObjectOutputStream newInstance(String filePath) throws IOException{
        File file = new File(filePath);
        long fileLen = file.length();
        ObjectOutputStream oos = null;
        if (fileLen == 0) {
            oos = new ObjectOutputStream(new FileOutputStream(file, true));
        } else {
            oos = new NewObjectOutputStream(new FileOutputStream(file, true));
        }
        return oos;
    }
}
