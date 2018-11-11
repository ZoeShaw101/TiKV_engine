package com.alibabacloud.polar_race.engine.utils;

import java.nio.ByteBuffer;

public class ByteBufferCommon {
    private static ByteBuffer[] byteBuffers = new ByteBuffer[CommonConfig.DATA_FILE_COUNT];
//    private static ByteBuffer[] duplicateBuffers = new ByteBuffer[Common.DATA_FILE_COUNT];

    static {
        for (int i = 0; i < byteBuffers.length; i++) {
            byteBuffers[i] = ByteBuffer.allocateDirect(CommonConfig.INDEX_KEY_LENGTH);
            byteBuffers[i].mark().position(0);
        }

        /*for (int i = 0; i < duplicateBuffers.length; i++) {
        	duplicateBuffers[i] = FileManager.getInstance().getIndexFileWriteMappedByteBuffer().duplicate();
        }*/
    }

    public static ByteBuffer getByteBuffer(int number) {
        return byteBuffers[number];
    }

    /*public static ByteBuffer getDuplicateBuffer(int number) {
    	return duplicateBuffers[number];
    }*/

}
