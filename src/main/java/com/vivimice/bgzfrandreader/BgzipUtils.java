package com.vivimice.bgzfrandreader;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BgzipUtils {

    public static int readUint16(ByteBuffer in) throws IOException {
        byte[] b = new byte[2];
        in.get(b);
        return (b[0] & 0xff) | ((b[1] & 0xff) << 8);
    }
    
    public static long readUint32(ByteBuffer in) throws IOException {
        byte[] b = new byte[4];
        in.get(b);
        return (b[0] & 0xff) | ((b[1] & 0xff) << 8) | ((b[2] & 0xff) << 16) | ((b[3] & 0xff) << 24);
    }
    
}
