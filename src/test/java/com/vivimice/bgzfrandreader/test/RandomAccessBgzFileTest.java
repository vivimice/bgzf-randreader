package com.vivimice.bgzfrandreader.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import com.vivimice.bgzfrandreader.RandomAccessBgzFile;

public class RandomAccessBgzFileTest {

    private static final File TEST_FILE = new File(
            RandomAccessBgzFileTest.class.getClassLoader().getResource("test.txt.bgz").getFile());
    
    @Test
    public void speedTest() throws Exception {
        for (int i = 0; i < 1000; i++) {
            new RandomAccessBgzFile(TEST_FILE);
        }
    }
    
    @Test
    public void randomAccessTest() throws Exception {
        byte[] expected;
        ByteArrayOutputStream out = new ByteArrayOutputStream(); 
        try (GZIPInputStream gin = new GZIPInputStream(new FileInputStream(TEST_FILE))) {
            byte[] buf = new byte[16384];
            while (true) {
                int cb = gin.read(buf);
                if (cb >= 0) {
                    out.write(buf, 0, cb);
                } else {
                    break;
                }
            }
            expected = out.toByteArray();
        }
        
        byte[] actual = new byte[expected.length];
        try (RandomAccessBgzFile bgzFile = new RandomAccessBgzFile(TEST_FILE)) {
            for (int n = 0; n < 500; n++) {
                int off = RandomUtils.nextInt(0, (int) bgzFile.inputLength());
                int len = RandomUtils.nextInt(0, (int) bgzFile.inputLength() - off);
                System.out.println(String.format("Testing (off=%d, len=%d) ...", off, len));
                bgzFile.seek(off);
                bgzFile.read(actual, off, len);
                for (int i = off; i < off + len; i++) {
                    if (expected[i] != actual[i]) {
                        Assert.fail(String.format("Test fail at off=%d, len=%d", off, len));
                    }
                }
            }
        }
    }
    
}
