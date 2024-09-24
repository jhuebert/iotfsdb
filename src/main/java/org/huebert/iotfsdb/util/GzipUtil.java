package org.huebert.iotfsdb.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class GzipUtil {

    public static boolean isGZipped(File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (raf.length() < 3) {
                return false;
            }
            return (raf.readUnsignedByte() == 0x1f) &&
                (raf.readUnsignedByte() == 0x8b) &&
                (raf.readUnsignedByte() == 0x08);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getUncompressedSize(File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(raf.length() - 4);
            long b4 = raf.read();
            long b3 = raf.read();
            long b2 = raf.read();
            long b1 = raf.read();
            return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
