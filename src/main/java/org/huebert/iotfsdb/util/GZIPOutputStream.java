package org.huebert.iotfsdb.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;

public class GZIPOutputStream extends java.util.zip.GZIPOutputStream {

    public GZIPOutputStream(OutputStream out) throws IOException {
        super(out);
        def.setLevel(Deflater.BEST_COMPRESSION);
    }

}
