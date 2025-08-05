package org.huebert.iotfsdb.persistence;

import java.nio.ByteBuffer;

public interface PartitionByteBuffer {

    ByteBuffer getByteBuffer();

    void close();

}
