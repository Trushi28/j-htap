package io.jhtap.storage;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

final class StreamingSSTableWriter implements AutoCloseable {
    private final DataOutputStream out;
    private final List<SSTableWriter.IndexEntry> index = new ArrayList<>();
    private final BitSet bloomFilter = new BitSet(100000);
    private long currentOffset;
    private long lastBlockOffset;
    private boolean firstInBlock = true;
    private final int blockSize = 4096;

    StreamingSSTableWriter(Path path) throws IOException {
        this.out = new DataOutputStream(new FileOutputStream(path.toFile()));
    }

    void append(Record record) throws IOException {
        if (firstInBlock || (currentOffset - lastBlockOffset >= blockSize)) {
            index.add(new SSTableWriter.IndexEntry(record.key(), currentOffset));
            lastBlockOffset = currentOffset;
            firstInBlock = false;
        }

        bloomFilter.set(Math.abs(java.util.Arrays.hashCode(record.key()) % 100000));
        int size = record.serializedSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        record.serialize(buffer);
        out.write(buffer.array(), 0, size);
        currentOffset += size;
    }

    @Override
    public void close() throws IOException {
        long dataEnd = currentOffset;
        long indexOffset = dataEnd;
        out.writeInt(index.size());
        for (SSTableWriter.IndexEntry entry : index) {
            out.writeInt(entry.key().length);
            out.write(entry.key());
            out.writeLong(entry.offset());
        }

        long indexSize = 4L;
        for (SSTableWriter.IndexEntry entry : index) {
            indexSize += 4L + entry.key().length + 8L;
        }
        long bloomOffset = indexOffset + indexSize;

        byte[] bloomBytes = bloomFilter.toByteArray();
        out.writeInt(bloomBytes.length);
        out.write(bloomBytes);

        out.writeLong(indexOffset);
        out.writeLong(bloomOffset);
        out.writeLong(dataEnd);
        out.flush();
        out.close();
    }
}
