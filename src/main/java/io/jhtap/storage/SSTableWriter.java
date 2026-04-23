package io.jhtap.storage;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class SSTableWriter {
    private final Path path;
    private final List<IndexEntry> index = new ArrayList<>();
    private final BitSet bloomFilter = new BitSet(100000);
    private long currentOffset = 0;
    private long lastBlockOffset = 0;
    private final int blockSize = 4096;

    public record IndexEntry(byte[] key, long offset) {}

    public SSTableWriter(Path path) {
        this.path = path;
    }

    public void write(MemTable memTable) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             DataOutputStream out = new DataOutputStream(fos)) {
            
            boolean firstInBlock = true;
            for (Map.Entry<InternalKey, Record> entry : memTable.entries()) {
                InternalKey key = entry.getKey();
                Record record = entry.getValue();

                if (firstInBlock || (currentOffset - lastBlockOffset >= blockSize)) {
                    index.add(new IndexEntry(key.userKey(), currentOffset));
                    lastBlockOffset = currentOffset;
                    firstInBlock = false;
                }

                bloomFilter.set(Math.abs(java.util.Arrays.hashCode(key.userKey()) % 100000));

                int size = record.serializedSize();
                ByteBuffer buf = ByteBuffer.allocate(size);
                record.serialize(buf);
                out.write(buf.array(), 0, size);
                currentOffset += size;
            }

            long dataEnd = currentOffset;
            long indexOffset = dataEnd;
            out.writeInt(index.size());
            for (IndexEntry ie : index) {
                out.writeInt(ie.key.length);
                out.write(ie.key);
                out.writeLong(ie.offset);
            }
            long indexEnd = indexOffset + calculateIndexSize();
            long bloomOffset = indexEnd;

            byte[] bloomBytes = bloomFilter.toByteArray();
            out.writeInt(bloomBytes.length);
            out.write(bloomBytes);

            // Footer (Fixed 24 bytes)
            out.writeLong(indexOffset);
            out.writeLong(bloomOffset);
            out.writeLong(dataEnd);
            out.flush();
        }
    }

    private int calculateIndexSize() {
        int size = 4;
        for (IndexEntry ie : index) {
            size += 4 + ie.key.length + 8;
        }
        return size;
    }
}
