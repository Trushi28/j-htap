package io.jhtap.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SSTableReader implements AutoCloseable {
    private final FileChannel channel;
    private final BitSet bloomFilter;
    private final long indexOffset;
    private final long bloomOffset;
    private final long dataEnd;

    public record IndexEntry(byte[] key, long offset) {}
    public record IndexedRecord(Record record, int putRowIndex) {}
    private final List<IndexEntry> index = new ArrayList<>();

    public SSTableReader(Path path) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
        this.channel = raf.getChannel();
        long size = channel.size();

        // Read Footer
        ByteBuffer footer = ByteBuffer.allocate(24);
        channel.read(footer, size - 24);
        footer.flip();
        this.indexOffset = footer.getLong();
        this.bloomOffset = footer.getLong();
        this.dataEnd = footer.getLong();

        // Map Index
        MappedByteBuffer indexBuffer = channel.map(FileChannel.MapMode.READ_ONLY, indexOffset, bloomOffset - indexOffset);
        loadIndex(indexBuffer);

        // Map & Load Bloom Filter
        MappedByteBuffer bloomBuffer = channel.map(FileChannel.MapMode.READ_ONLY, bloomOffset, size - 24 - bloomOffset);
        int bloomLen = bloomBuffer.getInt();
        byte[] bloomBytes = new byte[bloomLen];
        bloomBuffer.get(bloomBytes);
        this.bloomFilter = BitSet.valueOf(bloomBytes);
    }

    private void loadIndex(ByteBuffer indexBuffer) {
        int size = indexBuffer.getInt();
        for (int i = 0; i < size; i++) {
            int keyLen = indexBuffer.getInt();
            byte[] key = new byte[keyLen];
            indexBuffer.get(key);
            long offset = indexBuffer.getLong();
            index.add(new IndexEntry(key, offset));
        }
    }

    public Record get(byte[] userKey, long snapshotTimestamp) throws IOException {
        // Check Bloom Filter
        if (!bloomFilter.get(Math.abs(java.util.Arrays.hashCode(userKey) % 100000))) {
            return null;
        }

        // Binary Search Index to find the block
        int low = 0;
        int high = index.size() - 1;
        int blockIdx = -1;

        while (low <= high) {
            int mid = (low + high) / 2;
            int cmp = java.util.Arrays.compare(index.get(mid).key(), userKey);
            if (cmp <= 0) {
                blockIdx = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (blockIdx == -1) return null;

        // Scan the block
        long start = index.get(blockIdx).offset();
        long end = (blockIdx + 1 < index.size()) ? index.get(blockIdx + 1).offset() : dataEnd;

        if (end <= start) return null;

        ByteBuffer block = ByteBuffer.allocate((int)(end - start));
        channel.read(block, start);
        block.flip();

        Record best = null;
        while (block.remaining() >= 4) { // Min key length
            try {
                int pos = block.position();
                Record r = Record.deserialize(block);
                int cmp = java.util.Arrays.compare(r.key(), userKey);
                if (cmp == 0) {
                    if (r.timestamp() <= snapshotTimestamp) {
                        if (best == null || r.timestamp() > best.timestamp()) {
                            best = r;
                        }
                    }
                } else if (cmp > 0) {
                    break; // Sorted
                }
            } catch (Exception e) {
                break;
            }
        }
        return best;
    }

    public Iterator<Record> allRecordsIterator() {
        return new Iterator<Record>() {
            private long pos = 0;
            private Record nextRec = null;

            @Override
            public boolean hasNext() {
                if (nextRec != null) return true;
                if (pos >= indexOffset) return false;
                try {
                    ByteBuffer buf = ByteBuffer.allocate(1024 * 16); // 16KB buffer
                    int read = channel.read(buf, pos);
                    if (read <= 0) return false;
                    buf.flip();
                    nextRec = Record.deserialize(buf);
                    pos += (buf.position()); // Move by serialized size
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) throw new NoSuchElementException();
                Record r = nextRec;
                nextRec = null;
                return r;
            }
        };
    }

    public Iterator<IndexedRecord> allRecordsWithPositionsIterator() {
        return new Iterator<>() {
            private final Iterator<Record> delegate = allRecordsIterator();
            private int nextPutRowIndex = 0;

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public IndexedRecord next() {
                Record record = delegate.next();
                int putRowIndex = record.type() == Record.RecordType.PUT ? nextPutRowIndex++ : -1;
                return new IndexedRecord(record, putRowIndex);
            }
        };
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
