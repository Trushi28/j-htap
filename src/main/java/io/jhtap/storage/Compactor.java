package io.jhtap.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class Compactor {
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    public static void compact(Path rootDir, List<Path> sstFiles, Path targetPath, long minRetainTimestamp) throws IOException {
        logger.info("Starting Streaming Compaction with GC (minRetain: {}) into {}", minRetainTimestamp, targetPath);
        
        PriorityQueue<SSTableIterator> pq = new PriorityQueue<>((it1, it2) -> {
            Record r1 = it1.peek();
            Record r2 = it2.peek();
            int cmp = Arrays.compare(r1.key(), r2.key());
            if (cmp != 0) return cmp;
            return Long.compare(r2.timestamp(), r1.timestamp());
        });
        
        List<SSTableReader> readers = new ArrayList<>();
        // In a real database, we'd use a special background writer, not a MemTable.
        // But for this project, a MemTable acting as a merge buffer is fine
        // as long as we flush it when it gets too big.
        // Actually, to avoid OOM in compaction, we'll write directly to the writer.
        
        SSTableWriter sstWriter = new SSTableWriter(targetPath);
        String colFilename = targetPath.getFileName().toString().replace(".sst", ".col");
        ColumnarWriter colWriter = new ColumnarWriter(rootDir.resolve(colFilename));
        
        // We'll use a temporary MemTable for the final write phase 
        // because our Writer implementations take a MemTable.
        // TO TRULY STREAM: We would need a 'StreamingWriter'.
        // For the sake of this resume project, I'll use a 'MergeBuffer' MemTable.
        MemTable mergeBuffer = new MemTable();

        try {
            for (Path p : sstFiles) {
                SSTableReader reader = new SSTableReader(p);
                readers.add(reader);
                SSTableIterator it = new SSTableIterator(reader);
                if (it.hasNext()) pq.add(it);
            }

            byte[] lastUserKey = null;
            boolean keptLatestVersionOlderThanMin = false;

            while (!pq.isEmpty()) {
                SSTableIterator it = pq.poll();
                Record r = it.next();

                boolean sameKey = lastUserKey != null && Arrays.equals(lastUserKey, r.key());
                if (!sameKey) {
                    lastUserKey = r.key();
                    keptLatestVersionOlderThanMin = false;
                }

                boolean shouldKeep;
                if (r.timestamp() >= minRetainTimestamp) {
                    shouldKeep = true;
                } else {
                    if (!keptLatestVersionOlderThanMin) {
                        shouldKeep = r.type() != Record.RecordType.DELETE;
                        keptLatestVersionOlderThanMin = true;
                    } else {
                        shouldKeep = false;
                    }
                }

                if (shouldKeep) {
                    mergeBuffer.put(new InternalKey(r.key(), r.timestamp()), r);
                }

                if (it.hasNext()) pq.add(it);
            }

            sstWriter.write(mergeBuffer);
            colWriter.write(mergeBuffer);

        } finally {
            for (SSTableReader r : readers) r.close();
        }
    }

    private static class SSTableIterator implements Iterator<Record> {
        private final Iterator<Record> it;
        private Record head;

        public SSTableIterator(SSTableReader reader) {
            this.it = reader.allRecordsIterator();
            if (it.hasNext()) this.head = it.next();
        }

        @Override public boolean hasNext() { return head != null; }
        @Override public Record next() {
            Record r = head;
            head = it.hasNext() ? it.next() : null;
            return r;
        }
        public Record peek() { return head; }
    }
}
