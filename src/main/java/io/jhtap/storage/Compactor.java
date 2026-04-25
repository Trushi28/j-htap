package io.jhtap.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class Compactor {
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    public static void compact(Path rootDir, List<StorageGroup> groups, Path targetPath, long minRetainTimestamp) throws IOException {
        logger.info("Starting Streaming Compaction with GC (minRetain: {}) into {}", minRetainTimestamp, targetPath);
        
        // Acquire all groups first
        for (StorageGroup g : groups) {
            if (!g.acquire()) {
                throw new IOException("Failed to acquire storage group for compaction");
            }
        }

        try {
            PriorityQueue<SSTableIterator> pq = new PriorityQueue<>((it1, it2) -> {
                Record r1 = it1.peek();
                Record r2 = it2.peek();
                int cmp = Arrays.compare(r1.key(), r2.key());
                if (cmp != 0) return cmp;
                return Long.compare(r2.timestamp(), r1.timestamp());
            });

            Path colPath = rootDir.resolve(targetPath.getFileName().toString().replace(".sst", ".col"));
            try (StreamingSSTableWriter sstWriter = new StreamingSSTableWriter(targetPath);
                 StreamingColumnarWriter colWriter = new StreamingColumnarWriter(colPath)) {

                for (StorageGroup g : groups) {
                    SSTableIterator it = new SSTableIterator(g.getSstReader());
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
                        sstWriter.append(r);
                        colWriter.append(r);
                    }

                    if (it.hasNext()) pq.add(it);
                }
            }

        } finally {
            for (StorageGroup g : groups) {
                g.release();
            }
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
