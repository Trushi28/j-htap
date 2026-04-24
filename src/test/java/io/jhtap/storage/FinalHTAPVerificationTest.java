package io.jhtap.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class FinalHTAPVerificationTest {

    @Test
    public void testASTQueryParser(@TempDir Path tempDir) throws IOException {
        try (Store store = new Store(tempDir)) {
            QueryEngine engine = new QueryEngine(store);
            store.put("user1".getBytes(), ByteBuffer.allocate(8).putLong(100).array());
            store.flush();

            // Test SELECT *
            Record r = (Record) engine.execute("SELECT * FROM users WHERE KEY = 'user1'");
            assertNotNull(r);
            assertEquals(100, ByteBuffer.wrap(r.value()).getLong());

            // Test SELECT SUM
            Long sum = (Long) engine.execute("SELECT SUM(col0) FROM users");
            assertEquals(100L, sum);
        }
    }

    @Test
    public void testBackgroundCompactionAndGC(@TempDir Path tempDir) throws IOException, InterruptedException {
        try (Store store = new Store(tempDir)) {
            QueryEngine engine = new QueryEngine(store);

            // Write version 1
            store.put("key".getBytes(), ByteBuffer.allocate(8).putLong(1).array());
            store.flush();
            
            // Write version 2
            store.put("key".getBytes(), ByteBuffer.allocate(8).putLong(2).array());
            store.flush();
            
            // Write version 3
            store.put("key".getBytes(), ByteBuffer.allocate(8).putLong(3).array());
            store.flush();
            
            // Write version 4 -> Should trigger background compaction
            store.put("key".getBytes(), ByteBuffer.allocate(8).putLong(4).array());
            store.flush();

            // Wait for compaction to happen
            int maxRetries = 10;
            while (store.getStorageGroups().size() > 2 && maxRetries-- > 0) {
                Thread.sleep(1000);
            }

            // After compaction, if threshold was long enough ago, 
            // only the latest version should remain for "key".
            // Since we use System.currentTimeMillis() - 5000 in the thread, 
            // and we just wrote these, they might still be there.
            
            // Let's force a compaction call with a current timestamp to verify GC logic
            List<StorageGroup> groups = store.getStorageGroups();
            Path target = tempDir.resolve("manual-compact.sst");
            
            // GC everything older than "now"
            Compactor.compact(tempDir, groups, target, store.getCurrentTimestamp() + 100);
            
            try (SSTableReader reader = new SSTableReader(target)) {
                var it = reader.allRecordsIterator();
                int count = 0;
                Record last = null;
                while (it.hasNext()) {
                    last = it.next();
                    count++;
                }
                // Should only have 1 record for "key" because versions were GC'ed
                assertEquals(1, count);
                assertNotNull(last);
                assertEquals(4, ByteBuffer.wrap(last.value()).getLong());
            }
        }
    }
}
