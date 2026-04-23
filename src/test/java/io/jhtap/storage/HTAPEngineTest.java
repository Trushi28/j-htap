package io.jhtap.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class HTAPEngineTest {

    @Test
    public void testHTAPFullFlow(@TempDir Path tempDir) throws IOException {
        try (Store store = new Store(tempDir)) {
            QueryEngine engine = new QueryEngine(store);

            // 1. OLTP writes
            for (int i = 1; i <= 100; i++) {
                byte[] key = ("user" + i).getBytes();
                byte[] val = ByteBuffer.allocate(8).putLong(i * 10).array();
                store.put(key, val);
            }
            store.flush();

            // 2. Point Lookups
            Record r = (Record) engine.execute("SELECT * FROM users WHERE key = 'user50'");
            assertNotNull(r);
            assertEquals(500, ByteBuffer.wrap(r.value()).getLong());

            // 3. Time-Travel
            long t1 = store.getCurrentTimestamp() - 1; // Snapshot BEFORE update
            
            // Update user50
            store.put("user50".getBytes(), ByteBuffer.allocate(8).putLong(999).array());
            
            // Current value should be 999
            Record rCurrent = (Record) engine.execute("SELECT * FROM users WHERE key = 'user50'");
            assertEquals(999, ByteBuffer.wrap(rCurrent.value()).getLong());
            
            // Historical value should be 500
            Record rHistory = (Record) engine.execute("SELECT * FROM users WHERE key = 'user50' AS OF " + t1);
            assertNotNull(rHistory);
            assertEquals(500, ByteBuffer.wrap(rHistory.value()).getLong());

            // 4. OLAP Vectorized Query
            // Original sum: 10+20+...+1000 = 50500
            // User50 was 500. Updated to 999.
            // New sum: 50500 - 500 + 999 = 50999
            Long sum = (Long) engine.execute("SELECT SUM(col0) FROM users");
            assertEquals(50999L, sum);
            
            // Historical Sum (at t1) should be 50500
            Long historicalSum = (Long) engine.execute("SELECT SUM(col0) FROM users AS OF " + t1);
            assertEquals(50500L, historicalSum);
        }
    }
}
