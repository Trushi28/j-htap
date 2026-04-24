package io.jhtap.storage;

import io.jhtap.query.Lexer;
import io.jhtap.query.Parser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryEngine {
    private final Store store;

    public QueryEngine(Store store) {
        this.store = store;
    }

    public Object execute(String sql) throws IOException {
        Lexer lexer = new Lexer(sql);
        Parser parser = new Parser(lexer.tokenize());
        Object query = parser.parse();

        if (query instanceof Parser.SelectQuery select) {
            long ts = select.asOfTimestamp() != null ? select.asOfTimestamp() : store.getCurrentTimestamp();

            if (select.selectAll()) {
                if (select.whereKey() != null) {
                    Record r = store.get(select.whereKey().getBytes(), ts);
                    if (r != null && r.type() == Record.RecordType.DELETE) return null;
                    return r;
                } else {
                    return store.scan(ts);
                }
            } else if (select.sumColIdx() != null) {
                return calculateSum(select.sumColIdx(), ts);
            }
        } else if (query instanceof Parser.InsertQuery insert) {
            byte[][] fields = new byte[insert.values().size()][];
            for (int i = 0; i < insert.values().size(); i++) {
                fields[i] = ByteBuffer.allocate(8).putLong(insert.values().get(i)).array();
            }
            store.put(insert.key().getBytes(), fields);
            return "OK";
        }

        throw new UnsupportedOperationException("Unsupported query structure");
    }

    private long calculateSum(int colIdx, long ts) throws IOException {
        long sum = 0;
        
        // HTAP Optimized Path:
        // 1. Vectorized SIMD scan over all flushed Columnar (.col) files.
        for (StorageGroup group : store.getStorageGroups()) {
            if (group.acquire()) {
                try {
                    sum += group.getColReader().sum(colIdx);
                } finally {
                    group.release();
                }
            }
        }

        // 2. Add records still in MemTable (not yet in columnar format)
        for (var entry : store.getMemTable().entries()) {
            Record r = entry.getValue();
            if (r.timestamp() <= ts && r.type() == Record.RecordType.PUT) {
                byte[][] fields = r.fields();
                if (fields != null && colIdx < fields.length && fields[colIdx] != null && fields[colIdx].length == 8) {
                    sum += ByteBuffer.wrap(fields[colIdx]).getLong();
                }
            }
        }
        
        // Note: In a production system, we'd handle deduplication between 
        // Columnar files and MemTable using a validity bitmap or by only 
        // summing un-compacted records once.
        return sum;
    }
}
