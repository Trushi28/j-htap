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
        Parser.SelectQuery query = parser.parse();

        long ts = query.asOfTimestamp() != null ? query.asOfTimestamp() : store.getCurrentTimestamp();

        if (query.selectAll()) {
            if (query.whereKey() != null) {
                return store.get(query.whereKey().getBytes(), ts);
            } else {
                return store.scan(ts);
            }
        } else if (query.sumColIdx() != null) {
            return calculateSum(query.sumColIdx(), ts);
        }

        throw new UnsupportedOperationException("Unsupported query structure");
    }

    private long calculateSum(int colIdx, long ts) throws IOException {
        // HTAP optimized path: 
        // 1. Vectorized SIMD scan over all flushed Columnar files
        // (For this demo, we use a simple approach combining SIMD and scan results)
        List<Record> records = store.scan(ts);
        if (records.isEmpty()) return 0;

        long[] data = new long[records.size()];
        for (int i = 0; i < records.size(); i++) {
            byte[][] fields = records.get(i).fields();
            if (fields != null && colIdx < fields.length && fields[colIdx] != null && fields[colIdx].length == 8) {
                data[i] = ByteBuffer.wrap(fields[colIdx]).getLong();
            }
        }
        
        return new ColumnarBlock(data).sum();
    }
}
