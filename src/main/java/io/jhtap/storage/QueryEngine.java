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

        // Analytical queries must respect MVCC visibility rules. A raw scan across
        // every columnar file double-counts superseded versions, so aggregate over
        // the snapshot-visible record set first and decode the requested column.
        for (Record record : store.scan(ts)) {
            byte[][] fields = record.fields();
            if (fields == null || colIdx >= fields.length || fields[colIdx] == null || fields[colIdx].length != 8) {
                continue;
            }
            sum += ByteBuffer.wrap(fields[colIdx]).getLong();
        }

        return sum;
    }
}
