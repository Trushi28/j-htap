package io.jhtap.storage;

import io.jhtap.query.Lexer;
import io.jhtap.query.Parser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

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
        PriorityQueue<AggregateSource> queue = new PriorityQueue<>((left, right) -> {
            int keyCompare = Arrays.compare(left.currentRecord().key(), right.currentRecord().key());
            if (keyCompare != 0) {
                return keyCompare;
            }
            return Long.compare(right.currentRecord().timestamp(), left.currentRecord().timestamp());
        });
        List<StorageGroup> acquiredGroups = new ArrayList<>();

        try {
            AggregateSource memTableSource = AggregateSource.forMemTable(store.getMemTable().entries().iterator(), colIdx);
            if (memTableSource.hasCurrent()) {
                queue.add(memTableSource);
            }

            for (StorageGroup group : store.getStorageGroups()) {
                if (!group.acquire()) {
                    continue;
                }
                acquiredGroups.add(group);
                AggregateSource source = AggregateSource.forStorageGroup(group, colIdx);
                if (source.hasCurrent()) {
                    queue.add(source);
                }
            }

            while (!queue.isEmpty()) {
                AggregateSource source = queue.poll();
                Record record = source.currentRecord();
                byte[] currentKey = record.key();
                boolean resolved = false;

                while (true) {
                    if (!resolved && record.timestamp() <= ts) {
                        resolved = true;
                        if (record.type() == Record.RecordType.PUT) {
                            sum += source.currentColumnValue();
                        }
                    }

                    if (source.advance()) {
                        queue.add(source);
                    }

                    if (queue.isEmpty() || !Arrays.equals(queue.peek().currentRecord().key(), currentKey)) {
                        break;
                    }

                    source = queue.poll();
                    record = source.currentRecord();
                }
            }
        } finally {
            for (StorageGroup group : acquiredGroups) {
                group.release();
            }
        }

        return sum;
    }

    private static final class AggregateSource {
        private final Iterator<SSTableReader.IndexedRecord> indexedIterator;
        private final Iterator<Map.Entry<InternalKey, Record>> memTableIterator;
        private final ColumnarReader columnarReader;
        private final int colIdx;
        private Record currentRecord;
        private long currentColumnValue;

        private AggregateSource(
                Iterator<Map.Entry<InternalKey, Record>> memTableIterator,
                Iterator<SSTableReader.IndexedRecord> indexedIterator,
                ColumnarReader columnarReader,
                int colIdx
        ) {
            this.memTableIterator = memTableIterator;
            this.indexedIterator = indexedIterator;
            this.columnarReader = columnarReader;
            this.colIdx = colIdx;
        }

        static AggregateSource forMemTable(Iterator<Map.Entry<InternalKey, Record>> iterator, int colIdx) {
            AggregateSource source = new AggregateSource(iterator, null, null, colIdx);
            source.advance();
            return source;
        }

        static AggregateSource forStorageGroup(StorageGroup group, int colIdx) {
            AggregateSource source = new AggregateSource(
                    null,
                    group.getSstReader().allRecordsWithPositionsIterator(),
                    group.getColReader(),
                    colIdx
            );
            source.advance();
            return source;
        }

        boolean hasCurrent() {
            return currentRecord != null;
        }

        Record currentRecord() {
            return currentRecord;
        }

        long currentColumnValue() {
            return currentColumnValue;
        }

        boolean advance() {
            if (memTableIterator != null) {
                if (!memTableIterator.hasNext()) {
                    currentRecord = null;
                    currentColumnValue = 0L;
                    return false;
                }
                Map.Entry<InternalKey, Record> entry = memTableIterator.next();
                currentRecord = entry.getValue();
                currentColumnValue = decodeRecordField(currentRecord, colIdx);
                return true;
            }

            if (indexedIterator == null || !indexedIterator.hasNext()) {
                currentRecord = null;
                currentColumnValue = 0L;
                return false;
            }
            SSTableReader.IndexedRecord indexedRecord = indexedIterator.next();
            currentRecord = indexedRecord.record();
            currentColumnValue = currentRecord.type() == Record.RecordType.PUT
                    ? columnarReader.readLong(colIdx, indexedRecord.putRowIndex())
                    : 0L;
            return true;
        }

        private static long decodeRecordField(Record record, int colIdx) {
            byte[][] fields = record.fields();
            if (fields == null || colIdx >= fields.length || fields[colIdx] == null || fields[colIdx].length != 8) {
                return 0L;
            }
            return ByteBuffer.wrap(fields[colIdx]).getLong();
        }
    }
}
