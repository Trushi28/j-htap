package io.jhtap.storage;

import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {
    private final ConcurrentSkipListMap<InternalKey, Record> map;
    private final AtomicLong size;

    public MemTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong(0);
    }

    public void put(InternalKey key, Record record) {
        map.put(key, record);
        size.addAndGet(key.userKey().length + 8 + record.serializedSize());
    }

    public Record get(byte[] userKey, long snapshotTimestamp) {
        // Find the latest version of the key with timestamp <= snapshotTimestamp
        InternalKey searchKey = new InternalKey(userKey, snapshotTimestamp);
        var subMap = map.tailMap(searchKey);
        var entry = subMap.firstEntry();
        if (entry != null && Arrays.equals(entry.getKey().userKey(), userKey)) {
            return entry.getValue();
        }
        return null;
    }

    public long getSize() {
        return size.get();
    }

    public Iterable<java.util.Map.Entry<InternalKey, Record>> entries() {
        return map.entrySet();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public void clear() {
        map.clear();
        size.set(0);
    }
}
