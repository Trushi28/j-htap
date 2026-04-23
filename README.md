# J-HTAP: Hybrid OLTP + OLAP Storage Engine

J-HTAP is a high-performance, embedded hybrid storage engine written in Java 25. It demonstrates advanced distributed systems and database engineering concepts by combining fast OLTP point-lookups with true hardware-accelerated OLAP vectorized scans. 

It achieves this by maintaining a true **Dual-Storage Layout**, mirroring the architecture of modern HTAP (Hybrid Transactional/Analytical Processing) systems.

## Key Features

### 1. True Dual-Storage Architecture
When data is flushed from memory, J-HTAP simultaneously generates two distinct file formats:
*   **Row Store (`.sst`):** Immutable SSTables optimized for fast point lookups (OLTP). Features sparse indexing and Bloom Filters to minimize disk I/O.
*   **Column Store (`.col`):** Data is written contiguously by column. This layout maximizes CPU cache utilization and enables high-speed analytical scans (OLAP).

### 2. Zero-Copy SIMD Execution (Java 25)
The OLAP engine leverages the **Java 25 Incubator Vector API (`jdk.incubator.vector`)** and the **Foreign Function & Memory API**. 
*   `.col` files are mapped directly into memory using `MappedByteBuffer` and `MemorySegment`.
*   Data is fed directly into SIMD (Single Instruction, Multiple Data) lanes for hardware-accelerated aggregations (e.g., `SUM()`), resulting in zero object allocation during scans.

### 3. Multi-Version Concurrency Control (MVCC) & Time-Travel
*   **Snapshot Isolation:** Readers never block writers. Every mutation creates a new version of the record.
*   **Time-Travel Queries:** The SQL engine supports `AS OF <timestamp>` syntax, allowing you to query the exact state of the database at any point in the past.

### 4. Production-Grade Reliability
Built to survive the "long run", J-HTAP includes several stability mechanisms:
*   **Write-Ahead Log (WAL):** Ensures absolute durability. Transactions are appended to the WAL before being acknowledged.
*   **Background Compaction Daemon:** A dedicated background thread monitors storage fragmentation. When thresholds are met, it automatically merges `.sst` and `.col` files.
*   **Streaming Compaction:** The compactor streams records block-by-block using iterators, preventing Out-Of-Memory (OOM) errors even when compacting terabytes of data.
*   **MVCC Garbage Collection:** During compaction, the engine identifies a retention threshold and safely drops old, shadowed versions and tombstones (deleted records).
*   **Atomic Reference Counting:** Concurrent reads and compactions are synchronized. A compacted file is only physically deleted from the disk when the last active reader (e.g., a slow analytical query) releases its reference.

### 5. AST SQL Parser
Moves beyond simple regex matching by implementing a custom Lexer and Abstract Syntax Tree (AST) Parser to translate string queries into execution plans.
Supported syntax includes:
*   `SELECT * FROM table WHERE key = '...' [AS OF timestamp]`
*   `SELECT SUM(colX) FROM table [AS OF timestamp]`

## Technology Stack
*   **Java 25** (requires `--enable-preview` or specific incubator modules depending on the JDK build)
*   **Maven**
*   **JUnit 5** (Testing)
*   **SLF4J / Logback** (Logging)

## How to Build and Run Tests

Because J-HTAP uses incubating features (Vector API), you must run it with the appropriate JVM flags. The `pom.xml` is already configured for this.

```bash
# Compile and run the test suite
mvn clean test
```

### Core Tests Included:
*   `LSMTreeCoreTest`: Validates MemTable mutations, WAL recovery, and MVCC point-in-time reads.
*   `StoreTest`: Validates the full flush lifecycle, storage group creation, background compaction, and reference-counted garbage collection.
*   `FinalHTAPVerificationTest`: Validates the AST Parser, SIMD analytical queries, and end-to-end time-travel execution.

## Architecture Diagram (Logical)

```text
[ SQL Query ] -> [ AST Parser ] -> [ Query Engine ]
                                        |
                                        v
[ Write Path ]                     [ Read Path ]
      |                                 |
      v                                 v
[ Write-Ahead Log ]                (OLTP) -> [ .sst Row Store (Indexed + Bloom) ]
      |                                 |
      v                                 v
[ MemTable (MVCC) ] --(Flush)-->   (OLAP) -> [ .col Column Store (Memory Mapped) ]
                                        |
                                        v
                                 [ Java 25 Vector API (SIMD) ]
```

## Disclaimer
This is a demonstration project intended to showcase advanced software engineering concepts. While it implements production-grade stability patterns, it is an embedded educational engine and not intended as a drop-in replacement for production databases like PostgreSQL or DuckDB.
