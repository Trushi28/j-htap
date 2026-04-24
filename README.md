# J-HTAP: Hybrid OLTP + OLAP Storage Engine

J-HTAP is a high-performance, embedded hybrid storage engine written in Java 25. It combines the speed of OLTP (Online Transactional Processing) point-lookups with the analytical power of OLAP (Online Analytical Processing) vectorized scans—all without the operational complexity of running separate systems.

Think of it as a personal database engine where you can simultaneously:
- **Insert and query individual records** at high speed (OLTP)
- **Run analytical aggregations** across millions of rows with SIMD acceleration (OLAP)
- **Travel back in time** to see the database state at any past timestamp
- **Survive failures** with write-ahead logs and automatic background compaction

## Why Hybrid Storage?

Traditional databases make a choice: optimize for fast single-row lookups (OLTP) *or* for bulk analytical scans (OLAP). This creates a false dilemma.

J-HTAP solves it with **Dual-Storage Architecture**:

When you commit data, J-HTAP writes it to **two formats simultaneously**:
- **Row Store (`.sst`)**: Optimized for fast lookups with sparse indexing and Bloom filters
- **Column Store (`.col`)**: Optimized for scans with memory-mapped access and SIMD processing

No copying. No synchronization overhead. Just two perfectly-optimized views of the same data.

## Key Features

### 🚀 Zero-Copy SIMD Execution (Java 25)
The OLAP engine uses the Java 25 **Vector API** and **Foreign Function & Memory API** for hardware-accelerated aggregations:
- Column data is memory-mapped directly from disk (`MappedByteBuffer` / `MemorySegment`)
- Data flows directly into SIMD lanes with zero garbage collection
- Aggregate queries like `SUM()` run at CPU cache speed, not object allocation speed

### 📊 Intelligent Dual-Storage Layout
- **Row Store (`.sst` files)**: Fast point lookups with sparse indexing, Bloom filters, and binary search
- **Column Store (`.col` files)**: Contiguous column-oriented blocks for cache-friendly scans
- No duplication overhead: both formats reference the same underlying data blocks

### ⏱️ Multi-Version Concurrency Control (MVCC)
- **Snapshot Isolation**: Readers never block writers. Every mutation creates a new record version
- **Time-Travel Queries**: Query the database state at any point in the past with `AS OF <timestamp>` syntax
- **Zero Coordination**: Concurrent readers, writers, and compactors don't wait for each other

### 🛡️ Production-Grade Reliability
Built to survive the "long run" with several stability guarantees:

- **Write-Ahead Log (WAL)**: Every transaction is durably logged before being acknowledged
- **Background Compaction**: A dedicated daemon monitors fragmentation and automatically merges files
- **Streaming Compaction**: Processes large datasets block-by-block, preventing Out-Of-Memory errors even with terabytes of data
- **MVCC Garbage Collection**: Old record versions are safely dropped during compaction
- **Atomic Reference Counting**: Files are only physically deleted when the last concurrent reader releases them

### 🔍 AST-Based SQL Parser
Beyond simple regex matching, J-HTAP implements a proper lexer and parser:

Supported queries:
```sql
SELECT * FROM table WHERE key = 'foo' AS OF '2024-01-15 10:30:00'
SELECT SUM(column) FROM table AS OF '2024-01-15 10:30:00'
```

## Technology Stack

- **Java 25** (requires `--add-modules jdk.incubator.vector`)
- **JUnit 5**: Comprehensive test coverage
- **SLF4J + Logback**: Production-grade logging
- **Maven**: Build automation

## Getting Started

### Prerequisites
- Java 25 with incubating modules enabled
- Maven 3.8+

### Build and Run Tests

J-HTAP uses incubating features, so you must enable them:

```bash
# Compile and run the full test suite
mvn clean test
```

The `pom.xml` is pre-configured with the necessary JVM flags (`--add-modules jdk.incubator.vector`).

### Core Tests

The test suite validates the entire system end-to-end:

- **`LSMTreeCoreTest`**: Validates MemTable mutations, WAL recovery, and MVCC point-in-time reads
- **`StoreTest`**: Validates the full flush lifecycle, storage group creation, background compaction, and reference-counted garbage collection
- **`FinalHTAPVerificationTest`**: End-to-end validation of the AST parser, SIMD queries, and time-travel execution

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     SQL Query (typed)                        │
└────────────────────────┬─────────────────────────────────────┘
                         │
                    ┌────v──────┐
                    │ AST Parser │
                    └────┬───────┘
                         │
              ┌──────────┴─────────────┐
              │                        │
         (OLTP)                   (OLAP)
              │                        │
    ┌─────────v──────────┐   ┌────────v────────────┐
    │ Point Lookup Scan  │   │ Vectorized Scan     │
    │ (Binary Search)    │   │ (Java 25 Vector API)│
    └─────────┬──────────┘   └────────┬────────────┘
              │                        │
    ┌─────────v──────────┐   ┌────────v────────────┐
    │  .sst Row Store    │   │ .col Column Store   │
    │  (Sparse Index +   │   │ (Memory-Mapped)     │
    │   Bloom Filters)   │   │                     │
    └────────────────────┘   └─────────────────────┘
              │                        │
              └──────────┬─────────────┘
                         │
        ┌────────────────v─────────────────┐
        │      MemTable (In-Memory)        │
        │   with Write-Ahead Log (WAL)     │
        │   and MVCC Versioning            │
        └────────────────┬─────────────────┘
                         │
        ┌────────────────v─────────────────┐
        │  Background Compaction Daemon    │
        │  (Automatic, Non-Blocking)       │
        └────────────────────────────────────┘
```

## How It Works: A Real Example

**Scenario**: You insert 1M customer records, then query the sum of their purchase amounts.

1. **Write Phase** (OLTP):
   - Client issues: `INSERT INTO customers (id, name, purchase_amount) VALUES (...)`
   - Transaction is appended to WAL (durable)
   - Record is inserted into MemTable with MVCC metadata
   - Acknowledgement returned to client

2. **Flush Phase**:
   - MemTable reaches size threshold
   - **Row Store** writer generates `.sst` files with sparse indexing for fast point lookups
   - **Column Store** writer generates `.col` files with customer IDs grouped, amounts grouped, etc.
   - Both written atomically

3. **Query Phase** (OLAP):
   - Client issues: `SELECT SUM(purchase_amount) FROM customers`
   - Parser generates execution plan
   - OLAP engine loads `.col` file, memory-maps the purchase_amount column
   - Java 25 Vector API processes multiple aggregates in parallel at CPU cache speed
   - Result returned in milliseconds

4. **Time-Travel Phase**:
   - Client issues: `SELECT * FROM customers WHERE id = '123' AS OF '2024-01-15 10:00:00'`
   - MVCC layer finds the correct version of the record at that timestamp
   - Returned instantly (all versions are readily available)

5. **Cleanup Phase** (Background):
   - Compaction daemon detects old versions of records
   - Merges `.sst` and `.col` files while streaming data block-by-block
   - Garbage collects old MVCC versions no longer needed by any reader
   - Reference counting ensures a slow analytical query doesn't prevent cleanup

## Use Cases

✅ **Time-Series Analytics**: Query billions of sensor readings with time-travel
✅ **Financial Data**: OLTP for trades + OLAP for portfolio analysis
✅ **Compliance & Audit**: Query the exact state at any point in time
✅ **ETL Pipelines**: Embedded database for transformation and aggregation
✅ **Multi-Tenant SaaS**: One instance per customer with isolated data

## Performance Characteristics

| Operation | Latency | Notes |
|-----------|---------|-------|
| Point Lookup | µs | Binary search + Bloom filter |
| Range Scan | ms | From `.sst` or `.col` |
| SIMD Aggregation | **10-100x faster** | OLAP with Vector API vs naive iteration |
| Time-Travel Read | µs | Direct version lookup, no replay |
| Write | µs | In-memory MemTable, then async WAL |

## Limitations & Scope

This is an **educational showcase** of advanced database engineering concepts:

- Single-process embedded engine (not a distributed database)
- Not a drop-in replacement for PostgreSQL or DuckDB
- Limited SQL syntax (no JOINs, GROUP BY, ORDER BY yet)
- Designed to teach, not for production use

That said, the MVCC, compaction, and SIMD patterns here are used in real-world systems like TiDB, DuckDB, and Databricks.

## Project Structure

```
src/main/java/io/jhtap/
├── storage/
│   ├── Store.java              # Main entry point
│   ├── MemTable.java           # In-memory write buffer
│   ├── WriteAheadLog.java      # Durability
│   ├── SSTableWriter.java      # Row store generation
│   ├── SSTableReader.java      # Row store queries
│   ├── ColumnarBlock.java      # Column-oriented data
│   ├── ColumnarReader.java     # Column store queries
│   ├── StorageGroup.java       # Set of .sst/.col files
│   └── Compactor.java          # Background merge
├── sql/
│   ├── Lexer.java              # Tokenizer
│   ├── Parser.java             # AST builder
│   └── QueryPlan.java          # Execution instructions
└── Main.java
```

## Further Learning

If you want to understand hybrid storage at a deeper level, read these foundational papers:

- [The F1 Lightning Architecture](https://research.google/pubs/f1-lightning-fast-distributed-sql-for-data-at-scale/) – Google's HTAP design
- [Bw-Tree: A B-Tree for New Hardware](https://www.microsoft.com/en-us/research/publication/bw-tree-a-b-tree-for-new-hardware/) – Cache-optimized indexing
- [Java Vector API](https://openjdk.org/jeps/460) – Hardware acceleration in Java

## License

Educational and demonstration purposes.
