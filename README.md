# KV Store

A log-structured key-value storage engine built in Go, implementing concepts 
from Designing Data-Intensive Applications (DDIA) Chapter 3.

## Evolution
This engine was built in two stages:

### Stage 1: Bitcask Model
- Append-only log file on disk
- Full hash index in memory (one entry per key)
- Crash recovery by replaying the log
- Log compaction via atomic file swap

### Stage 2: SSTable-Based LSM-Tree
- Writes go to an in-memory memtable first
- Memtable flushes to a sorted SSTable file when full
- Reads check memtable first, then SSTables newest to oldest
- SSTable files are immutable
- sstableCount restored on restart via directory scan

## What This Demonstrates
- Log-structured storage design
- Memtable + SSTable architecture
- Crash recovery for both hash index and SSTable segments
- Immutable file design
- Sequential writes for performance

## Known Limitations
- Memtable is lost on crash if not flushed (Write ahead Log not yet implemented)
- No sparse index yet
- No SSTable compaction yet

## Next
- Write-ahead log (WAL) for memtable durability
- Sparse index to avoid full SSTable scans
- SSTable compaction

## Reference
- Designing Data-Intensive Applications, Chapter 3 — Storage and Retrieval

