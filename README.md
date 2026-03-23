# KV Store

A log-structured key-value storage engine built in Go, implementing the 
Bitcask model described in Designing Data-Intensive Applications (DDIA) Chapter 3.

## How It Works
- Writes are append-only to an on-disk log file
- An in-memory hash index maps keys to their latest values
- On restart, the index is rebuilt by replaying the log from disk
- Log compaction removes duplicate keys and reclaims disk space

## What This Demonstrates
- Log-structured storage design
- Crash recovery via log replay
- Atomic file swapping during compaction
- Separation of concerns between storage and indexing

## Caveat(s)
- All the keys must fit in the available RAM,

## Time Complexity
- Write: O(1), append to end of file and update map
- Read: O(1), pure map lookup
- Compaction: O(n), scan whole file once
- Startup/crash recovery: O(n), replay entire log to rebuild map

## Space Complexity
- Write: O(n), every key in RAM
- Read: O(n), rebuild full map
- Compaction: O(n), full map in memory
- Startup/crash recovery: O(n), rebuild full map

## Usage
go run .

## Next
Upgrading to SSTable-based storage with sorted segments and sparse indexing.

