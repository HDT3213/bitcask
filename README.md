# bitcask

a tiny kv database by  bitcask

## Example

```rust
let mut database = Database::open("testdata", Options::default()).unwrap();
let key = "hello".to_string();
let value = "world".to_string();
database.write(key.as_bytes(), value.as_bytes()).unwrap();
let result: Option<Bytes> = database.read(key.as_bytes()).unwrap();
```

## Benchmark

For the benchmark:
- every write is pushed to the operating system, but does not wait for the write to reach the disk.
- Keys are 16 bytes each. Value are 100 bytes each

```
random write 5590 ns/ops 178890.877 ops/s
random read 6147 ns/ops 162680.983 ops/s
load  202675.314 records/s
```

Enable mmap for read:
```
random read 1797 ns/ops 556483.027 ops/s
```