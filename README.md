# brickdb
A simple concurrent hash index based key-value store in go.

### Features and Limitations
- Concurrent - It uses byte-range locking to allow multiple readers and writers at the same time to the database
- Embeddable - Instead of a stand-alone process, this is an embeddable database library with persistence to disk
- Hash index based - It uses a hash table to index the data. Currently it uses a fixed size static table, which
means with as the number of keys grow, access will get slower. A dynamic hash table can make it a constant time read
and write (TODO)
- Query Engine - There is no query engine implemented yet. There are functions available in the library to query data though.

### Usage

*Create database*
```go
	db := index.NewBrick()
	db.Open("testdb", os.O_RDWR|os.O_CREATE)
```

*Insert record*
```go
	err := db.Store("key1", "val1", index.INSERT)
	if err != nil {
		panic(err)
	}
```

*Query by key*
```go
	val, err := db.Fetch("key1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("value: %s\n", val)
```

### Why the name Brickdb?
Because it is a database built for education purpose, not tested or designed for industrial use case.
In the embedded systems community, often it happens that screwing around with the low level software on
the device can make the device unusable, which is referred to as `bricking the device' because the device
is no better than a piece of brick after that. In the same vein using an ametuer piece of software such as this
can brick your data, thus brickdb :) 
