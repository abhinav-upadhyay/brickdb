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

*Delete key*
```go
	err := db.Delete("key1")
	if err != nil {
		panic(err)
	}
```

*Update key*
```go
	err := db.Update("key1", "newval")
	if err != nil {
		panic(err)
	}
```
### Cautions to be taken when using with goroutines
The database uses the posix byte range locking to support concurrent reads and writes. The Brickdb object maintains state internally to operate which makes it difficult to share the same object with multiple goroutines as the state will get corrupted, possibly leading to a deadlock. The solution is to let each goroutine obtain its own handle to the database by calling `NewBrickdb()`.


### Using the shell
The shell can be built using `go build cmd/shell`
It takes the name of the database file as a parameter. If the db file exists it will open it, or it will create a new file.
The shell supports four commands:

**put**

It inserts a new record. For example
`> put key1 val1` 
will insert a key1 with value val1

**get**

It prints the value of the given key:
`> get key1`

**update**

It updates the value of an existing key:
`> update key1 val2`

**delete**

It deletes the record for the given key
`> delete key`

### Why the name Brickdb?
Because it is a database built for education and learning purposes, not tested or designed for industrial use case.
In the embedded systems community, often it happens that screwing around with the low level software on
the device can make the device unusable, which is referred to as `bricking the device` because the device
is no better than a piece of brick after that. In the same vein using an ametuer piece of software such as this
can brick your data, thus brickdb :)

### Future Work
Following are the list of things I've in mind to implement in near future
- **Support for multiplc columns** - right now only a single string value can be stored for a key, but can we extend the storage format to support storing multiple column values for a given key, the way it is in most of the RDBMS and columnar databases.
- **Support for more datatypes** - Right now key and values are expected to be strings, can we support more types natively?
- **Support for multiple tables per database** - Right now one database is a flat store of key-values, can we provide an abstraction layer and support multple tables
- **Query Language** - Once we have multiple tables support, it would be interesting to implement a SQL like query language
- **Multiple index formats** - Right now only hash indexing is implemented. Implementing btree and LSM indexing would be nice
