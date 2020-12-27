# brickdb
A simple concurrent hash index based key-value store in go. It is very much inspired from the DBM library which originated in the AT&T UNIX, but this implementation provides concurrency safe access methods using byte-range locking facilities.


### Features and Limitations
- **Concurrent** - It uses byte-range locking to allow multiple readers and writers at the same time to the database
- **Embeddable** - Instead of a stand-alone process, this is an embeddable database library with persistence to disk
- **Supported index types** - Currently only hash based indexing is supported. There are two hash index implementations, one is a static hash table in which the index is initialized with a fixed size. As more and more keys are stored, the table will get slower due to increased collision. The second implementation is a dynamic hash index using linear hashing, it dynamically grows the table as collisions increase. But it can get slow if there are two many processes/threads writing at the same time due to increased lock contention.
- **Ordered Access** - Since the only supported index format is hash table based, there is no mechanism to access the keys in sorted order.
- **Query Engine** - There is no query engine implemented yet. There are functions available in the library to query data though.

### Dependencies
	xxhash: `go get github.com/OneOfOne/xxhash`
	sys/unix: `go get golang.org/x/sys/unix`

### Building the shell
`go build ./cmd/shell`

### Usage

*Create/Open database*
(following will create the database with given name if one doesn't exist already, or open the existing one)
```go
	// second parameter is index type, two types are available:
	// index.HashIndexType which is a static hash table and
	// second is index.LinearHashIndex which is a dynamic hash table using linear hashing
	db := brickdb.New(name, index.LinearHashIndexType)
	err := db.Open()
```

*Insert record*
```go
	err := db.Store("key1", "val1", brickdb.Insert)
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
	err := db.Store("key1", "newval", brickdb.Update)
	if err != nil {
		panic(err)
	}
```

*Fetch all records*
```go
	valuesMap, err := db.FetchAll() //returns a map[string]string
	if err != nil {
		panic(err)
	}
	for key, val := range valuesMap {
		fmt.Printf("key: %s, value: %s\n", key, val)
	}

```
### Cautions to be taken when using with goroutines
- The database uses the posix byte range locking to support concurrent reads and writes. The Brickdb object maintains state internally to operate which makes it difficult to share the same object with multiple goroutines as the state will get corrupted, possibly leading to a deadlock. The solution is to let each goroutine obtain its own handle to the database by calling `NewBrickdb()`.
- When using the static hash index (`index.HashIndexType`), the reads will get slower over time as number of keys stored increase.
- When using the linear hash index (`index.LinearHashIndexType`), even though it will grow the hash table to reduce collisions, it comes at the cost of extra locking. Every read/write/delete needs to do extra locking to ensure that the index is not being grown while the read/write is going because that can cause corruption of data. Therefore, this will get slower if there are too many processes/goroutines writing data at the same time.


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

**get all key values**

`> get *`

**update**

It updates the value of an existing key:
`> update key1 val2`

**delete**

It deletes the record for the given key
`> delete key`

### Why the name Brickdb?
Brickdb may be a reference to the verb [brick](https://en.wikipedia.org/wiki/Brick_(electronics)) which means corrupting something to the point of being euqivalent to a brick, or it may be a reference to the character Brick from the movie Anchorman. In other words the database does not gurantee any sort of usefulness :-)

### Future Work
Following are the list of things I've in mind to implement in near future
- ~~**Linear Hashing** - Replace the fixed size hash index with a [linear hash index](https://en.wikipedia.org/wiki/Linear_hashing)~~ (Done)
- **Support for multiple columns** - right now only a single string value can be stored for a key, but can we extend the storage format to support storing multiple column values for a given key, the way it is in most of the RDBMS and columnar databases.
- **Support for more datatypes** - Right now key and values are expected to be strings, can we support more types natively?
- **Support for multiple tables per database** - Right now one database is a flat store of key-values, can we provide an abstraction layer and support multple tables
- **Query Language** - Once we have multiple tables support, it would be interesting to implement a SQL like query language
- **Multiple index formats** - Right now only hash indexing is implemented. Implementing btree and LSM indexing would be nice
- **Reduce overhead of locking** - The locking mechanism currently uses the `fcntl` system call, which has high overhead. Can we replace it with a lightweight mechanism?
- **Caching** - Can caching be implemented while keeping the database concurrent? Right now it is using the `readv`/`writev` system calls which are unbufferred and that works well for concurrency. Adding caching would require making sure that cache is valid and some other thread/process has not modified the cached data.
One possibility is to use shared memory.
