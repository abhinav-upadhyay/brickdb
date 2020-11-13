package main

import (
	"fmt"
	"os"

	"github.com/abhinav-upadhyay/brickdb/index"
	brickdb "github.com/abhinav-upadhyay/brickdb/pkg/brickdb"
)

func main() {
	db := brickdb.New("testdb")
	err := db.Create(index.HashIndexType)
	if err != nil {
		panic(err)
	}
	db.Open(os.O_RDWR)
	err = db.Store("key1", "val1", brickdb.Insert)
	if err != nil {
		panic(err)
	}
	err = db.Store("key2", "val2", brickdb.Insert)
	if err != nil {
		panic(err)
	}
	val, err := db.Fetch("key1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("value: %s\n", val)
	val, err = db.Fetch("key2")
	if err != nil {
		panic(err)
	}
	fmt.Printf("value: %s\n", val)
}
