package main

import (
	"fmt"
	"os"

	"github.com/abhinav-upadhyay/brickdb/index"
)

func main() {
	db := index.NewBrick()
	db.Open("testdb", os.O_RDWR|os.O_CREATE)
	err := db.Store("key1", "val1", index.INSERT)
	if err != nil {
		panic(err)
	}
	val, err := db.Fetch("key1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("value: %s\n", val)
}
