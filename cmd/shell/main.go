/*-
 * Copyright (c) 2020 Abhinav Upadhyay
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/abhinav-upadhyay/brickdb/index"
)

func main() {
	dbName := os.Args[1]
	db := openDB(dbName)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf(">")
		scanner.Scan()
		cmd := scanner.Text()
		doExit := executeCmd(db, cmd)
		if doExit {
			break
		}
	}
}

func openDB(name string) *index.Brickdb {
	finfo, err := os.Stat(name)
	exists := false
	if os.IsNotExist(err) {
		exists = false
	}
	if exists {
		exists = !finfo.IsDir()
	}
	db := index.NewBrick()
	if exists {
		db.Open(name, os.O_RDWR)
	} else {
		db.Open(name, os.O_RDWR|os.O_CREATE)
	}
	return db
}

func executeCmd(db *index.Brickdb, cmdArgs string) bool {
	args := strings.Split(cmdArgs, " ")
	cmd := args[0]
	switch cmd {
	case "put":
		if len(args) != 3 {
			fmt.Printf("Invalid syntax for put: <put key value>\n")
			return false
		}
		key := args[1]
		val := args[2]
		err := db.Store(key, val, index.INSERT)
		if err != nil {
			fmt.Printf("Failed to insert key %s with value %s due to error %v\n", key, val, err)
			return false
		}
	case "update":
		if len(args) != 3 {
			fmt.Printf("Invalid syntax for update: <update key value>")
			return false
		}
		key := args[1]
		val := args[2]
		err := db.Store(key, val, index.STORE)
		if err != nil {
			fmt.Printf("Failed to update key %s with value %s due to error %v\n", key, val, err)
			return false
		}
	case "get":
		if len(args) != 2 {
			fmt.Printf("Invalid syntax for get: <get key>\n")
			return false
		}
		key := args[1]
		val, err := db.Fetch(key)
		if err != nil {
			fmt.Printf("Failed to get key %s, due to error %v\n", key, err)
			return false
		}
		if val == "" {
			fmt.Printf("Key %s not found\n", key)
			return false
		}
		fmt.Printf("%s\n", val)
		return false
	case "delete":
		if len(args) != 2 {
			fmt.Printf("Invalid syntax for delete: <delete key>\n")
			return false
		}
		key := args[1]
		err := db.Delete(key)
		if err != nil {
			fmt.Printf("Failed to delete key %s with error %v\n", key, err)
			return false
		}
		return false
	case "quit":
		err := db.Close()
		if err != nil {
			fmt.Printf("Failed to close the db with error: %v\n", err)
			return true
		}
		return true
	}
	return false
}
