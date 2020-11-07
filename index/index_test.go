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

package index

import (
	"fmt"
	"os"
	"testing"
)

const (
	EMPTY_INDEX_FILE_SIZE = 967
	TEST_DB_NAME          = "index_test"
)

func TestCreateDB(t *testing.T) {
	_, err := openNewDB()
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	idxFinfo, err := os.Stat(TEST_DB_NAME + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	if idxFinfo.Size() != EMPTY_INDEX_FILE_SIZE {
		t.Errorf("Initial index file size %d, want %d", idxFinfo.Size(), EMPTY_INDEX_FILE_SIZE)
	}
	dataFinfo, err := os.Stat(TEST_DB_NAME + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	if dataFinfo.Size() != 0 {
		t.Errorf("Initial data file is %d, want 0", dataFinfo.Size())
	}

}

func TestStoreOneRecord(t *testing.T) {
	db, err := openNewDB()
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Store("k1", "v1", INSERT)
	if err != nil {
		t.Fatal(err)
	}
	val, err := db.Fetch("k1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Errorf("Expected value v1 for key k1, got %s", val)
	}
}

func TestStoreMultipleRecords(t *testing.T) {
	db, err := openNewDB()
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	nrecords := 10
	keys := make([]string, nrecords)
	vals := make([]string, nrecords)
	for i := 0; i < nrecords; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		vals[i] = fmt.Sprintf("v%d", i)
	}
	for i, k := range keys {
		err = db.Store(k, vals[i], INSERT)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, k := range keys {
		val, err := db.Fetch(k)
		if err != nil {
			t.Fatal(k)
		}
		if val != vals[i] {
			t.Errorf("Expected value %s for key %s, got %s", vals[i], k, val)
		}
	}
}

func TestDeleteSimple(t *testing.T) {
	db, err := openNewDB()
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Store("k1", "v1", INSERT)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Store("k2", "v2", INSERT)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Delete("k2")
	if err != nil {
		t.Fatal(err)
	}
	val, err := db.Fetch("k1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Errorf("Expected value v1, got %s", val)
	}
	val, err = db.Fetch("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "" {
		t.Errorf("Expected value for k2 to be deleted, found value %s", val)
	}
}

func TestDeleteMulti(t *testing.T) {
	db, err := openNewDB()
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	nrecords := 10
	keys := make([]string, nrecords)
	vals := make([]string, nrecords)
	delKeys := make([]string, 4)
	for i := 0; i < nrecords; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		vals[i] = fmt.Sprintf("v%d", i)
		if i%2 == 0 {
			delKeys = append(delKeys, keys[i])
		}
	}
	for i, k := range keys {
		err = db.Store(k, vals[i], INSERT)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, k := range delKeys {
		err = db.Delete(k)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, k := range delKeys {
		val, err := db.Fetch(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != "" {
			t.Errorf("Expected value for key %s to be deleted, found value %s returned", k, val)
		}
	}

}

func TestInsertDeleteInsertFetch(t *testing.T) {
	db, err := openNewDB()
	// defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Store("k1", "v1", INSERT)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Store("k2", "v2", INSERT)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Delete("k2")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Store("k2", "v3", INSERT)
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Fetch("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v3" {
		t.Errorf("Expected value v3 for key k2, got %s", val)
	}

}

func openNewDB() (*Brickdb, error) {
	removeDB(TEST_DB_NAME)
	db := NewBrick()
	err := db.Open(TEST_DB_NAME, os.O_RDWR|os.O_CREATE)
	return db, err
}

func removeDB(name string) {
	os.Remove(name + ".idx")
	os.Remove(name + ".dat")
}
