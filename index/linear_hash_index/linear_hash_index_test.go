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
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"testing"
)

const (
	// EMPTY_INDEX_FILE_SIZE = 967
	TEST_DB_NAME = "index_test"
)

func TestCreateIndex(t *testing.T) {
	_, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	// _, err := os.Stat(TEST_DB_NAME + ".idx")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if idxFinfo.Size() != EMPTY_INDEX_FILE_SIZE {
	// 	t.Errorf("Initial index file size %d, want %d", idxFinfo.Size(), EMPTY_INDEX_FILE_SIZE)
	// }
	dataFinfo, err := os.Stat(TEST_DB_NAME + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	if dataFinfo.Size() != 0 {
		t.Errorf("Initial data file is %d, want 0", dataFinfo.Size())
	}

}

func TestStoreOneRecord(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k1", "v1")
	if err != nil {
		t.Fatal(err)
	}
	val, err := hashIndex.Fetch("k1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Errorf("Expected value v1 for key k1, got %s", val)
	}
}

func TestStoreMultipleRecords(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	// defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	nrecords := 300
	keys := make([]string, nrecords)
	vals := make([]string, nrecords)
	for i := 0; i < nrecords; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		vals[i] = fmt.Sprintf("v%d", i)
	}
	for i, k := range keys {
		err = hashIndex.Insert(k, vals[i])
		if err != nil {
			t.Fatal(k, err)
		}
	}
	for i, k := range keys {
		val, err := hashIndex.Fetch(k)
		if err != nil {
			t.Fatal(k, err)
		}
		if val != vals[i] {
			t.Errorf("Expected value %s for key %s, got %s", vals[i], k, val)
		}
	}
}

func TestDeleteSimple(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k1", "v1")
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k2", "v2")
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Delete("k2")
	if err != nil {
		t.Fatal(err)
	}
	val, err := hashIndex.Fetch("k1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Errorf("Expected value v1, got %s", val)
	}
	val, err = hashIndex.Fetch("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "" {
		t.Errorf("Expected value for k2 to be deleted, found value %s", val)
	}
}

func TestDeleteMulti(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	nrecords := 10000
	keys := make([]string, nrecords)
	vals := make([]string, nrecords)
	delKeys := make([]string, nrecords/2)
	for i := 0; i < nrecords; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		vals[i] = fmt.Sprintf("v%d", i)
		if i%2 == 0 {
			delKeys = append(delKeys, keys[i])
		}
	}
	for i, k := range keys {
		err = hashIndex.Insert(k, vals[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, k := range delKeys {
		err = hashIndex.Delete(k)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, k := range delKeys {
		val, err := hashIndex.Fetch(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != "" {
			t.Errorf("Expected value for key %s to be deleted, found value %s returned", k, val)
		}
	}

}

func TestInsertDeleteInsertFetch(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k1", "v1")
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k2", "v2")
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Delete("k2")
	if err != nil {
		t.Fatal(err)
	}

	err = hashIndex.Insert("k2", "v3")
	if err != nil {
		t.Fatal(err)
	}

	val, err := hashIndex.Fetch("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v3" {
		t.Errorf("Expected value v3 for key k2, got %s", val)
	}
}

func TestUpdate(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k1", "v1")
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Insert("k2", "v2")
	if err != nil {
		t.Fatal(err)
	}
	err = hashIndex.Update("k2", "v3")
	if err != nil {
		t.Fatal(err)
	}

	val, err := hashIndex.Fetch("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v3" {
		t.Errorf("Expected value v3 for key k2, got %s", val)
	}

}

func TestConcurrentReadWrite(t *testing.T) {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()
	var wg sync.WaitGroup
	openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	nrecords := 10000
	keys := make([]string, nrecords)
	vals := make([]string, nrecords)
	for i := 0; i < nrecords; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
		vals[i] = fmt.Sprintf("val_%d", i)
	}
	nthreads := 10
	step := 100
	for i := 0; i < nthreads; i++ {
		wg.Add(1)
		start := i * step
		end := start + step
		// fmt.Printf("%d-%d\n", start, end)
		go work(t, &wg, keys[start:end], vals[start:end])
	}
	wg.Wait()
}

func work(t *testing.T, wg *sync.WaitGroup, keys []string, vals []string) {
	defer wg.Done()
	hashIndex, err := openNewDB(false, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	defer hashIndex.Close()
	for i, k := range keys {
		err := hashIndex.Insert(k, vals[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, k := range keys {
		val, err := hashIndex.Fetch(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != vals[i] {
			t.Errorf("Expected value %s for key %s, got %s", vals[i], k, val)
		}
	}

	for _, k := range keys {
		err := hashIndex.Delete(k)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, k := range keys {
		val, err := hashIndex.Fetch(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != "" {
			t.Errorf("Expected key %s to be deleted, found value %s", k, val)
		}
	}
}

func TestFetchAll(t *testing.T) {
	hashIndex, err := openNewDB(true, os.O_RDWR|os.O_CREATE)
	defer removeDB(TEST_DB_NAME)
	if err != nil {
		t.Fatal(err)
	}
	nrecords := 100
	keys := make([]string, nrecords)
	vals := make([]string, nrecords)
	for i := 0; i < nrecords; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		vals[i] = fmt.Sprintf("v%d", i)
	}
	for i, k := range keys {
		err = hashIndex.Insert(k, vals[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	valuesMap, err := hashIndex.FetchAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(valuesMap) != nrecords {
		t.Errorf("Expected to read %d records, read %d records", nrecords, len(valuesMap))
	}
	for i := 0; i < nrecords; i++ {
		v, ok := valuesMap[keys[i]]
		if !ok {
			t.Errorf("No value found for key %s", keys[i])
		}
		if v != vals[i] {
			t.Errorf("value for key %s expected to be %s, found %s", keys[i], vals[i], v)
		}
	}
}

func openNewDB(removeExisting bool, mode int) (*LinearHashIndex, error) {
	if removeExisting {
		removeDB(TEST_DB_NAME)
	}
	hashIndex := new(LinearHashIndex)
	err := hashIndex.Open(TEST_DB_NAME, mode)
	return hashIndex, err
}

func removeDB(name string) {
	os.Remove(name + ".idx")
	os.Remove(name + ".dat")
	os.Remove(name + ".bkt")
}
