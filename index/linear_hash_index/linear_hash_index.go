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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/OneOfOne/xxhash"
	"golang.org/x/sys/unix"
)

// all sizes are in number of ascii characters since the current impl uses ascii encoding
// TODO: switch to binary encoding to save space
const (
	idx_header_off      = 0
	idx_header_size     = 64 // idxtype(1) + nbuckets(8) + split(8) + nrecords(8) + newline
	idxtype_sz          = 3  //one byte
	nbuckets_sz         = 20 // 8 bytes - max number of buckets can be 2 ** 64
	split_pointer_sz    = 20 // 8 bytes - max number of buckets can be 2 ** 64
	nrecords_sz         = 20
	idxlen_sz           = 4 //index record length
	sep                 = ':'
	sep_str             = ":"
	ptr_sz              = 7                                //size of ptr field in hash chain
	ptr_max             = 9999999                          // max file offset = 10 ** PTR_SZ - 1
	hashtable_size      = 1024                             //initial hash table size
	free_off            = idx_header_off + idx_header_size //free list offset in index file
	hash_off            = free_off + ptr_sz                //hash table offset in index file
	idxlen_min          = 6
	idxlen_max          = 1024
	datlen_min          = 2
	datlen_max          = 1024
	idxfile_startoffset = 1
)

type indexStoreOp int

const (
	insert indexStoreOp = iota
	update
	upsert
)

type LinearHashIndex struct {
	idxFile  *os.File
	bktFile  *os.File
	datFile  *os.File
	idxbuf   string
	datbuf   string
	name     string
	idxoff   int64
	idxlen   int64
	datoff   int64
	datlen   int64
	ptrval   int64
	ptroff   int64
	chainoff int64
	hashoff  int64
	nhash    uint64
	i        int16
	s        uint64
	nrecords int64
	debug    bool
}

func (self *LinearHashIndex) EnableDebug() {
	self.debug = true
}

func (self *LinearHashIndex) Open(name string, mode int) error {
	self.nhash = hashtable_size
	self.hashoff = hash_off
	self.name = name
	self.nrecords = 0
	self.i = 10
	self.s = 0
	var err error
	self.idxFile, err = os.OpenFile(self.name+".idx", mode, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create index file %s", self.name+".idx")
	}

	self.bktFile, err = os.OpenFile(self.name+".bkt", mode, 0644)
	if err != nil {
		return err
	}

	self.datFile, err = os.OpenFile(self.name+".dat", mode, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create data file %s", self.name+".dat")
	}

	isCreateMode := mode&(os.O_CREATE|os.O_TRUNC) == os.O_CREATE || mode&(os.O_CREATE|os.O_TRUNC) == os.O_TRUNC
	if isCreateMode {
		/**
		 * If the database was created we need to initialize it. We need to lock the entire file,
		 * stat it, check its size and initialize it atomically
		 */
		if WriteLockW(self.idxFile.Fd(), 0, io.SeekStart, 0) != nil {
			return errors.New("Failed to write lock index for init")
		}

		defer func() error {
			return Unlock(self.idxFile.Fd(), 0, io.SeekStart, 0)
		}()

		idxFileInfo, err := self.idxFile.Stat()
		if err != nil {
			return errors.New("Failed to stat the index file")
		}

		if idxFileInfo.Size() == 0 {
			/**
			 * We need to write the 256 byte index header first. Header is defined as:
			 * number of buckets (4 bytes): split pointer (4 bytes): rest 0 bytes, reserved for future use
			 */
			header := fmt.Sprintf("%*d%*d%*d%*d", idxtype_sz, 1, nbuckets_sz, hashtable_size, split_pointer_sz, 0, nrecords_sz, 0)
			header = header + "\n"
			bytesWritten, err := self.idxFile.Write([]byte(header))
			if err != nil {
				return err
			}
			/**
			 * We have to build a chain NHASH_DEF + 1 hash chain pointers
			 */
			hashPointer := fmt.Sprintf("%*d", ptr_sz, 0)
			hashPointer = strings.Repeat(hashPointer, hashtable_size+1)
			// hashPointer = hashPointer + "\n"
			bytes := []byte(hashPointer)
			bytesWritten, err = self.idxFile.Write(bytes)
			if err != nil {
				return errors.New("Write to index file failed")
			}
			if bytesWritten != len(bytes) {
				return errors.New("Failed to initialize index file")
			}
			self.bktFile.Write([]byte("\n"))
		}
	} else {
		self.readHeader(true, false)
		defer func() error {
			return Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
		}()

		if err != nil {
			return err
		}
	}
	self.Rewind()
	return nil
}

func (self *LinearHashIndex) Close() error {
	if self.idxFile != nil {
		err := self.idxFile.Close()
		if err != nil {
			return err
		}
	}

	if self.datFile != nil {
		err := self.datFile.Close()
		if err != nil {
			return err
		}
	}

	if self.bktFile != nil {
		err := self.bktFile.Close()
		return err
	}
	return nil
}

//TODO: fix this?
func (self *LinearHashIndex) FetchAll() (map[string]string, error) {
	records := make(map[string]string)
	var i uint64
	var startOff int64 = free_off
	for i = 0; i < self.nhash; i++ {
		startOff += ptr_sz
		err := ReadLockW(self.idxFile.Fd(), startOff, io.SeekStart, 1)
		if err != nil {
			return nil, err
		}
		offset, err := self.readPtr(startOff, self.idxFile)
		if err != nil {
			Unlock(self.idxFile.Fd(), startOff, io.SeekStart, 1)
			return nil, err
		}
		if offset == 0 {
			Unlock(self.idxFile.Fd(), startOff, io.SeekStart, 1)
			continue
		}

		for {
			nextOffset, err := self.readIdx(offset)
			if err != nil {
				Unlock(self.idxFile.Fd(), startOff, io.SeekStart, 1)
				return nil, err
			}
			val, err := self.readData()
			if err != nil {
				Unlock(self.idxFile.Fd(), startOff, io.SeekStart, 1)
				return nil, err
			}
			records[self.idxbuf] = val
			if nextOffset != 0 {
				offset = nextOffset
			} else {
				err = Unlock(self.idxFile.Fd(), startOff, io.SeekStart, 1)
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}
	return records, nil

}

func (self *LinearHashIndex) Fetch(key string) (string, error) {
	found, err := self.findAndLock(key, false)
	defer Unlock(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
	defer Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	val, err := self.readData()
	if err != nil {
		return "", err
	}
	return val, nil
}

/**
 * Find the record associated with the given key
 */
func (self *LinearHashIndex) findAndLock(key string, isWriteLock bool) (bool, error) {
	/**
	 * Calculate the hash value for the key, and then calculate the offset of
	 * corresponding chain pointer in hash table
	 */
	err := self.readHeader(true, false)
	if err != nil {
		return false, err
	}
	hash := self.dbHash(key)
	if self.debug {
		if isWriteLock {
			fmt.Printf("[%d] Inserting/deleting key %s into bucket %d\n", getGID(), key, hash)
		} else {
			fmt.Printf("[%d] reading key %s from bucket %d\n", getGID(), key, hash)
		}
	}
	self.chainoff = int64(hash*ptr_sz) + self.hashoff
	self.ptroff = self.chainoff

	/**
	 * We lock the hash chain, the caller must unlock it. Note we lock and unlock only
	 * the first byte
	 */
	if isWriteLock {
		err = WriteLockW(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
	} else {
		err = ReadLockW(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
	}
	if err != nil {
		return false, err
	}

	/**
	 * Get the offset of the first record in hash chain
	 */
	offset, err := self.readPtr(self.ptroff, self.idxFile)
	if err != nil {
		return false, err
	}

	for offset != 0 {
		nextOffset, err := self.readIdx(offset)
		if err != nil {
			return false, err
		}
		if self.idxbuf == key {
			break
		}
		self.ptroff = offset
		offset = nextOffset
	}

	if offset == 0 {
		return false, nil
	}
	return true, nil
}

func (self *LinearHashIndex) dbHash(key string) uint64 {
	hasher := xxhash.NewS64(42)
	hasher.WriteString(key)
	hash := hasher.Sum64()
	if self.debug {
		fmt.Printf("[%d] hash for key %s is %d, i=%d\n", getGID(), key, hash, self.i)
	}
	bktidx := hash & ((1 << self.i) - 1)
	if bktidx < self.nhash {
		if self.debug {
			fmt.Printf("[%d] 1- bucket for %s is %d\n", getGID(), key, bktidx)
		}
		return bktidx
	} else {
		if self.debug {
			fmt.Printf("[%d] 2- bucket for %s is %d, nhash: %d\n", getGID(), key, (bktidx ^ (1 << (self.i - 1))), self.nhash)
		}
		return bktidx ^ (1 << (self.i - 1))
	}
}

/**
 * Read a chain pointer field from anywhere in the index file -
 * the free list pointer, the hash table chain pointer or an index
 * record chain pointer
 */
func (self *LinearHashIndex) readPtr(offset int64, f *os.File) (int64, error) {
	buf := make([]byte, ptr_sz)
	_, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return -1, err
	}
	readBytes, err := f.Read(buf)
	if err != nil {
		return -1, err
	}
	if readBytes != ptr_sz {
		return -1, errors.New("Failed to read pointer data")
	}
	s := string(buf)
	return parseInt(s)
}

// func createIOVecArray(size int, byteArrays ...[]byte) [][]byte {
// 	iovecBytes := make([][]byte, size)
// 	for i, b := range byteArrays {
// 		iovecBytes[i] = b
// 	}
// 	return iovecBytes
// }

func parseInt(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

func parseUint(s string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(s), 10, 64)
}

/**
 * Read next index record. Starting from the specified offset, we read
 * the index record into idxbuf field. We set datoff and datlen to
 * offset and length of the value in data file
 */
func (self *LinearHashIndex) readIdx(offset int64) (int64, error) {
	/**
	 * Position index file and record the offset.
	 */

	seekPos := io.SeekStart
	if offset == 0 {
		seekPos = io.SeekCurrent
	}
	curOffset, err := self.bktFile.Seek(offset, seekPos)
	if err != nil {
		return -1, err
	}
	self.idxoff = curOffset

	/* Read the fixed length header in the index record */
	ptrbuf := make([]byte, ptr_sz)
	idxLenbuf := make([]byte, idxlen_sz)
	iovecBytes := make([][]byte, 2)
	iovecBytes[0] = ptrbuf
	iovecBytes[1] = idxLenbuf
	// iovecBytes := createIOVecArray(2, ptrbuf, idxbuf)
	bytesRead, err := unix.Readv(int(self.bktFile.Fd()), iovecBytes)
	if err != nil {
		return -1, err
	}

	if bytesRead == 0 && offset == 0 {
		return -1, nil
	}
	self.ptrval, _ = parseInt(string(ptrbuf))
	self.idxlen, _ = parseInt(string(idxLenbuf))
	if self.idxlen < idxlen_min || self.idxlen > idxlen_max {
		return -1, fmt.Errorf("Invalid index record length %d", self.idxlen)
	}
	idxbufBytes := make([]byte, self.idxlen)

	/* Now read the actual index record */
	bytesRead, err = self.bktFile.Read(idxbufBytes)
	if err != nil {
		return -1, err
	}
	if int64(bytesRead) != self.idxlen {
		return -1, fmt.Errorf("Failed to read index record at offset %d", offset)
	}

	if !testNewLine(string(idxbufBytes)) {
		return -1, fmt.Errorf("Corrupted index record at offset %d, not ending with new line", offset)
	}
	idxbufBytes = idxbufBytes[:self.idxlen-1] //ignore the newline
	idxbuf := string(idxbufBytes)

	parts := strings.Split(idxbuf, sep_str)
	if len(parts) == 0 {
		return -1, fmt.Errorf("Invalid index record: missing separators")
	}

	if len(parts) > 3 {
		return -1, fmt.Errorf("Invalid index record: too many separators (%d)", len(parts))
	}

	self.idxbuf = parts[0]
	self.datoff, err = parseInt(parts[1])
	if err != nil {
		return -1, err
	}

	if self.datoff < 0 {
		return -1, errors.New("Starting data offset < 0")
	}

	self.datlen, err = parseInt(parts[2])
	if err != nil {
		return -1, err
	}
	if self.datlen < 0 || self.datlen > datlen_max {
		return -1, errors.New("Invalid data record length")
	}
	return self.ptrval, nil
}

func (self *LinearHashIndex) readData() (string, error) {
	_, err := self.datFile.Seek(self.datoff, io.SeekStart)
	if err != nil {
		return "", err
	}

	datbuf := make([]byte, self.datlen)
	bytesRead, err := self.datFile.Read(datbuf)
	if err != nil {
		return "", err
	}
	if int64(bytesRead) != self.datlen {
		return "", fmt.Errorf("Failed to read data record from offset %d", self.datoff)
	}
	if !testNewLine(string(datbuf)) {
		return "", errors.New("Corrupted data record: missing newline")
	}
	datbuf = datbuf[:self.datlen-1]
	self.datbuf = string(datbuf)
	return self.datbuf, nil
}

func (self *LinearHashIndex) readHeader(doLock bool, isWriteLock bool) error {
	if doLock {
		var err error
		if isWriteLock {
			err = WriteLockW(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
		} else {
			err = ReadLockW(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
		}
		if err != nil {
			return err
		}
	}
	_, err := self.idxFile.Seek(idx_header_off, io.SeekStart)
	if err != nil {
		return err
	}
	indexTypeBuf := make([]byte, 3)
	nhashBuf := make([]byte, 20)
	sBUf := make([]byte, 20)
	nrecordsBuf := make([]byte, 20)
	iovecBytes := make([][]byte, 4)
	iovecBytes[0] = indexTypeBuf
	iovecBytes[1] = nhashBuf
	iovecBytes[2] = sBUf
	iovecBytes[3] = nrecordsBuf
	_, err = unix.Readv(int(self.idxFile.Fd()), iovecBytes)
	if err != nil {
		return err
	}
	self.nhash, _ = parseUint(string(nhashBuf))
	self.s, _ = parseUint(string(sBUf))
	self.nrecords, _ = parseInt(string(nrecordsBuf))
	self.i = int16(math.Ceil(math.Log2(float64(self.nhash))))
	if self.debug {
		fmt.Printf("[%d] read header with nhash:%d, s:%d, i:%d, nrecords:%d\n", getGID(), self.nhash, self.s, self.i, self.nrecords)
	}
	return nil
}

func (self *LinearHashIndex) updateHeader(nrecordsChange int64, nhashChange uint64, sChange uint64) error {
	self.nrecords += nrecordsChange
	self.nhash += nhashChange
	self.s += sChange
	if self.s*2 == self.nhash {
		self.s = 0
	}
	self.writeHeader()
	return nil
}

func (self *LinearHashIndex) writeHeader() error {
	header := fmt.Sprintf("%*d%*d%*d%*d\n", idxtype_sz, 1, nbuckets_sz, self.nhash, split_pointer_sz, self.s, nrecords_sz, self.nrecords)
	if self.debug {
		fmt.Printf("[%d] writing header %s", getGID(), header)
	}
	_, err := self.idxFile.Seek(idx_header_off, io.SeekStart)
	_, err = self.idxFile.Write([]byte(header))
	return err
}

func (self *LinearHashIndex) delete2(key string) (bool, error) {
	found, err := self.findAndLock(key, true)
	if err != nil {
		return found, err
	}
	defer func() (bool, error) {
		return found, Unlock(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
	}()
	if found {
		//TODO: update nrecords in header
		if self.debug {
			fmt.Printf("[%d] offset for deleting %s: %d, ptroff: %d\n", getGID(), key, self.chainoff, self.ptroff)
		}
		err = self._delete()
		if err != nil {
			return found, err
		}
		if self.debug {
			fmt.Printf("[%d] deleted key %s\n", getGID(), key)
		}
		// self.updateHeader(-1, 0, 0)
	}
	return found, nil
}

func (self *LinearHashIndex) Delete(key string) error {
	if self.debug {
		fmt.Printf("[%d] deleting key %s\n", getGID(), key)
	}

	_, err := self.delete2(key)
	defer func() error {
		return Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
	}()
	if err != nil {
		return err
	}
	return nil
}

func (self *LinearHashIndex) _delete() error {
	var freeptr, saveptr int64
	self.datbuf = strings.Repeat(" ", int(self.datlen)-1)
	self.idxbuf = strings.Repeat(" ", len(self.idxbuf))
	err := WriteLockW(self.idxFile.Fd(), free_off, io.SeekStart, 1)
	if err != nil {
		return err
	}
	defer func() error {
		return Unlock(self.idxFile.Fd(), free_off, io.SeekStart, 1)
	}()

	self.writeData(self.datbuf, self.datoff, io.SeekStart)
	freeptr, err = self.readPtr(free_off, self.idxFile)
	if err != nil {
		return err
	}
	saveptr = self.ptrval
	err = self.writeIdx(self.idxbuf, self.idxoff, io.SeekStart, freeptr)
	if err != nil {
		return err
	}
	err = self.writePtr(self.idxFile, free_off, self.idxoff)
	if err != nil {
		return err
	}
	if self.ptroff != self.chainoff {
		return self.writePtr(self.bktFile, self.ptroff, saveptr)
	} else {
		return self.writePtr(self.idxFile, self.ptroff, saveptr)
	}
}

func (self *LinearHashIndex) writeData(data string, offset int64, whence int) error {
	// we need to lock if we are adding a new record - no need for lock for overwriting
	if whence == io.SeekEnd {
		err := WriteLockW(self.datFile.Fd(), 0, io.SeekStart, 0) //lock whole file
		if err != nil {
			return err
		}
		defer func() error {
			return Unlock(self.datFile.Fd(), 0, io.SeekStart, 0)
		}()
	}

	newoffset, err := self.datFile.Seek(offset, whence)
	if err != nil {
		return err
	}
	self.datoff = newoffset

	self.datlen = int64(len(data) + 1) // +1 for newline
	iovecBytes := make([][]byte, 2)
	iovecBytes[0] = []byte(data)
	iovecBytes[1] = []byte("\n")
	_, err = unix.Writev(int(self.datFile.Fd()), iovecBytes)
	return err
}

func (self *LinearHashIndex) writeIdx(key string, offset int64, whence int, ptrval int64) error {
	if self.ptrval < 0 || self.ptrval > ptr_max {
		return fmt.Errorf("Invalid pointer: %d", self.ptrval)
	}

	self.idxbuf = fmt.Sprintf("%s%c%d%c%d\n", key, sep, self.datoff, sep, self.datlen)
	length := len(self.idxbuf)
	if length < idxlen_min || length > idxlen_max {
		return errors.New("Invalid index record length")
	}

	indexRecPrefix := fmt.Sprintf("%*d%*d", ptr_sz, ptrval, idxlen_sz, length)

	// if we are appending we need to lock the index file
	if whence == io.SeekEnd {
		lockOff := self.hashoff + ((int64(self.nhash) + 1) * ptr_sz) + 1
		err := WriteLockW(self.idxFile.Fd(), lockOff, io.SeekStart, 0)
		if err != nil {
			return err
		}
		defer func() error {
			return Unlock(self.idxFile.Fd(), lockOff, io.SeekStart, 0)
		}()
	}

	idxoff, err := self.bktFile.Seek(offset, whence)
	if err != nil {
		return err
	}
	self.idxoff = idxoff
	iovecBytes := make([][]byte, 2)
	iovecBytes[0] = []byte(indexRecPrefix)
	iovecBytes[1] = []byte(self.idxbuf)
	bytesWritten, err := unix.Writev(int(self.bktFile.Fd()), iovecBytes)
	if err != nil {
		return err
	}
	if bytesWritten != len(indexRecPrefix)+len(self.idxbuf) {
		return errors.New("Error while writing index record")
	}

	return nil
}

/**
 * Write a chain pointer field in the index file
 */
func (self *LinearHashIndex) writePtr(f *os.File, offset int64, ptrval int64) error {
	if ptrval < 0 || ptrval > ptr_max {
		return fmt.Errorf("Invalid ptrval: %d", ptrval)
	}
	if self.debug {
		fmt.Printf("[%d] writing ptr %d at offset %d\n", getGID(), ptrval, offset)
	}
	asciiptr := fmt.Sprintf("%*d", ptr_sz, ptrval)
	_, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	bytesWritten, err := f.Write([]byte(asciiptr))
	if bytesWritten != ptr_sz {
		return errors.New("Failed to write index pointer")
	}
	return nil
}

func (self *LinearHashIndex) Insert(key string, value string) error {
	if self.debug {
		fmt.Printf("[%d] inserting key %s\n", getGID(), key)
	}
	err := self.store(key, value, insert)
	if err != nil {
		return err
	}
	if self.debug {
		fmt.Printf("[%d] insert done\n", getGID())
	}
	// we read the header and lock the index file to update the header
	defer func() error {
		return Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
	}()
	self.nrecords++
	//TODO: is the cast really required here?
	if self.computeLoadFactor() >= 0.8 {
		Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
		self.readHeader(true, true)
		self.nrecords++
		if self.computeLoadFactor() < 0.8 {
			return self.updateHeader(0, 0, 0)
		}
		if self.debug {
			fmt.Printf("[%d] Splitting bucket %d\n", getGID(), self.s)
		}
		err = self.split()
		if err != nil {
			return err
		}
		self.updateHeader(0, 0, 0)
		if self.debug {
			fmt.Printf("[%d] split done, new s: %d\n", getGID(), self.s)
		}
	} else {
		Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
		self.readHeader(true, true)
		err = self.updateHeader(1, 0, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *LinearHashIndex) computeLoadFactor() float64 {
	return float64(1.0 * self.nrecords / int64(30*self.nhash))
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func (self *LinearHashIndex) split() error {
	oldS := self.s
	self.s++
	oldChainPtrOff := int64(oldS*ptr_sz) + self.hashoff
	err := WriteLockW(self.idxFile.Fd(), oldChainPtrOff, io.SeekStart, 1)
	if err != nil {
		return err
	}
	defer Unlock(self.idxFile.Fd(), oldChainPtrOff, io.SeekStart, 1)
	hashPointer := fmt.Sprintf("%*d", ptr_sz, 0)
	bytes := []byte(hashPointer)
	newChainPtrOff, err := self.idxFile.Seek(0, io.SeekEnd)
	bytesWritten, err := self.idxFile.Write(bytes)
	if err != nil {
		return errors.New("Write to index file failed")
	}
	if bytesWritten != len(bytes) {
		return errors.New("Failed to initialize index file")
	}
	err = WriteLockW(self.idxFile.Fd(), newChainPtrOff, io.SeekStart, 1)
	if err != nil {
		return err
	}
	defer Unlock(self.idxFile.Fd(), newChainPtrOff, io.SeekStart, 1)
	self.nhash++
	if self.s*2 == self.nhash {
		self.s = 0
	}
	if self.nhash > (1 << self.i) {
		self.i++
	}

	// rehash the chain being split
	newChainPtrOffFile := self.idxFile
	oldChainPtrOffFile := self.idxFile
	offset, err := self.readPtr(oldChainPtrOff, self.idxFile)
	self.ptroff = oldChainPtrOff
	if err != nil {
		return err
	}

	for offset != 0 {
		nextOffset, err := self.readIdx(offset)
		if err != nil {
			return err
		}
		chainOff := int64(self.dbHash(self.idxbuf))
		if chainOff != int64(oldS) {
			if self.debug {
				fmt.Printf("[%d] Moving %s from bucket %d to %d\n", getGID(), self.idxbuf, oldS, chainOff)
			}
			err = self.writePtr(newChainPtrOffFile, newChainPtrOff, offset)
			if err != nil {
				return err
			}
			newChainPtrOffFile = self.bktFile
			newChainPtrOff = offset
			err = self.writePtr(oldChainPtrOffFile, self.ptroff, nextOffset)
			if err != nil {
				return err
			}
			offset = self.ptroff
		} else {
			oldChainPtrOffFile = self.bktFile
		}
		self.ptroff = offset
		offset = nextOffset
	}

	return nil
}

func (self *LinearHashIndex) Update(key string, value string) error {
	defer Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
	return self.store(key, value, update)
}

func (self *LinearHashIndex) Upsert(key string, value string) error {
	//TODO: handle split
	defer Unlock(self.idxFile.Fd(), idx_header_off, io.SeekStart, 1)
	return self.store(key, value, upsert)
}

func (self *LinearHashIndex) store(key string, value string, op indexStoreOp) error {
	keyLen := int64(len(key))
	valueLen := int64(len(value))
	if valueLen < datlen_min || valueLen > datlen_max {
		return fmt.Errorf("Invalid data length: %d", valueLen)
	}

	found, err := self.findAndLock(key, true)
	defer func() error {
		return Unlock(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
	}()
	if err != nil {
		return err
	}
	if !found {
		if op == update {
			return fmt.Errorf("Record with key %s does not exist", key)
		}

		ptrval, err := self.readPtr(self.chainoff, self.idxFile)
		if err != nil {
			return err
		}

		foundFree, err := self.findFree(keyLen, valueLen)
		if err != nil {
			return err
		}
		if !foundFree {
			err = self.writeData(value, 0, io.SeekEnd)
			if err != nil {
				return err
			}
			err = self.writeIdx(key, 0, io.SeekEnd, ptrval)
			if err != nil {
				return err
			}
			err = self.writePtr(self.idxFile, self.chainoff, self.idxoff)
			if err != nil {
				return err
			}
		} else {
			err = self.writeData(value, self.datoff, io.SeekStart)
			if err != nil {
				return err
			}
			err = self.writeIdx(key, self.idxoff, io.SeekStart, ptrval)
			if err != nil {
				return err
			}
			err = self.writePtr(self.idxFile, self.chainoff, self.idxoff)
			if err != nil {
				return err
			}
		}
	} else {
		if op == insert {
			return fmt.Errorf("Record already exists with key: %s", key)
		}
		if valueLen != self.datlen {
			err = self._delete()
			if err != nil {
				return err
			}
			ptrval, err := self.readPtr(self.chainoff, self.idxFile)
			if err != nil {
				return err
			}
			self.writeData(value, 0, io.SeekEnd)
			self.writeIdx(key, 0, io.SeekEnd, ptrval)
			self.writePtr(self.idxFile, self.chainoff, self.idxoff)
		} else {
			self.writeData(value, self.datoff, io.SeekStart)
		}
	}
	return nil
}

func (self *LinearHashIndex) findFree(keylen int64, datlen int64) (bool, error) {
	var offset, nextOffset, saveOffset int64
	err := WriteLockW(self.idxFile.Fd(), free_off, io.SeekStart, 1)
	if err != nil {
		return false, err
	}
	defer Unlock(self.idxFile.Fd(), free_off, io.SeekStart, 1)
	saveOffset = free_off
	offset, err = self.readPtr(saveOffset, self.idxFile)
	found := false
	for offset != 0 {
		nextOffset, err = self.readIdx(offset)
		if err != nil {
			return false, err
		}
		if int64(len(self.idxbuf)) == keylen && self.datlen == datlen {
			break
		}
		saveOffset = offset
		offset = nextOffset
	}

	if offset != 0 {
		self.writePtr(self.bktFile, saveOffset, self.ptrval)
		found = true
	}
	return found, nil
}

func testNewLine(s string) bool {
	buf := []byte(s)
	lastRune, _ := utf8.DecodeLastRune(buf)
	return lastRune == '\n'
}

func (self *LinearHashIndex) Rewind() {
	offset := (self.nhash + 1) * ptr_sz
	self.idxFile.Seek(int64(offset), io.SeekStart)
}

func ReadLock(fd uintptr, offset int64, whence int16, len int64) error {
	return getLock(fd, unix.F_OFD_SETLK, unix.F_RDLCK, offset, whence, len)
}

func ReadLockW(fd uintptr, offset int64, whence int16, len int64) error {
	return getLock(fd, unix.F_OFD_SETLKW, unix.F_RDLCK, offset, whence, len)
}

func WriteLock(fd uintptr, offset int64, whence int16, len int64) error {
	return getLock(fd, unix.F_OFD_SETLK, unix.F_WRLCK, offset, whence, len)
}

func WriteLockW(fd uintptr, offset int64, whence int16, len int64) error {
	return getLock(fd, unix.F_OFD_SETLKW, unix.F_WRLCK, offset, whence, len)
}

func Unlock(fd uintptr, offset int64, whence int16, len int64) error {
	return getLock(fd, unix.F_OFD_SETLK, unix.F_UNLCK, offset, whence, len)
}

func getLock(fd uintptr, cmd int, lockType int16, offset int64, whence int16, len int64) error {
	var lock *unix.Flock_t = new(unix.Flock_t)
	lock.Type = lockType
	lock.Whence = whence
	lock.Start = offset
	lock.Len = len
	return unix.FcntlFlock(fd, cmd, lock)
}
