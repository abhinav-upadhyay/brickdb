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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/OneOfOne/xxhash"
	"golang.org/x/sys/unix"
)

const (
	idx_header_off  = 0
	idx_header_size = 4 // idxtype(1)
	idxtype_sz      = 3 //one byte
	IDXLEN_SZ       = 4 //index record length
	SEP             = ':'
	SEP_STR         = ":"
	PTR_SZ          = 7                                //size of ptr field in hash chain
	PTR_MAX         = 9999999                          // max file offset = 10 ** PTR_SZ - 1
	HASHTABLE_SIZE  = 137                              //hash table size
	FREE_OFF        = idx_header_off + idx_header_size //free list offset in index file
	HASH_OFF        = FREE_OFF + PTR_SZ                //hash table offset in index file
	IDXLEN_MIN      = 6
	IDXLEN_MAX      = 1024
	DATLEN_MIN      = 2
	DATLEN_MAX      = 1024
)

type HashIndex struct {
	idxFile  *os.File
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
}

func (self *HashIndex) Open(name string, mode int) error {
	self.nhash = HASHTABLE_SIZE
	self.hashoff = HASH_OFF
	self.name = name
	var err error
	self.idxFile, err = os.OpenFile(self.name+".idx", mode, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create index file %s", self.name+".idx")
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
			self.writeHeader()
			/**
			 * We have to build a chain NHASH_DEF + 1 hash chain pointers
			 */
			hashPointer := fmt.Sprintf("%*d", PTR_SZ, 0)
			hashPointer = strings.Repeat(hashPointer, HASHTABLE_SIZE+1)
			hashPointer = hashPointer + "\n"
			bytes := []byte(hashPointer)
			bytesWritten, err := self.idxFile.Write(bytes)
			if err != nil {
				return errors.New("Write to index file failed")
			}
			if bytesWritten != len(bytes) {
				return errors.New("Failed to initialize index file")
			}
		}

	}
	self.Rewind()
	return nil
}

func (self *HashIndex) writeHeader() error {
	/**
	 * We need to write the 256 byte index header first. Header is defined as:
	 * number of buckets (4 bytes): split pointer (4 bytes): rest 0 bytes, reserved for future use
	 */
	header := fmt.Sprintf("%*d\n", idxtype_sz, HashIndexType)
	_, err := self.idxFile.Seek(idx_header_off, io.SeekStart)
	_, err = self.idxFile.Write([]byte(header))
	return err
}

func (self *HashIndex) Close() error {
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
	return nil
}

func (self *HashIndex) FetchAll() (map[string]string, error) {
	records := make(map[string]string)
	var i uint64
	var startOff int64 = FREE_OFF
	for i = 0; i < self.nhash; i++ {
		startOff += PTR_SZ
		err := ReadLockW(self.idxFile.Fd(), startOff, io.SeekStart, 1)
		if err != nil {
			return nil, err
		}
		offset, err := self.readPtr(startOff)
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

func (self *HashIndex) Fetch(key string) (string, error) {
	found, err := self.findAndLock(key, false)
	defer Unlock(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
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
func (self *HashIndex) findAndLock(key string, isWriteLock bool) (bool, error) {
	/**
	 * Calculate the hash value for the key, and then calculate the offset of
	 * corresponding chain pointer in hash table
	 */
	self.chainoff = int64(self.dbHash(key)*PTR_SZ) + self.hashoff
	self.ptroff = self.chainoff
	var err error

	/**
	 * We lock the hash chain, the caller must unlock it.Note we lock and unlock only
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
	offset, err := self.readPtr(self.ptroff)
	if err != nil {
		return false, nil
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

func (self *HashIndex) dbHash(key string) uint64 {
	hasher := xxhash.NewS64(42)
	hasher.WriteString(key)
	return hasher.Sum64() % uint64(self.nhash)
}

/**
 * Read a chain pointer field from anywhere in the index file -
 * the free list pointer, the hash table chain pointer or an index
 * record chain pointer
 */
func (self *HashIndex) readPtr(offset int64) (int64, error) {
	buf := make([]byte, PTR_SZ)
	_, err := self.idxFile.Seek(offset, io.SeekStart)
	if err != nil {
		return -1, err
	}
	readBytes, err := self.idxFile.Read(buf)
	if err != nil {
		return -1, err
	}
	if readBytes != PTR_SZ {
		return -1, errors.New("Failed to read pointer data")
	}
	s := string(buf)
	return parseInt(s)
}

func createIOVecArray(size int, byteArrays ...[]byte) [][]byte {
	iovecBytes := make([][]byte, size)
	for i, b := range byteArrays {
		iovecBytes[i] = b
	}
	return iovecBytes
}

/**
 * Read next index record. Starting from the specified offset, we read
 * the index record into idxbuf field. We set datoff and datlen to
 * offset and length of the value in data file
 */
func (self *HashIndex) readIdx(offset int64) (int64, error) {
	/**
	 * Position index file and record the offset.
	 */

	seekPos := io.SeekStart
	if offset == 0 {
		seekPos = io.SeekCurrent
	}
	curOffset, err := self.idxFile.Seek(offset, seekPos)
	if err != nil {
		return -1, err
	}
	self.idxoff = curOffset

	/* Read the fixed length header in the index record */
	ptrbuf := make([]byte, PTR_SZ)
	idxLenbuf := make([]byte, IDXLEN_SZ)
	iovecBytes := make([][]byte, 2)
	iovecBytes[0] = ptrbuf
	iovecBytes[1] = idxLenbuf
	// iovecBytes := createIOVecArray(2, ptrbuf, idxbuf)
	bytesRead, err := unix.Readv(int(self.idxFile.Fd()), iovecBytes)
	if err != nil {
		return -1, err
	}

	if bytesRead == 0 && offset == 0 {
		return -1, nil
	}
	self.ptrval, _ = parseInt(string(ptrbuf))
	self.idxlen, _ = parseInt(string(idxLenbuf))
	if self.idxlen < IDXLEN_MIN || self.idxlen > IDXLEN_MAX {
		return -1, fmt.Errorf("Invalid index record length %d", self.idxlen)
	}
	idxbufBytes := make([]byte, self.idxlen)

	/* Now read the actual index record */
	bytesRead, err = self.idxFile.Read(idxbufBytes)
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

	parts := strings.Split(idxbuf, SEP_STR)
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
	if self.datlen < 0 || self.datlen > DATLEN_MAX {
		return -1, errors.New("Invalid data record length")
	}
	return self.ptrval, nil
}

func (self *HashIndex) readData() (string, error) {
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

func (self *HashIndex) Delete(key string) error {
	found, err := self.findAndLock(key, true)
	if err != nil {
		return err
	}
	defer func() error {
		return Unlock(self.idxFile.Fd(), self.chainoff, io.SeekStart, 1)
	}()
	if found {
		return self._delete()
	}
	return nil
}

func (self *HashIndex) _delete() error {
	var freeptr, saveptr int64
	self.datbuf = strings.Repeat(" ", len(self.datbuf))
	self.idxbuf = strings.Repeat(" ", len(self.idxbuf))
	err := WriteLockW(self.idxFile.Fd(), FREE_OFF, io.SeekStart, 1)
	if err != nil {
		return err
	}
	defer func() error {
		return Unlock(self.idxFile.Fd(), FREE_OFF, io.SeekStart, 1)
	}()
	self.writeData(self.datbuf, self.datoff, io.SeekStart)
	freeptr, err = self.readPtr(FREE_OFF)
	if err != nil {
		return err
	}
	saveptr = self.ptrval
	err = self.writeIdx(self.idxbuf, self.idxoff, io.SeekStart, freeptr)
	if err != nil {
		return err
	}
	err = self.writePtr(FREE_OFF, self.idxoff)
	if err != nil {
		return err
	}
	return self.writePtr(self.ptroff, saveptr)
}

func (self *HashIndex) writeData(data string, offset int64, whence int) error {
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

func (self *HashIndex) writeIdx(key string, offset int64, whence int, ptrval int64) error {
	if self.ptrval < 0 || self.ptrval > PTR_MAX {
		return fmt.Errorf("Invalid pointer: %d", self.ptrval)
	}
	self.idxbuf = fmt.Sprintf("%s%c%d%c%d\n", key, SEP, self.datoff, SEP, self.datlen)
	length := len(self.idxbuf)
	if length < IDXLEN_MIN || length > IDXLEN_MAX {
		return errors.New("Invalid index record length")
	}

	indexRecPrefix := fmt.Sprintf("%*d%*d", PTR_SZ, ptrval, IDXLEN_SZ, length)

	// if we are appending we need to lock the index file
	if whence == io.SeekEnd {
		err := WriteLockW(self.idxFile.Fd(), ((int64(self.nhash)+1)*PTR_SZ)+1, io.SeekStart, 0)
		if err != nil {
			return err
		}
		defer func() error {
			return Unlock(self.idxFile.Fd(), (int64(self.nhash+1)*PTR_SZ)+1, io.SeekStart, 0)
		}()
	}

	idxoff, err := self.idxFile.Seek(offset, whence)
	if err != nil {
		return err
	}
	self.idxoff = idxoff
	iovecBytes := make([][]byte, 2)
	iovecBytes[0] = []byte(indexRecPrefix)
	iovecBytes[1] = []byte(self.idxbuf)
	bytesWritten, err := unix.Writev(int(self.idxFile.Fd()), iovecBytes)
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
func (self *HashIndex) writePtr(offset int64, ptrval int64) error {
	if ptrval < 0 || ptrval > PTR_MAX {
		return fmt.Errorf("Invalid ptrval: %d", ptrval)
	}
	asciiptr := fmt.Sprintf("%*d", PTR_SZ, ptrval)
	_, err := self.idxFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	bytesWritten, err := self.idxFile.Write([]byte(asciiptr))
	if bytesWritten != PTR_SZ {
		return errors.New("Failed to write index pointer")
	}
	return nil
}

func (self *HashIndex) Insert(key string, value string) error {
	return self.store(key, value, insert)
}

func (self *HashIndex) Update(key string, value string) error {
	return self.store(key, value, update)
}

func (self *HashIndex) Upsert(key string, value string) error {
	return self.store(key, value, upsert)
}

func (self *HashIndex) store(key string, value string, op indexStoreOp) error {
	keyLen := int64(len(key))
	valueLen := int64(len(value))
	if valueLen < DATLEN_MIN || valueLen > DATLEN_MAX {
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

		ptrval, err := self.readPtr(self.chainoff)
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
			err = self.writePtr(self.chainoff, self.idxoff)
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
			err = self.writePtr(self.chainoff, self.idxoff)
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
			ptrval, err := self.readPtr(self.chainoff)
			if err != nil {
				return err
			}
			self.writeData(value, 0, io.SeekEnd)
			self.writeIdx(key, 0, io.SeekEnd, ptrval)
			self.writePtr(self.chainoff, self.idxoff)
		} else {
			self.writeData(value, self.datoff, io.SeekStart)
		}
	}
	return nil
}

func (self *HashIndex) findFree(keylen int64, datlen int64) (bool, error) {
	var offset, nextOffset, saveOffset int64
	err := WriteLockW(self.idxFile.Fd(), FREE_OFF, io.SeekStart, 1)
	if err != nil {
		return false, err
	}
	defer Unlock(self.idxFile.Fd(), FREE_OFF, io.SeekStart, 1)
	saveOffset = FREE_OFF
	offset, err = self.readPtr(saveOffset)
	found := false
	for offset != 0 {
		nextOffset, err = self.readIdx(offset)
		if int64(len(self.idxbuf)) == keylen && self.datlen == datlen {
			break
		}
		saveOffset = offset
		offset = nextOffset
	}

	if offset != 0 {
		self.writePtr(saveOffset, self.ptrval)
		found = true
	}
	return found, nil
}

func (self *HashIndex) Rewind() {
	offset := (self.nhash + 1) * PTR_SZ
	self.idxFile.Seek(int64(offset), io.SeekStart)
}
