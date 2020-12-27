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
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

type IndexType int

const (
	HashIndexType       IndexType = 1
	LinearHashIndexType IndexType = 2
)

type indexStoreOp int

const (
	insert indexStoreOp = iota
	update
	upsert
)

type BrickIndex interface {
	Open(name string, mode int) error
	Close() error
	Fetch(key string) (string, error)
	FetchAll() (map[string]string, error)
	Delete(key string) error
	Insert(key string, value string) error
	Update(key string, value string) error
	Upsert(key string, value string) error
}

func parseInt(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

func parseUint(s string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(s), 10, 64)
}

func testNewLine(s string) bool {
	buf := []byte(s)
	lastRune, _ := utf8.DecodeLastRune(buf)
	return lastRune == '\n'
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
