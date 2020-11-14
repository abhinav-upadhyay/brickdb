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

package brickdb

import (
	"fmt"
	"os"

	"github.com/abhinav-upadhyay/brickdb/index"
)

type Brickdb struct {
	name      string
	indexType index.IndexType
	index     index.BrickIndex
}

type StoreOp int

const (
	Insert StoreOp = iota
	Update
	Upsert
)

func New(name string) *Brickdb {
	db := new(Brickdb)
	db.name = name
	return db
}

func (self *Brickdb) Create(indexType index.IndexType) error {
	self.indexType = indexType
	return self.openIndex(os.O_RDWR | os.O_CREATE)
}

func (self *Brickdb) openIndex(mode int) error {
	switch self.indexType {
	case index.HashIndexType:
		self.index = new(index.HashIndex)
		return self.index.Open(self.name, mode)
	default:
		return fmt.Errorf("Invalid indexType: %v", self.indexType)
	}
}

func (self *Brickdb) Open(mode int) error {
	if self.index == nil {
		return self.openIndex(os.O_RDWR)
	}
	return nil
}

func (self *Brickdb) Close() error {
	return self.index.Close()
}

func (self *Brickdb) Fetch(key string) (string, error) {
	return self.index.Fetch(key)
}

func (self *Brickdb) Delete(key string) error {
	return self.index.Delete(key)
}

func (self *Brickdb) Store(key string, value string, storeOp StoreOp) error {
	switch storeOp {
	case Insert:
		return self.index.Insert(key, value)
	case Update:
		return self.index.Update(key, value)
	case Upsert:
		return self.index.Upsert(key, value)
	default:
		return fmt.Errorf("Unsupported storeOp value: %v", storeOp)
	}
}

func (self *Brickdb) FetchAll() (map[string]string, error) {
	return self.index.FetchAll()
}
