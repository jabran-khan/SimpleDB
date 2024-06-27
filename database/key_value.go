package database

import (
	"fmt"
	"os"
	"syscall"
)

const DB_SIG = "TreeVaultDB"

// file may larger than our mapping
// so we create a struct which allows us to extend our mapping by using multiple mappings
type KeyValue struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

// callback for Btree, allocate a new page
func (db *KeyValue) pageNew(node BNode) uint64 {
	if len(node.data) > BTREE_PAGE_SIZE {
		panic("pageNew: node is larger than page size")
	}
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.data)
	return ptr
}

// callback for BTree, dereference a pointer
func (db *KeyValue) pageGet(ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("pageGet: bad ptr")
}

// callback for Btree, deallocate a page
func (db *KeyValue) pageDel(uint64) {
	// TODO: complete
}

func (db *KeyValue) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// btree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}
	// done
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanup
func (db *KeyValue) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		if err != nil {
			panic("Close: couldn't delete mappings for specified chunk")
		}
	}
	_ = db.fp.Close()
}

// read the db
func (db *KeyValue) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// update the db
func (db *KeyValue) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

// delete from the db
func (db *KeyValue) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}
