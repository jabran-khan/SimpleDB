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
	free FreeList

	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64 // database size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer
		// nil value denotes a deallocated page
		updates map[uint64][]byte
	}
}

// callback for Btree, allocate a new page
func (db *KeyValue) pageNew(node BNode) uint64 {
	if len(node.data) > BTREE_PAGE_SIZE {
		panic("pageNew: node is larger than page size")
	}
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

// callback for BTree, dereference a pointer
func (db *KeyValue) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		if page == nil {
			panic("pageGet: page is nil")
		}
		return BNode{page} // for new pages
	}
	return pageGetMapped(db, ptr) // for written pages
}

func pageGetMapped(db *KeyValue, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("pageGetMapped: bad ptr")
}

// callback for Btree, deallocate a page
func (db *KeyValue) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
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

	// freelist callbacks
	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse

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

// callback for FreeList, allocate a new page
func (db *KeyValue) pageAppend(node BNode) uint64 {
	if len(node.data) > BTREE_PAGE_SIZE {
		panic("pageAppend: node is larger than BTREE_PAGE_SIZE")
	}
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

// callback for FreeList, reuse a page
func (db *KeyValue) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}
