package database

import "unsafe"

type Container struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newContainer() *Container {
	pages := map[uint64]BNode{}
	return &Container{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				if !ok {
					panic("could not find page")
				}
				return node
			},
			new: func(node BNode) uint64 {
				if node.nbytes() > BTREE_PAGE_SIZE {
					panic("node does not fit within page")
				}
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				if pages[key].data != nil {
					panic("the page has data in it")
				}
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				if !ok {
					panic("page does not exist")
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *Container) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *Container) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

// test cases below here
