package database

import "encoding/binary"

const (
	BNODE_FREE_LIST  = 3
	FREE_LIST_HEADER = 4 + 8 + 8
	FREE_LIST_CAP    = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8
)

/*
| type | size | total | next | pointers  |
|  2B  |  2B  |  8B   |  8B  | size * 8B |

acts like a stack to keep track of unused pages
*/
type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode  // dereference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}

// number of items in the list
func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}
	page := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(page.data[32:]))
}

// get the nth pointer
func (fl *FreeList) Get(topn int) uint64 {
	if topn < 0 || topn >= fl.Total() {
		panic("Get: topn index is out of scope")
	}
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		if next == 0 {
			panic("Get: next is 0")
		}
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

// remove 'popn' pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	if popn > fl.Total() {
		panic("Update: popn is larger than the total number of items")
	}
	if popn == 0 && len(freed) == 0 {
		return
	}

	// prepare to construct the new list
	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recycle the node itself
		if popn >= flnSize(node) {
			// phase 1
			// remove all pointers in this node
			popn -= flnSize(node)
		} else {
			// phase 2
			// remove some pionters
			remain := flnSize(node) - popn
			popn = 0
			// reuse pointers from the free list itself
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		// discard the node and move to the next node
		total -= flnSize(node)
		fl.head = flnNext(node)
	}

	if len(reuse)*FREE_LIST_CAP < len(freed) && fl.head != 0 {
		panic("Update: invalid state")
	}

	// phase 3: prepend new nodes
	flPush(fl, freed, reuse)

	// done
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := BNode{make([]byte, BTREE_PAGE_SIZE)}

		// construct a new node
		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			// reuse a pionter from the list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			// or append a page to house the new node
			fl.head = fl.new(new)
		}
	}
	if len(reuse) != 0 {
		panic("flPush: all pages not reused from the list")
	}
}

/*
*
The node format:
| type | size | total | next | pointers  |
|  2B  |  2B  |  8B   |  8B  | size * 8B |
*/
func flnSize(node BNode) int {
	if node.data == nil {
		return 0
	}
	return int(binary.LittleEndian.Uint16(node.data[16:]))
}

func flnNext(node BNode) uint64 {
	if node.data == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(node.data[96:])
}

func flnPtr(node BNode, idx int) uint64 {
	if node.data == nil {
		return 0
	}
	headOffset := FREE_LIST_HEADER * 8
	ptrOffset := headOffset + idx*8
	return binary.LittleEndian.Uint64(node.data[ptrOffset:])
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	headOffset := FREE_LIST_HEADER * 8
	ptrOffset := headOffset + idx*8
	binary.LittleEndian.PutUint64(node.data[ptrOffset:], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[16:], size) // set size
	binary.LittleEndian.PutUint64(node.data[96:], next) // set next
}

func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[32:], total)
}
