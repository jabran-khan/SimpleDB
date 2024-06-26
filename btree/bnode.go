package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	BNODE_NODE = 1 // nodes without values
	BNODE_LEAF = 2 // leaf nodes with values

	HEADER             = 4 // contains type of node and number of keys
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	// ensures that a node with a single KV-pair will not exceed the size of the page
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("Node size exceeds size of page")
	}
}

type BNode struct {
	// structure of data is as follows
	// type: 2B
	// nkeys: 2B
	// pointers: nkeys * 8B
	// offsets: nkeys * 2B
	// key-values: ...
	//		klen: 2B
	//		vlen: 2B
	//		key: ...
	//		val: ...
	data []byte // using bytes to dump the value to disk
}

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointer methods
func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic(fmt.Sprintf(
			"getPtr: Index(%d) is greater than or equal to number of keys(%d)",
			idx, node.nkeys()))
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic(fmt.Sprintf(
			"setPtr: Index(%d) is greater than or equal to number of keys(%d)",
			idx, node.nkeys()))
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.AppendUint64(node.data[pos:], val)
}

// offset functions and methods
func offsetPos(node BNode, idx uint16) uint16 {
	if idx > node.nkeys() {
		panic(fmt.Sprintf(
			"offSetPos: idx (%d) is outside of valid offset range: 0 - %d",
			idx, node.nkeys()))
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffSet(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffSet(idx uint16, offset uint16) {
	binary.LittleEndian.AppendUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic(fmt.Sprintf(
			"kvPos: idx (%d) out of range of keys (1 - %d)",
			idx, node.nkeys()))
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffSet(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx > node.nkeys() {
		panic(fmt.Sprintf(
			"getKey: idx (%d) out of range of keys (1 - %d)",
			idx, node.nkeys()))
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx > node.nkeys() {
		panic(fmt.Sprintf(
			"getKey: idx (%d) out of range of keys (1 - %d)",
			idx, node.nkeys()))
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// finds the first child node where our key is in the range of the keys of the child
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	// the first key is a copy from the parent node,
	// it is always less than or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node
func leafInsert(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// update an existing key to a leaf node
func leafUpdate(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx+1)
}

// copy KVs into the position
func nodeAppendRange(
	new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16,
) {
	if srcOld+n > old.nkeys() {
		panic("problem with nodeAppendRange")
	}
	if dstNew+n > new.nkeys() {
		panic("problem with nodeAppendRange")
	}

	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	// offsets
	dstBegin := new.getOffSet(dstNew)
	srcBegin := old.getOffSet(srcOld)
	for i := uint16(1); i <= n; i++ { // range is [1,n]
		offset := dstBegin + old.getOffSet(srcOld+i) - srcBegin
		new.setOffSet(dstNew+i, offset)
	}

	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

func nodeAppendKV(
	new BNode, idx uint16, ptr uint64, key []byte, val []byte,
) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffSet(idx+1, new.getOffSet(idx)+4+uint16((len(key)+len(val))))
}

// split a bigger-than-allowed node into two
// the right node always fits on a page
func splitSingleNode(left BNode, right BNode, old BNode) {
	nkeys := old.nkeys()
	totalBytes := old.nbytes()
	idx := uint16(0)
	curBytes := 0
	for i := uint16(0); i < nkeys; i++ {
		oldPos := old.kvPos(i)
		keyLen := binary.LittleEndian.Uint16(old.data[oldPos:])
		valLen := binary.LittleEndian.Uint16(old.data[oldPos+2:])
		// 8 for pointer, 2 for offset, 2 for klen, 2 for vlen
		curBytes += 8 + 2 + 2 + 2 + int(keyLen) + int(valLen)

		// remove 4 from page size since 4 bytes will be used for the header
		if totalBytes-uint16(curBytes) <= BTREE_PAGE_SIZE-4 {
			idx = i
			break
		}
	}

	left.setHeader(old.btype(), idx)
	right.setHeader(old.btype(), nkeys-idx)

	nodeAppendRange(left, old, 0, 0, idx)
	nodeAppendRange(right, old, 0, idx, nkeys-idx)
}

// splits the node if it's too big, resulting in 1 to 3 nodes
func splitNode(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	splitSingleNode(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	splitSingleNode(leftleft, middle, left)
	if leftleft.nbytes() > BTREE_PAGE_SIZE {
		panic("leftleft page size is still larger than page size")
	}
	return 3, [3]BNode{leftleft, middle, right}
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}
