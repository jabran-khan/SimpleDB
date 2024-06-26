package btree

import "bytes"

type BTree struct {
	root uint64             // pointer to a page on disk
	get  func(uint64) BNode // dereferencing a pointer
	new  func(BNode) uint64 // allocate a new page
	del  func(uint64)       // deallocate a page
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node
	// can be bigger than 1 page, will be split if bigger
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// index to insert/update key
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

// KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte,
) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	//split the result
	nsplit, splited := splitNode(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-idx+1)
}
