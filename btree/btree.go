package btree

import "bytes"

type BTree struct {
	root uint64             // pointer to a page on disk
	get  func(uint64) BNode // dereferencing a pointer
	new  func(BNode) uint64 // allocate a new page
	del  func(uint64)       // deallocate a page
}

func (tree *BTree) Delete(key []byte) bool {
	if len(key) == 0 {
		panic("Delete: key is of size 0")
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		panic("Delete: key is larger than max key size")
	}
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key []byte, val []byte) {
	if len(key) == 0 {
		panic("Insert: key is of size 0")
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		panic("Insert: key is larger than max key size")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		panic("Insert: val is larger than max val size")
	}

	if tree.root == 0 {
		// create first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space
		// thus a lookup can always find a containing node
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	node = treeInsert(tree, node, key, val)
	nsplit, splitted := splitNode(node)
	if nsplit > 1 {
		// the root split, add a new level
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
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

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// find key
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		// delete the key in the leaf
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the child
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		if updated.nkeys() <= 0 {
			panic("Number of keys in updated node is 0 or lower")
		}
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// conditions for merging a node
// 1. node is smaller than 1/4 of a page
// 2. has sibling and merged result does not exceed one page
func shouldMerge(
	tree *BTree, node BNode, idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}
