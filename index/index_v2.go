package index

import (
	"CZ4031_Project_1/storage"
	"fmt"
)

func (tree *BPTree) Insert(key uint32, addr *storage.RecordLocation) {
	var node *BPNode

	if tree.Root == nil {
		node = tree.newLeafNode()
		tree.Root = node
	} else {
		node, _ = tree.locateLeaf(key, false)
	}

	// Add the duplicate key linked list if key exists
	for i, item := range node.Key {
		if item == key {
			node.DataPtr[i].insert(addr)
			return
		}
	}

	if node.getKeySize() < tree.Order-1 {
		node.insertIntoLeaf(key, addr)
	} else {
		tree.splitAndInsertIntoLeaf(node, key, addr)
	}

}

// func (tree *BPTree) Search(key uint32, verbose bool) []*byte {
// 	node, count := tree.locateLeaf(key, verbose)

// 	if verbose {
// 		fmt.Printf("Total index node accessed: %v\n", count)
// 	}
// 	for i, item := range node.Key {
// 		if item == key {
// 			return node.DataPtr[i].extractDuplicateKeyRecords()
// 		}
// 	}
// 	return nil
// }

func (tree *BPTree) Delete(key uint32) {
	node, _ := tree.locateLeaf(key, false)
	tree.deleteKey(node, key)
}

func (tree *BPTree) Print() {
	fmt.Println("Tree:")
	node := tree.Root
	next := tree.Root.Children
	fmt.Printf("%v\n", node.Key)

	for {
		if len(next) == 0 {
			break
		}

		var tempNext []*BPNode
		for _, value := range next {
			if value == nil {
				continue
			}
			fmt.Printf("%v", value.Key)
			if !value.IsLeaf {
				tempNext = append(tempNext, value.Children...)
			}
		}
		fmt.Println("")
		next = tempNext
	}
}

func (tree *BPTree) PrintLeaves() {
	fmt.Println("Leaves:")
	node, _ := tree.locateLeaf(0, false)

	for node != nil {
		fmt.Printf("%v -> ", node.Key)
		node = node.Next
	}
	fmt.Println("End")

}

func (tree *BPTree) GetHeight() int {
	cursor := tree.Root
	height := 0

	if cursor == nil {
		return 0
	}

	for !cursor.IsLeaf {
		cursor = cursor.Children[0]
		height++
	}
	height += 1
	return height
}

func (tree *BPTree) GetTotalNodes() int {
	node := tree.Root

	if node == nil {
		return 0
	}

	children := tree.Root.Children

	count := 1
	for {
		if len(children) == 0 {
			break
		}

		var tempChildren []*BPNode
		for _, value := range children {
			if value == nil {
				continue
			}

			count++

			if !value.IsLeaf {
				tempChildren = append(tempChildren, value.Children...)
			}
		}
		children = tempChildren
	}

	return count
}

// Extract all records with the same key
// func (record *RecordLLNode) extractDuplicateKeyRecords() []*byte {
// 	r := record
// 	res := []*byte{r.Addr}

// 	// Traverse the linked list
// 	for r.Next != nil {
// 		r = r.Next
// 		res = append(res, r.Addr)
// 	}

// 	return res
// }

// Insert a record to the end of the record linked list
func (record *RecordLLNode) insert(addr *storage.RecordLocation) {
	r := record
	for r.Next != nil {
		r = r.Next
		continue
	}

	r.Next = &RecordLLNode{
		RecordInfo: addr,
		Next:       nil,
	}
}

// Get the current key size of a node
func (node *BPNode) getKeySize() int {
	count := 0

	if node == nil {
		panic("Node is nil")
	}

	for _, value := range node.Key {
		// Possible issue with this implementation if there exist NumVotes = 0
		if value == 0 {
			break
		}
		count += 1
	}
	return count
}

// search the tree to locate the leaf node
// return the leaf node the key is at
func (tree *BPTree) locateLeaf(key uint32, verbose bool) (*BPNode, int) {
	var keySize int

	cursor := tree.Root
	// Empty tree
	if cursor == nil {
		return cursor, 0
	}

	if verbose {
		fmt.Println("Node content while traversing the tree (up to first 5):")
	}

	// Recursive search until leaf
	count := 0
	for !cursor.IsLeaf {
		count++
		if verbose {
			if count <= 5 {
				fmt.Printf("Node content: %v\n", cursor.Key)
			}
		}

		keySize = cursor.getKeySize()

		found := false
		for i := 0; i < keySize; i++ {
			if key < cursor.Key[i] {
				cursor = cursor.Children[i]
				found = true
				break
			}
		}
		if !found {
			cursor = cursor.Children[keySize]
		}
	}

	count++

	if verbose {
		if count <= 5 {
			fmt.Printf("Node content: %v\n", cursor.Key)
		}
	}

	return cursor, count
}

// Get the split point when 1 node is split into 2
// Lecture definition: LEFT has ceil(n/2) keys, RIGHT has floor(n/2) keys
func getSplitIndex(order int) int {
	n := order - 1
	if n%2 == 0 {
		return n / 2
	}

	return n/2 + 1 // = Ceil(n/2)
}

// Create a non-leaf node
func (tree *BPTree) newNode() *BPNode {
	return &BPNode{
		IsLeaf:   false,
		Key:      make([]uint32, tree.Order-1),
		Children: make([]*BPNode, tree.Order),
		Parent:   nil,
	}
}

// Create a leaf node
func (tree *BPTree) newLeafNode() *BPNode {
	return &BPNode{
		IsLeaf:  true,
		Key:     make([]uint32, tree.Order-1),
		DataPtr: make([]*RecordLLNode, tree.Order),
		Parent:  nil,
	}
}

//
//
// Insert related codes
//
//

// helper function to insert node/addr/key into their slice at target index
func insertAt[T *BPNode | *RecordLLNode | uint32](arr []T, value T, target int) {

	// Shift 1 position down the array
	for i := len(arr) - 1; i >= 0; i-- {
		if i == target {
			break
		}
		arr[i] = arr[i-1]
	}
	arr[target] = value
}

// helper function to get the insertion index
func getInsertIndex(keyList []uint32, key uint32) int {
	for i, item := range keyList {
		if item == 0 {
			// 0 == nil in key list -> empty slot found
			return i
		}

		if key < item {
			return i
		}
	}
	panic("Error: getInsertIndex()")
}

// Insert into leaf, given a space in leaf
func (node *BPNode) insertIntoLeaf(key uint32, addr *storage.RecordLocation) {
	targetIndex := getInsertIndex(node.Key, key)
	insertAt(node.DataPtr, &RecordLLNode{RecordInfo: addr}, targetIndex) // insert ptr
	insertAt(node.Key, key, targetIndex)                                 // insert key
}

// Split the node and insert
func (tree *BPTree) splitAndInsertIntoLeaf(node *BPNode, key uint32, addr *storage.RecordLocation) {

	tempKeys := make([]uint32, tree.Order) // Temp key's size is key + 1 (Order)
	tempPointers := make([]*RecordLLNode, tree.Order+1)
	copy(tempKeys, node.Key)
	copy(tempPointers, node.DataPtr)

	targetIndex := getInsertIndex(tempKeys, key)
	insertAt(tempKeys, key, targetIndex)
	insertAt(tempPointers, &RecordLLNode{RecordInfo: addr}, targetIndex)

	splitIndex := getSplitIndex(tree.Order)

	node.Key = make([]uint32, tree.Order-1)
	node.DataPtr = make([]*RecordLLNode, tree.Order-1)
	copy(node.Key, tempKeys[:splitIndex])
	copy(node.DataPtr, tempPointers[:splitIndex])

	// Create a new node on the right
	newNode := tree.newNode() // Make a new node for the right side
	newNode.Key = make([]uint32, tree.Order-1)
	newNode.DataPtr = make([]*RecordLLNode, tree.Order-1)
	copy(newNode.Key, tempKeys[splitIndex:])
	copy(newNode.DataPtr, tempPointers[splitIndex:])
	newNode.Parent = node.Parent // new node shares the same parent as the left node
	newNode.IsLeaf = true
	newNode.Next = node.Next
	node.Next = newNode

	tree.insertIntoParent(node, newNode, newNode.Key[0])

}

// Insert into internal node, given a space in the node
func (node *BPNode) insertIntoNode(key uint32, rightNode *BPNode) {
	targetIndex := getInsertIndex(node.Key, key)
	if key == 19 {
		fmt.Printf("%v\n", targetIndex)
	}
	insertAt(node.Children, rightNode, targetIndex+1) // insert ptr
	insertAt(node.Key, key, targetIndex)              // insert key
}

func (tree *BPTree) splitAndInsertIntoNode(node *BPNode, insertedNode *BPNode, key uint32) {
	tempKeys := make([]uint32, tree.Order)
	tempPointers := make([]*BPNode, tree.Order+1)

	copy(tempKeys, node.Key)
	copy(tempPointers, node.Children)

	insertIndex := getInsertIndex(tempKeys, key)
	insertAt(tempKeys, key, insertIndex)
	insertAt(tempPointers, insertedNode, insertIndex+1)

	splitIndex := getSplitIndex(tree.Order)

	// Left node
	node.Key = make([]uint32, tree.Order-1)
	node.Children = make([]*BPNode, tree.Order)
	copy(node.Key, tempKeys[:splitIndex])
	copy(node.Children, tempPointers[:splitIndex+1])

	// Right node
	newNode := tree.newNode() // Make a new node for the right side
	newNode.Key = make([]uint32, tree.Order-1)
	newNode.Children = make([]*BPNode, tree.Order)
	copy(newNode.Key, tempKeys[splitIndex+1:])
	copy(newNode.Children, tempPointers[splitIndex+1:])
	newNode.Parent = node.Parent // new node shares the same parent as the left node

	for _, item := range newNode.Children {
		if item != nil {
			item.Parent = newNode
		}
	}

	// Ascend the mid-key and ptr
	ascendKey := tempKeys[splitIndex]
	ascendPtr := newNode

	tree.insertIntoParent(node, ascendPtr, ascendKey)

}

func (tree *BPTree) insertIntoParent(leftNode *BPNode, rightNode *BPNode, key uint32) {
	var insertIndex int
	parent := leftNode.Parent

	if parent == nil {
		// No parent, create new
		parent = tree.newNode()
		tree.Root = parent
		insertAt(parent.Key, key, insertIndex)
		insertAt(parent.Children, leftNode, 0)
		insertAt(parent.Children, rightNode, 1)

		// Update parent
		for _, item := range parent.Children {
			if item != nil {
				item.Parent = parent
			}
		}
	} else if parent.getKeySize() < tree.Order-1 {
		parent.insertIntoNode(key, rightNode)
	} else {
		tree.splitAndInsertIntoNode(parent, rightNode, key)
	}

}

//
//
// Delete related codes
//
//

// helper function to remove node/addr/key into their slice at target index
func removeAt[T *BPNode | *RecordLLNode | uint32](arr []T, target int) {
	// Shift item forward by 1
	for i := target + 1; i < len(arr); i++ {
		arr[i-1] = arr[i]
	}
}

func (node *BPNode) delete(key uint32) {
	var target int

	found := false
	for i, item := range node.Key {
		if item == key {
			target = i
			found = true
			break
		}
	}

	if !found {
		panic("Key does not exist")
	}

	removeAt(node.Key, target)
	node.Key[len(node.Key)-1] = 0
	if node.IsLeaf {
		removeAt(node.DataPtr, target)
		node.DataPtr[len(node.DataPtr)-1] = nil

		// Update the parent's key if the key deleted is the first
		if target == 0 && node.getKeySize() != 0 {
			for i, item := range node.Parent.Key {
				if item == key {
					node.Parent.Key[i] = node.Key[0]
				}
			}
		}

	} else {
		removeAt(node.Children, target+1)
		node.Children[len(node.Children)-1] = nil
	}

}

func (tree *BPTree) deleteKey(node *BPNode, key uint32) {
	var minKey int

	node.delete(key)

	if tree.Root == node {
		// Tree is root
		if node.getKeySize() >= 0 {
			return
		}

		if node.IsLeaf {
			// Tree is empty
			tree.Root = nil
		} else {
			//move the first child up to become root
			tree.Root = node.Children[0]
			node.Parent = nil
		}
		return
	}

	if node.IsLeaf {
		minKey = tree.Order / 2 // floor( (n+1)/2 )
	} else {
		minKey = (tree.Order - 1) / 2 // floor( n/2 )
	}

	keySize := node.getKeySize()
	if keySize >= minKey {
		// Enough keys
		return
	}

	availableNode, isPrev, mergeableNode := node.findAvailableNeighbour(minKey)

	//if key == 42 {
	//	fmt.Printf("%v %v %v", availableNode, isPrev, mergeableNode)
	//}

	if availableNode == nil {
		// Can't borrow anything, merging is needed
		tree.mergeNode(node, mergeableNode, isPrev)
	} else {
		// Borrow 1 from neighbour
		node.borrowFromNode(availableNode, isPrev)
	}

	//fmt.Printf("Neighbour: %v\n", neighbour)
}

// Find a neighbouring node that can borrow a node
// Return the available node (can be nil) and left & right neighbours
func (node *BPNode) findAvailableNeighbour(minKey int) (available *BPNode, isPrev bool, mergeable *BPNode) {
	var left, right *BPNode
	for i, item := range node.Parent.Children {
		if item == node {
			if i != 0 {
				// node is not the first node
				left = node.Parent.Children[i-1]
			}

			if i < len(node.Parent.Children)-1 {
				// node is not the last node
				right = node.Parent.Children[i+1]
			}
		}
	}

	if left != nil && left.getKeySize()-1 >= minKey {
		return left, true, nil
	}

	if right != nil && right.getKeySize()-1 >= minKey {
		return right, false, nil
	}

	// No available node to borrow, return mergeable node
	if left != nil {
		return nil, true, left
	} else {
		return nil, false, right
	}
}

func (tree *BPTree) mergeNode(node *BPNode, mergeInto *BPNode, isPrev bool) {
	tempKeys := make([]uint32, len(node.Key))

	if node.IsLeaf {
		tempPtrs := make([]*RecordLLNode, len(node.DataPtr))
		if isPrev {
			copy(tempKeys[:mergeInto.getKeySize()], mergeInto.Key[:mergeInto.getKeySize()])
			copy(tempKeys[mergeInto.getKeySize():], node.Key)

			copy(tempPtrs[:mergeInto.getKeySize()], mergeInto.DataPtr[:mergeInto.getKeySize()])
			copy(tempPtrs[mergeInto.getKeySize():], node.DataPtr)

			// Fix next pointer
			for _, item := range node.Parent.Children {
				if item == nil {
					break
				}

				if item.Next == mergeInto {
					item.Next = node
					break
				}
			}
		} else {
			copy(tempKeys[:node.getKeySize()], node.Key[:node.getKeySize()])
			copy(tempKeys[mergeInto.getKeySize()-1:], mergeInto.Key[:mergeInto.getKeySize()])
			copy(tempPtrs[:node.getKeySize()], node.DataPtr[:node.getKeySize()])
			copy(tempPtrs[mergeInto.getKeySize()-1:], mergeInto.DataPtr[:mergeInto.getKeySize()])
			node.Next = mergeInto.Next
		}

		node.Key = tempKeys
		node.DataPtr = tempPtrs

		var deleteKey uint32
		for i, item := range mergeInto.Parent.Children {
			if item == mergeInto {
				if isPrev {
					deleteKey = mergeInto.Parent.Key[i]
					node.Parent.Children[i] = node
				} else {
					deleteKey = mergeInto.Parent.Key[i-1]
					node.Parent.Children[i] = node
				}
			}
		}

		tree.deleteKey(mergeInto.Parent, deleteKey)
	}
}

func (node *BPNode) borrowFromNode(borrowFrom *BPNode, isPrev bool) {
	var insertIndex, removeIndex int
	var parentKey, parentReplaceKey uint32

	if isPrev {
		// Move the last item of borrowFrom to first item of node
		insertIndex = 0
		removeIndex = borrowFrom.getKeySize() - 1
		parentKey = node.Key[0]
		parentReplaceKey = borrowFrom.Key[borrowFrom.getKeySize()-1]
	} else {
		// Move the first item of borrowFrom to the last item of node
		insertIndex = node.getKeySize() - 1
		removeIndex = 0
		parentKey = borrowFrom.Key[0]
		parentReplaceKey = borrowFrom.Key[1]
	}

	insertAt(node.Key, borrowFrom.Key[removeIndex], insertIndex)
	removeAt(borrowFrom.Key, removeIndex)
	borrowFrom.Key[len(borrowFrom.Key)-1] = 0 // set last index as nil

	if node.IsLeaf {

		insertAt(node.DataPtr, borrowFrom.DataPtr[removeIndex], insertIndex)
		removeAt(borrowFrom.DataPtr, removeIndex)
		borrowFrom.DataPtr[len(borrowFrom.DataPtr)-1] = nil // set last index as nil

		//Fix parent's key
		for i, item := range node.Parent.Key {
			if item == parentKey {
				node.Parent.Key[i] = parentReplaceKey
				break
			}
		}
	} else {
		if isPrev {
			insertAt(node.Children, borrowFrom.Children[removeIndex+1], insertIndex)
		} else {
			insertAt(node.Children, borrowFrom.Children[removeIndex+1], insertIndex+1)
		}

		removeAt(borrowFrom.Children, removeIndex+1)
		borrowFrom.Children[len(borrowFrom.Children)-1] = nil // set last index as nil

		//Fix parent's key
		for i, item := range node.Parent.Children {
			if item == node {
				if isPrev {
					temp := node.Parent.Key[i-1]
					node.Parent.Key[i-1] = parentReplaceKey
					node.Key[0] = temp
				} else {
					temp := node.Parent.Key[i]
					node.Parent.Key[i] = parentReplaceKey
					node.Key[node.getKeySize()-1] = temp
				}
				break
			}
		}
	}
}

func (tree *BPTree) Search(config SearchConfig, verbose bool) []*storage.RecordLocation {
	if config.Type == ValueQuery {
		key := config.Values[0]
		node, count := tree.locateLeaf(key, verbose)

		if verbose {
			fmt.Printf("Total index node accessed: %v\n", count)
		}
		for i, item := range node.Key {
			if item == key {
				return node.DataPtr[i].getRecordsFromLinkedList()
			}
		}
		return nil
	} else if config.Type == RangeQuery {

	} else {
		fmt.Printf("Incorrect query type")
	}
	return nil
}

//func (tree *BPTree) SearchRange(fromKey uint32, toKey uint32, verbose bool) []*storage.RecordLocation {
//	var records []*storage.RecordLocation
//	node, count := tree.locateLeaf(fromKey, verbose)
//
//	// Process first node
//	for i, item := range node.Key {
//		if item == 0 {
//			break
//		}
//		if item >= fromKey {
//			records = append(records, node.DataPtr[i].extractDuplicateKeyRecords()...)
//		}
//	}
//	node = node.Next
//
//	for node != nil {
//		count += 1
//
//		if verbose {
//			if count <= 5 {
//				fmt.Printf("Node content: %v\n", node.Key)
//			}
//		}
//
//		for i, item := range node.Key {
//			if item == 0 || item > toKey {
//				break
//			}
//			records = append(records, node.DataPtr[i].extractDuplicateKeyRecords()...)
//		}
//
//		if node.Key[node.getKeySize()-1] >= toKey {
//			// Range reached
//			break
//		}
//		node = node.Next
//
//	}
//	if verbose {
//		fmt.Printf("Total index node accessed: %v\n", count)
//	}
//	return records
//
//}

func (recordNode *RecordLLNode) getRecordsFromLinkedList() []*storage.RecordLocation {
	res := []*storage.RecordLocation{recordNode.RecordInfo}
	for recordNode.Next != nil {
		recordNode = recordNode.Next
		res = append(res, recordNode.RecordInfo)
	}
	return res
}

type SearchConfig struct {
	Type   string // RangeQuery or Value query
	Values []uint32
}

const RangeQuery string = "range"
const ValueQuery string = "value"
