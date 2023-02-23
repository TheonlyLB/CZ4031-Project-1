package index

import (
	"CZ4031_Project_1/storage"
	"errors"
	"fmt"
	"math"
)

func NewTree() *BPTree {
	return &BPTree{
		Root: nil,
	}
}

func (tree *BPTree) CreateIndex() *BPTree {
	index := BPTree{}
	return &index
}

func (tree *BPTree) Insert(recordLoc *storage.RecordLocation, val uint32) {
	// if no root, create leaf node -> insert record -> end

	if tree.Root == nil {
		fmt.Println("No existing root, create a new root")
		leafNode := NewBPNode(true)
		leafNode.Keys = append(leafNode.Keys, val)
		recordPtr := &RecordLLNode{
			RecordInfo: recordLoc,
			Next:       nil,
		}

		leafNode.RecordPtrs = append(leafNode.RecordPtrs, recordPtr)
		tree.Root = leafNode
		return
	}

	fmt.Println("Found existing root, perform Insert directly now...")

	leafNode := tree.findLeaf(val)
	leafNode.InsertValIntoLeaf(recordLoc, val)

}

func (tree *BPTree) findLeaf(key uint32) *BPNode {
	currNode := tree.Root

	for !currNode.IsLeaf {

		foundChild := false
		for keyIdx, keyVal := range currNode.Keys {
			if key < keyVal {
				currNode = currNode.KeyPtrs[keyIdx]
				foundChild = true
				break
			}
		}
		if !foundChild {
			currNode = currNode.KeyPtrs[len(currNode.Keys)]
		}
	}

	fmt.Println("Node to be inserted to: ", currNode)
	fmt.Println("Current Keys: ", currNode.Keys)
	fmt.Println("Current KeysPtrs: ", currNode.KeyPtrs)

	return currNode
}

func (node *BPNode) InsertValIntoLeaf(recordLoc *storage.RecordLocation, val uint32) error {
	if !node.IsLeaf {
		return errors.New("[InsertValIntoLeaf] Node is not a leaf node")
	}

	for i, key := range node.Keys {
		if key == val {
			// Found existing key -> insert into linked list
			record := node.RecordPtrs[i]
			record.InsertRecordToLinkedList(recordLoc)
			return nil
		}
	}

	if !node.isFull() {
		fmt.Println("\n...Current Node got space, insert directly! ")
		node.insertIntoLeafWithoutSplitting(recordLoc, val)
	} else {
		fmt.Println("\n...Current Node is Full, insert with split ")
		node.insertIntoLeafWithSplit(recordLoc, val)
	}

	return nil
}

func getInsertIndex(keyList []uint32, val uint32) int {
	insertIndex := 0
	found := false
	for idx, key := range keyList {
		if val < key {
			insertIndex = idx
			found = true
			break
		}
	}
	if !found {
		insertIndex = len(keyList)
	}
	return insertIndex
}

func (node *BPNode) insertIntoLeafWithoutSplitting(recordLoc *storage.RecordLocation, val uint32) {
	index := getInsertIndex(node.Keys, val)
	var (
		newKeyList    []uint32
		newRecordPtrs []*RecordLLNode
	)
	newKeyList = node.Keys[:index]
	newKeyList = append(newKeyList, val)
	newKeyList = append(newKeyList, node.Keys[index:]...)
	node.Keys = newKeyList

	newRecord := RecordLLNode{
		RecordInfo: recordLoc,
		Next:       nil,
	}
	newRecordPtrs = node.RecordPtrs[:index]
	newRecordPtrs = append(newRecordPtrs, &newRecord)
	newRecordPtrs = append(newRecordPtrs, node.RecordPtrs[index:]...)
	node.RecordPtrs = newRecordPtrs
}

func (node *BPNode) insertIntoLeafWithSplit(recordLoc *storage.RecordLocation, val uint32) {
	index := getInsertIndex(node.Keys, val)
	var (
		allKeysList   []uint32
		allRecordPtrs []*RecordLLNode
	)
	allKeysList = node.Keys[:index]
	allKeysList = append(allKeysList, val)
	allKeysList = append(allKeysList, node.Keys[index:]...)

	newRecord := RecordLLNode{
		RecordInfo: recordLoc,
		Next:       nil,
	}
	allRecordPtrs = node.RecordPtrs[:index]
	allRecordPtrs = append(allRecordPtrs, &newRecord)
	allRecordPtrs = append(allRecordPtrs, node.RecordPtrs[index:]...)

	numOfLeftKeys := math.Ceil((float64(MAX_NUM_KEYS) + 1) / 2)

	// Current node will be made as the left node
	newRightNode := NewBPNode(node.IsLeaf)
	// newParentNode := NewBPNode(false)

	// newRightNode.Next = node.Next
	// node.Next = newRightNode

	newRightNode.Keys = allKeysList[int(numOfLeftKeys):]
	newRightNode.RecordPtrs = allRecordPtrs[int(numOfLeftKeys):]

	fmt.Println("...updating current node info")
	fmt.Println("All key list: ", allKeysList)
	node.Keys = allKeysList[:int(numOfLeftKeys)]
	node.RecordPtrs = allRecordPtrs[int(numOfLeftKeys):]
	fmt.Println(node.Keys)

	/// update parent node for the new RightNode

	oldParentNode := node.ParentNode
	fmt.Println("\nOld parent node: ", oldParentNode)

	if !oldParentNode.isFull() {
		fmt.Println("Old parent node is not full, can modify direcly")
		node.ParentNode.Keys = append(node.ParentNode.Keys, newRightNode.Keys[0])
		node.ParentNode.KeyPtrs = append(node.ParentNode.KeyPtrs, newRightNode)
		node.ParentNode.RecordPtrs = append(node.ParentNode.RecordPtrs, newRightNode.RecordPtrs[0])
		fmt.Println("\nUpdated parent node: ", node.ParentNode)

	} else {
		fmt.Println("Old parent node is full, need to split the parent node")

	}

	node.Next = newRightNode
	newRightNode.ParentNode = node.ParentNode

	// if newRightNode.ParentNode == nil {
	// 	fmt.Println("\nNo existing parent node for the newly created node, create parent node now")

	// 	newParentNode.Keys = append(newParentNode.Keys, newRightNode.Keys[0])
	// 	newParentNode.KeyPtrs = append(newParentNode.KeyPtrs, node, newRightNode)
	// 	// return parent?
	// } else {
	// 	// insert newRightNode into parent
	// 	fmt.Println("Found existing parent node: ", node.ParentNode, "\nInsert to parent now")
	// 	node.ParentNode.insertKeyIntoParent(newRightNode)
	// }

}

func (node *BPNode) insertKeyIntoParent(newNode *BPNode) {
	// I think need to find index to insert again
	newKey := newNode.Keys[0]

	if !node.isFull() {
		// Insert into parent without splitting
		index := getInsertIndex(node.Keys, newKey)
		var (
			newKeyList    []uint32
			newKeyPtrList []*BPNode
		)
		newKeyList = node.Keys[:index]
		newKeyList = append(newKeyList, newKey)
		newKeyList = append(newKeyList, node.Keys[index:]...)
		node.Keys = newKeyList

		newKeyPtrList = node.KeyPtrs[:index]
		newKeyPtrList = append(newKeyPtrList, newNode)
		newKeyPtrList = append(newKeyPtrList, node.KeyPtrs[index:]...)
		node.KeyPtrs = newKeyPtrList
		return // Need return anth?
	} else {

	}

}

func (node *BPNode) insertIntoParentWithSplit(insertNode *BPNode) {
	key := insertNode.Keys[0]
	index := getInsertIndex(node.Keys, key)

	var (
		allKeysList []uint32
		allKeyPtrs  []*BPNode
	)
	allKeysList = node.Keys[:index]
	allKeysList = append(allKeysList, key)
	allKeysList = append(allKeysList, node.Keys[index:]...)

	allKeyPtrs = node.KeyPtrs[:index]
	allKeyPtrs = append(allKeyPtrs, insertNode)
	allKeyPtrs = append(allKeyPtrs, node.KeyPtrs[index:]...)

	numOfLeftKeys := math.Ceil((float64(MAX_NUM_KEYS) + 1) / 2)

	// Current node will be made as the left node
	newRightNode := NewBPNode(node.IsLeaf)
	newParentNode := NewBPNode(false)

	newRightNode.Keys = allKeysList[int(numOfLeftKeys):]
	newRightNode.KeyPtrs = allKeyPtrs[int(numOfLeftKeys):]

	node.Keys = allKeysList[:int(numOfLeftKeys)]
	node.KeyPtrs = allKeyPtrs[int(numOfLeftKeys):]

	if node.ParentNode == nil {
		newParentNode.Keys = append(newParentNode.Keys, newRightNode.Keys[0])
		newParentNode.KeyPtrs = append(newParentNode.KeyPtrs, node, newRightNode)
		// return parent?
	} else {
		// insert newRightNode into parent
		node.ParentNode.insertKeyIntoParent(newRightNode)
	}
}

func (tree *BPTree) Delete(key uint32) {
	leafNode := tree.findLeaf(key)
	tree.deleteKey(leafNode, key)

}

func (tree *BPTree) deleteKey(node *BPNode, key uint32) {
	node.deleteKeyFromNode(key)
	// // If the leafNode is the root node, we need to check if the leafNode has any keys
	if tree.Root == node {
		if len(node.Keys) == 0 {
			tree.Root = nil
		}
		return
	}
	node.rebalance()
}

// Delete a key from node, and edit parent key if needed
func (node *BPNode) deleteKeyFromNode(key uint32) {
	var (
		deleteIndex int = -1
	)

	for idx, k := range node.Keys {
		if k == key {
			deleteIndex = idx
			break
		}
	}
	// If deleteIndex == -1, means not found
	if deleteIndex == -1 {
		panic("Key does not exist, nothing to delete")
	}
	node.Keys = deleteAtIndex(node.Keys, deleteIndex)
	if node.IsLeaf {
		node.RecordPtrs = deleteAtIndex(node.RecordPtrs, deleteIndex)
	} else {
		// Delete index +1 because there is 1 more keyptr than key
		node.KeyPtrs = deleteAtIndex(node.KeyPtrs, deleteIndex+1)
	}

	// If there is a parent node, and the index we delete is at 0, need update parent key
	if node.ParentNode != nil && deleteIndex == 0 {
		for i, k := range node.ParentNode.Keys {
			if k == key {
				node.ParentNode.Keys[i] = node.Keys[0]
				break
			}
		}
	}
}



// Rebalances the node by either borrowing from neighbours or merging with neighbours
func (node *BPNode) rebalance() {
	var threshold int
	if node.IsLeaf {
		threshold = int(math.Floor(float64(MAX_NUM_KEYS+1) / 2))
	} else {
		threshold = int(math.Floor(float64(MAX_NUM_KEYS) / 2))
	}

	// Enough keys
	if len(node.Keys) >= threshold {
		return
	}

	neighbour, isLeft, ok := canBorrowFromNeighbour(node, threshold)
	if ok {
		// Can borrow
		node.BorrowKeyFromNode(neighbour, isLeft)
	} else {
		// Have to merge
		node.Merge(neighbour, isLeft)
	}
}

func (node *BPNode) Merge(mergeIntoNode *BPNode, isLeft bool) {
	tempKeys := make([]uint32, len(node.Keys))
	mergeIntoKeyLen := len(mergeIntoNode.Keys)
	nodeKeyLen := len(node.Keys)
	if node.IsLeaf {
		tempPtrs := make([]*RecordLLNode, len(node.RecordPtrs))
		if isLeft {
			copy(tempKeys[:mergeIntoKeyLen], mergeIntoNode.Keys[:mergeIntoKeyLen])
			copy(tempKeys[mergeIntoKeyLen:], node.Keys)

			copy(tempPtrs[:mergeIntoKeyLen], mergeIntoNode.RecordPtrs[:mergeIntoKeyLen])
			copy(tempPtrs[mergeIntoKeyLen:], node.RecordPtrs)

			// Fix next pointer
			for _, item := range node.ParentNode.KeyPtrs {
				if item == nil {
					break
				}

				if item.Next == mergeIntoNode {
					item.Next = node
					break
				}
			}
		} else {
			copy(tempKeys[:nodeKeyLen], node.Keys[:nodeKeyLen])
			copy(tempKeys[mergeIntoKeyLen:], mergeIntoNode.Keys[:mergeIntoKeyLen])
			copy(tempPtrs[:nodeKeyLen], node.RecordPtrs[:nodeKeyLen])
			copy(tempPtrs[mergeIntoKeyLen:], mergeIntoNode.RecordPtrs[:mergeIntoKeyLen])
			node.Next = mergeIntoNode.Next
		}

		node.Keys = tempKeys
		node.RecordPtrs = tempPtrs

		var deleteKey uint32
		for i, item := range mergeIntoNode.ParentNode.KeyPtrs {
			if item == mergeIntoNode {
				if isLeft {
					deleteKey = mergeIntoNode.ParentNode.Keys[i]
					node.ParentNode.KeyPtrs[i] = node
				} else {
					deleteKey = mergeIntoNode.ParentNode.Keys[i-1]
					node.ParentNode.KeyPtrs[i] = node
				}
			}
		}

		mergeIntoNode.ParentNode.deleteKeyFromNode(deleteKey)
	}
}

// Transfers one key from borrowNode
func (node *BPNode) BorrowKeyFromNode(borrowNode *BPNode, isLeft bool) {
	var (
		insertIndex          int
		removeIndex          int
		parentKey            uint32
		parentReplacementKey uint32
	)

	if isLeft {
		// Last item of borrowNode becomes first item of node
		insertIndex = 0
		removeIndex = len(borrowNode.Keys) - 1
		parentKey = node.Keys[0]
		parentReplacementKey = borrowNode.Keys[len(borrowNode.Keys)-1]

	} else {
		// First item of borrowNode becomes last item of node
		insertIndex = len(node.Keys) - 1
		removeIndex = 0
		parentKey = borrowNode.Keys[0]
		parentReplacementKey = borrowNode.Keys[1]
	}

	node.Keys = insertAtIndex(node.Keys, borrowNode.Keys[removeIndex], insertIndex)
	borrowNode.Keys = deleteAtIndex(borrowNode.Keys, removeIndex)

	if node.IsLeaf {
		node.RecordPtrs = insertAtIndex(node.RecordPtrs, borrowNode.RecordPtrs[removeIndex], insertIndex)
		borrowNode.RecordPtrs = deleteAtIndex(borrowNode.RecordPtrs, removeIndex)

		for i, k := range node.ParentNode.Keys {
			if k == parentKey {
				node.ParentNode.Keys[i] = parentReplacementKey
				break
			}
		}
	} else {
		if isLeft {
			node.KeyPtrs = insertAtIndex(node.KeyPtrs, borrowNode.KeyPtrs[removeIndex+1], insertIndex)
		} else {
			node.KeyPtrs = insertAtIndex(node.KeyPtrs, borrowNode.KeyPtrs[removeIndex+1], insertIndex+1)
		}
		borrowNode.KeyPtrs = deleteAtIndex(borrowNode.KeyPtrs, removeIndex+1)

		//Fix parent's key
		for i, k := range node.ParentNode.KeyPtrs {
			if k == node {
				if isLeft {
					temp := node.ParentNode.Keys[i-1]
					node.ParentNode.Keys[i-1] = parentReplacementKey
					node.Keys[0] = temp
				} else {
					temp := node.ParentNode.Keys[i]
					node.ParentNode.Keys[i] = parentReplacementKey
					node.Keys[len(node.Keys)-1] = temp
				}
				break
			}
		}
	}

}

// ok - whether can borrow or not
// isLeft - whether the node is left or right neighbour
// neighbour - either left neighbour or right neighbour
func canBorrowFromNeighbour(node *BPNode, threshold int) (neighbour *BPNode, isLeft bool, ok bool) {
	parent := node.ParentNode
	var leftNeighbour, rightNeighbour *BPNode

	for i, keyPtr := range parent.KeyPtrs {
		if keyPtr == node {
			if i != 0 {
				leftNeighbour = parent.KeyPtrs[i-1]
				break
			} else if i < len(parent.KeyPtrs)-1 {
				rightNeighbour = parent.KeyPtrs[i+1]
			}
		}
	}

	// not leftmost
	if leftNeighbour != nil {
		neighbour = leftNeighbour
		isLeft = true

		if len(leftNeighbour.Keys) > threshold {
			// can borrow from left neighbour
			ok = true
			return
		}
	}

	// not rightmost
	if rightNeighbour != nil {
		neighbour = rightNeighbour
		isLeft = false

		if len(rightNeighbour.Keys) > threshold {
			ok = true
			return
		}
	}
	ok = false
	// if cannot borrow, neighbour will be rightnode
	return
}

// helper function to remove node/addr/key into their slice at target index
func deleteAtIndex[T *BPNode | *RecordLLNode | uint32](arr []T, target int) []T {
	newArray := make([]T, 0)
	for i, val := range arr {
		if i == target {
			continue
		}
		newArray = append(newArray, val)
	}
	return newArray
}

// helper function to insert node/addr/key into their slice at target index
func insertAtIndex[T *BPNode | *RecordLLNode | uint32](arr []T, value T, target int) []T {
	newArray := make([]T, 0)
	// Shift 1 position down the array
	for i, val := range arr {
		if i == target {
			newArray = append(newArray, value)
		}
		newArray = append(newArray, val)
	}
	return newArray
}