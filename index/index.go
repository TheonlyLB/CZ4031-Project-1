package index

import (
	"CZ4031_Project_1/storage"
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
		leafNode := NewBPNode(true)
		leafNode.Keys = append(leafNode.Keys, val)
		recordPtr := &RecordLLNode{
			RecordInfo: recordLoc,
			Next:       nil,
		}

		leafNode.RecordPtrs = append(leafNode.RecordPtrs, recordPtr)
		tree.Root = leafNode
	}

	fmt.Println("Tree has existing root, perform Insert now...")

	leafNode := tree.findLeafFromTree(val)
	newRoot := leafNode.InsertValIntoLeaf(recordLoc, val)
	if newRoot != nil {
		tree.Root = newRoot
	}

}

func (tree *BPTree) findLeafFromTree(key uint32) *BPNode {
	fmt.Println("\nFinding leaf to insert now...")
	currNode := tree.Root
	foundChild := false
	for !currNode.IsLeaf {
		// fmt.Println("Curretn Node", currNode)
		for keyIdx, keyVal := range currNode.Keys {
			// fmt.Println("Debug 2")
			if key < keyVal {
				currNode = currNode.KeyPtrs[keyIdx]
				fmt.Println(currNode)
				if currNode.IsLeaf {
					fmt.Println("found!")
					foundChild = true
				}
				break
			}
		}
		if !foundChild {
			// fmt.Println("Debug 3")
			currNode = currNode.KeyPtrs[len(currNode.Keys)]
			// fmt.Println("2- current Node", currNode)

			// fmt.Println("Debug 3.2")
		}
	}
	// fmt.Println("Debug 4")

	// fmt.Println("Debug 5")

	fmt.Println("Node to be inserted to: ", currNode)
	fmt.Println("Current Keys: ", currNode.Keys)
	fmt.Println("Current KeysPtrs: ", currNode.KeyPtrs)
	if !foundChild {
		fmt.Println("\nKey", key, "is not found in the tree!!! \nSKIP and EXIT now....")
		return nil
	} else {
		return currNode
	}
}

func (node *BPNode) InsertValIntoLeaf(recordLoc *storage.RecordLocation, val uint32) *BPNode {

	if !node.IsLeaf {
		// return errors.New("[InsertValIntoLeaf] Node is not a leaf node")
		fmt.Println("Error: [InsertValIntoLeaf] Node is not a leaf node")
	}

	for i, key := range node.Keys {
		if key == val {
			// Found existing key -> insert into linked list
			record := node.RecordPtrs[i]
			record.InsertRecordToLinkedList(recordLoc)
			return nil
		}
	}

	root := NewBPNode(false)
	returnNode := NewBPNode(false)

	if !node.isFull() {
		fmt.Println("\n...Current Node got space, insert directly! ")
		fmt.Println("Current node", node)
		returnNode = node.insertIntoLeafWithoutSplitting(recordLoc, val)
	} else {
		fmt.Println("\n...Current Node is Full, insert with split ")
		returnNode = node.insertIntoLeafWithSplit(recordLoc, val)
	}

	root = returnNode.FindRoot()

	return root

}

func (node *BPNode) FindRoot() *BPNode {
	fmt.Println("Finding root node..")
	fmt.Println(node)
	fmt.Println(node.ParentNode)

	tempNode := node
	for tempNode.ParentNode != nil {
		tempNode = tempNode.ParentNode
		// fmt.Println(tempNode)
	}

	rootNode := tempNode
	fmt.Println("Current root is: ", rootNode)
	return rootNode
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

func (node *BPNode) insertIntoLeafWithoutSplitting(recordLoc *storage.RecordLocation, val uint32) *BPNode {
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

	return node
}

func (node *BPNode) insertIntoLeafWithSplit(recordLoc *storage.RecordLocation, val uint32) *BPNode {
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
	// fmt.Println("1 All recordptrslist: ", allRecordPtrs)
	allRecordPtrs = append(allRecordPtrs, &newRecord)
	// fmt.Println("1 All recordptrslist: ", allRecordPtrs)
	// allRecordPtrs = append(allRecordPtrs, node.RecordPtrs[index:]...)

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
	fmt.Println("All recordptrslist: ", allRecordPtrs)

	node.Keys = allKeysList[:int(numOfLeftKeys)]
	node.RecordPtrs = allRecordPtrs[:int(numOfLeftKeys)]
	// fmt.Println(node.Keys)

	/// update parent node for the new RightNode

	oldParentNode := node.ParentNode
	node.Next = newRightNode
	newRightNode.ParentNode = node.ParentNode
	// fmt.Println("old paremt node ptrs: ", &oldParentNode.KeyPtrs)

	fmt.Println("Old parent node: ", oldParentNode)
	fmt.Println("\nFirst new Right Node", newRightNode)

	rootNode, tempReturn := node.insertKeyIntoParent(newRightNode)
	fmt.Println(tempReturn)

	return rootNode
}

func (node *BPNode) insertKeyIntoParent(newNode *BPNode) (*BPNode, bool) {
	// I think need to find index to insert again
	// newKey := newNode.Keys[0]
	loopAgain := false
	if node.ParentNode == nil {
		loopAgain = false
		fmt.Println(loopAgain)
		fmt.Println("Old parent node was a root node, need to set a new root node and update tree")
		newRoot := NewBPNode(false)
		newRoot.Keys = []uint32{newNode.Keys[0]}
		newRoot.KeyPtrs = []*BPNode{node, newNode}
		node.ParentNode = newRoot
		newNode.ParentNode = newRoot
		return newRoot, loopAgain

	} else if !node.ParentNode.isFull() {
		loopAgain = false
		fmt.Println(loopAgain)

		// Insert into parent without splitting
		fmt.Println("Old parent node is not full, can modify direcly")
		newParent := node.insertIntoParentWithoutSplit(newNode)

		return newParent, loopAgain

	} else {
		loopAgain = true
		fmt.Println(loopAgain)

		fmt.Println("Old parent node is full, need to split")
		currentNode := node
		// currentNode.insertKeyIntoParent(newNode)
		// currentNode.insertIntoParentWithSplit(newNode)
		fmt.Println("currentNode: ", currentNode)
		newAddedParent := currentNode.insertIntoParentWithSplit(newNode)
		currentNode = currentNode.ParentNode
		newNode = newAddedParent

		newParentTemp := NewBPNode(false)
		for loopAgain {
			fmt.Println("-----loop")
			newParentTemp, loopAgain = currentNode.insertKeyIntoParent(newNode)
			currentNode = currentNode.ParentNode
			newNode = newParentTemp
		}

		returnNode := newParentTemp

		return returnNode, loopAgain
	}
}

func (node *BPNode) insertIntoParentWithoutSplit(insertNode *BPNode) *BPNode {
	fmt.Println("Old parent node is not full, can modify direcly")
	node.ParentNode.Keys = append(node.ParentNode.Keys, insertNode.Keys[0])

	node.ParentNode.KeyPtrs = append(node.ParentNode.KeyPtrs, insertNode)
	node.ParentNode.RecordPtrs = append(node.ParentNode.RecordPtrs, insertNode.RecordPtrs[0])
	fmt.Println("Updated parent node: ", node.ParentNode)

	return node.ParentNode
}

func (node *BPNode) insertIntoParentWithSplit(insertNode *BPNode) *BPNode {
	numOfLeftKeys := math.Ceil((float64(MAX_NUM_KEYS) + 1) / 2)
	fmt.Println("num of left keys: ", numOfLeftKeys)
	allKeys := node.ParentNode.Keys
	allKeyPtrs := insertNode.ParentNode.KeyPtrs
	// fmt.Println("1. All keys or current parent ", allKeys)
	// fmt.Println("parents keyptrs", allKeyPtrs)

	allKeys = append(allKeys, insertNode.Keys[0])
	allKeyPtrs = append(allKeyPtrs, insertNode)
	fmt.Println("All keys for parent ", allKeys)
	fmt.Println("All key pptrs for parent ", allKeyPtrs)

	node.ParentNode.Keys = allKeys[:int(numOfLeftKeys)]
	node.ParentNode.KeyPtrs = allKeyPtrs[:int(numOfLeftKeys)+1]
	fmt.Println("\nUpdated old parent", node.ParentNode)

	newRightParentNode := NewBPNode(false)
	newRightParentNode.Keys = allKeys[int(numOfLeftKeys)+1:]
	newRightParentNode.KeyPtrs = allKeyPtrs[int(numOfLeftKeys)+1:]
	newRightParentNode.ParentNode = node.ParentNode

	node.ParentNode.Next = newRightParentNode
	insertNode.ParentNode = newRightParentNode

	return newRightParentNode

}

func (tree *BPTree) Delete(key uint32) {
	leafNode := tree.findLeafFromTree(key)
	if leafNode == nil {
		return
	} else {
		tree.deleteKey(leafNode, key)
	}
}

func (tree *BPTree) deleteKey(node *BPNode, key uint32) {
	node.deleteKeyFromNode(key)
	// // If the leafNode is the root node, we need to check if the leafNode has any keys
	if tree.Root == node {
		keyLen := len(node.Keys)
		if keyLen == 0 && node.IsLeaf {
			tree.Root = nil
		} else if keyLen == 0 && !node.IsLeaf {
			tree.Root = node.KeyPtrs[0]
			tree.Root.ParentNode = nil
			node.ParentNode = nil
		}
		return
	}
	tree.rebalance(node)
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
func (tree *BPTree) rebalance(node *BPNode) {
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
		tree.Merge(node, neighbour, isLeft)
	}
}

func (tree *BPTree) Merge(node *BPNode, mergeIntoNode *BPNode, isLeft bool) {
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

		tree.deleteKey(mergeIntoNode.ParentNode, deleteKey)
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
