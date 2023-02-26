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
	fmt.Println("\nINSERTING KEY ", val)
	fmt.Println("Initial Tree root: ", tree.Root)
	if tree.Root == nil {
		fmt.Println("Current no tree root, creating a node now")
		leafNode := NewBPNode(true)
		leafNode.Keys = append(leafNode.Keys, val)
		recordPtr := &RecordLLNode{
			RecordInfo: recordLoc,
			Next:       nil,
		}

		leafNode.RecordPtrs = append(leafNode.RecordPtrs, recordPtr)
		tree.Root = leafNode
		fmt.Println("Initial Tree root Created: ", tree.Root)
		fmt.Println("--------------------------------")
	}

	fmt.Println("\nTree has existing root at: ", tree.Root, "\nPerform Insert now...")

	leafNode := tree.findLeafFromTree(val)
	newRoot := leafNode.InsertValIntoLeaf(recordLoc, val)
	if newRoot != nil {
		tree.Root = newRoot
	}
	fmt.Println("\n *****************************************")

}

func (tree *BPTree) findLeafFromTree(key uint32) *BPNode {
	fmt.Println("\nFinding leaf to insert now...")
	currNode := tree.Root
	foundChild := false
	for !currNode.IsLeaf {
		fmt.Println("Curretn Node", currNode)
		for keyIdx, keyVal := range currNode.Keys {
			fmt.Println("\nKey val, keyIdx", keyVal, "id", keyIdx)
			fmt.Println("idx", currNode.KeyPtrs[keyIdx])
			fmt.Println("idx=1", currNode.KeyPtrs[1])
			fmt.Println("idx=0", currNode.KeyPtrs[0])
			fmt.Println("idx=last", currNode.KeyPtrs[len(currNode.KeyPtrs)-1])

			if key <= keyVal {
				fmt.Println("KEY <= KEYVAL", key, keyVal)
				currNode = currNode.KeyPtrs[keyIdx]
				fmt.Println(currNode)
				if currNode.IsLeaf {
					fmt.Println("found!")
					foundChild = true
					break
				}

			}
		}
		if !foundChild {
			// fmt.Println("Debug 3")
			fmt.Println("\nfindleafcurrNode", currNode)
			// fmt.Println("crrNodes last ptrs", currNode.KeyPtrs[len(currNode.Keys)])
			if currNode.Next == nil {
				currNode = currNode.KeyPtrs[len(currNode.Keys)]
			} else {
				currNode = currNode.Next
			}

			// fmt.Println("2- current Node", currNode)

			// fmt.Println("Debug 3.2")
		}
	}
	// fmt.Println("Debug 4")

	// fmt.Println("Debug 5")

	fmt.Println("Node to be inserted to: ", currNode)
	fmt.Println("Node is leaf: ", currNode.IsLeaf)

	fmt.Println("Current Keys: ", currNode.Keys)
	fmt.Println("Current KeysPtrs: ", currNode.KeyPtrs)

	return currNode

}

func (node *BPNode) InsertValIntoLeaf(recordLoc *storage.RecordLocation, val uint32) *BPNode {

	if !node.IsLeaf {
		// return errors.New("[InsertValIntoLeaf] Node is not a leaf node")
		fmt.Println("Error: [InsertValIntoLeaf] Node is not a leaf node")
	}
	fmt.Println("Key ranges: ", node.Keys)
	fmt.Println("Val: ", val)
	for i, key := range node.Keys {
		if key == val {
			// Found existing key -> insert into linked list
			fmt.Println("Duplicate key! Append to linked list")
			record := node.RecordPtrs[i]
			record.InsertRecordToLinkedList(recordLoc)
			root := node.FindRoot()
			return root

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
	fmt.Println("\nFinding root node..")
	fmt.Println(node)
	fmt.Println(node.ParentNode)

	tempNode := node
	for tempNode.ParentNode != nil {
		tempNode = tempNode.ParentNode
		// fmt.Println(tempNode)
	}

	rootNode := tempNode
	fmt.Println("Current root is: ", rootNode)
	if len(rootNode.KeyPtrs) > 0 {
		fmt.Println("root first: ", rootNode.KeyPtrs[0])

		fmt.Println("root last!!", rootNode.KeyPtrs[len(rootNode.KeyPtrs)-1])
	}
	return rootNode
}

func getInsertIndex(keyList []uint32, val uint32) int {
	insertIndex := 0
	found := false
	for idx, key := range keyList {
		if val <= key {
			insertIndex = idx
			found = true
			break
		}
	}
	if !found {

		fmt.Println("not found, insert to last at ", len(keyList))
		fmt.Println("key list: ", keyList)
		insertIndex = len(keyList)
	}
	return insertIndex
}

func (node *BPNode) insertIntoLeafWithoutSplitting(recordLoc *storage.RecordLocation, val uint32) *BPNode {
	index := getInsertIndex(node.Keys, val)
	// nodeTemp := node
	fmt.Println("Index to be inserted in the leaf: ", index)
	var newKeyList []uint32
	var newRecordPtrs []*RecordLLNode

	origKeys := node.Keys

	// fmt.Println("\nAll current keys in node temp: ", node.Keys)
	// fmt.Println("Node now: ", node)
	// newKeyList = node.Keys[:index]
	// fmt.Println("New Keys left: ", newKeyList)

	// fmt.Println("\nAll current keys: ", node.Keys)
	// fmt.Println("Node now: ", node)
	// newKeyList2 := append(newKeyList, val)
	// fmt.Println("New Keys mid: ", newKeyList)
	// node.Keys = origKeys

	// fmt.Println("\nAll current keys: ", node.Keys)
	// fmt.Println("Orig keys: ", origKeys)
	// fmt.Println("Node now: ", node)
	// newKeyList3 := append(newKeyList2, node.Keys[index:]...)
	// fmt.Println("New Keys to append right: ", node.Keys[index:])
	// fmt.Println("New Keys right: ", newKeyList3)
	newKeyList = origKeys

	fmt.Println("next (before find root)", node.Next)

	fmt.Println("\nOrig keylist: ", newKeyList)
	fmt.Println("\nnext keylist: ", node.Next)
	var nextPreserve = []uint32{}
	if node.Next != nil {
		for idx, key := range node.Next.Keys {
			nextPreserve = append(nextPreserve, key)
			fmt.Println(idx)
		}
	}

	fmt.Println("\nnextpreserve", nextPreserve)

	if len(newKeyList) == index {
		newKeyList = append(newKeyList, val)
		fmt.Println("\nnextpreserve", nextPreserve)

		fmt.Println("!!!next (before find root)", node.Next)

	} else {
		newKeyList = append(newKeyList[:index+1], newKeyList[index:]...)
		newKeyList[index] = val
		fmt.Println("\nnextpreserve", nextPreserve)

		fmt.Println("!!!next (before find root)", node.Next)

	}
	if node.Next != nil {
		node.Next.Keys = nextPreserve
	}
	fmt.Println("Updated keylist: ", newKeyList)

	node.Keys = newKeyList
	fmt.Println("!!!next (before find root)", node.Next)

	newRecord := RecordLLNode{
		RecordInfo: recordLoc,
		Next:       nil,
	}
	newRecordPtrs = node.RecordPtrs[:index] //len 4
	fmt.Println("nNew Record Ptrs left: ", newRecordPtrs)
	newRecordPtrs = append(newRecordPtrs, &newRecord) //len 5
	fmt.Println("New Record Ptrs mid: ", newRecordPtrs)

	//panic: runtime error: slice bounds out of range [4:3]
	newRecordPtrs = append(newRecordPtrs, node.RecordPtrs[index:]...)
	fmt.Println("New Record Ptrs right: ", newRecordPtrs)

	node.RecordPtrs = newRecordPtrs
	fmt.Println("next (before find root)", node.Next)

	return node
}

func (node *BPNode) insertIntoLeafWithSplit(recordLoc *storage.RecordLocation, val uint32) *BPNode {
	index := getInsertIndex(node.Keys, val)
	fmt.Println("Insert at index: ", index)
	var (
		allKeysList   []uint32
		allRecordPtrs []*RecordLLNode
	)

	fmt.Println("Original All key list: ", node.Keys)

	allKeysList = node.Keys[:index]

	if len(allKeysList) == index {
		allKeysList = append(allKeysList, val)
	} else {
		allKeysList = append(allKeysList[:index+1], allKeysList[index:]...)
		allKeysList[index] = val
	}
	// allKeysList = append(allKeysList, val)
	// allKeysList = append(allKeysList, node.Keys[index:]...)
	fmt.Println("New All key list: ", node.Keys)

	newRecord := RecordLLNode{
		RecordInfo: recordLoc,
		Next:       nil,
	}
	allRecordPtrs = node.RecordPtrs[:index]
	// fmt.Println("1 All recordptrslist: ", allRecordPtrs)
	allRecordPtrs = append(allRecordPtrs, &newRecord)
	// fmt.Println("All key list: ", node.Keys)

	// fmt.Println("1 All recordptrslist: ", allRecordPtrs)
	// allRecordPtrs = append(allRecordPtrs, node.RecordPtrs[index:]...)
	// fmt.Println("All key list: ", node.Keys)

	numOfLeftKeys := math.Ceil((float64(MAX_NUM_KEYS) + 1) / 2)
	// fmt.Println("All key list: ", node.Keys)

	// Current node will be made as the left node
	rightNode := NewBPNode(node.IsLeaf)
	// newParentNode := NewBPNode(false)

	// newRightNode.Next = node.Next
	// node.Next = newRightNode
	fmt.Println("All key list: ", node.Keys)
	fmt.Print("Nof left keys: ", numOfLeftKeys)
	fmt.Print("All record ptrs: ", allRecordPtrs)
	allKeysListCopy := node.Keys
	allRecordPtrsCopy := node.RecordPtrs

	rightNodeNew := false
	if index <= MAX_NUM_KEYS {
		rightNodeNew = false
	} else {
		rightNodeNew = true
	}

	fmt.Println("new node keys: ", allKeysListCopy[int(numOfLeftKeys):])
	rightNode.Keys = allKeysListCopy[int(numOfLeftKeys):]
	rightNode.RecordPtrs = allRecordPtrsCopy[int(numOfLeftKeys):]
	leftNode := node
	leftNode.Keys = allKeysList[:int(numOfLeftKeys)]
	leftNode.RecordPtrs = allRecordPtrs[:int(numOfLeftKeys)]

	fmt.Print("New  Node info: ", rightNode)

	fmt.Println("...updating current node info")
	// fmt.Println("All key list: ", allKeysList)
	// fmt.Println("All recordptrslist: ", allRecordPtrs)

	// fmt.Println(node.Keys)

	/// update parent node for the new RightNode

	oldParentNode := node.ParentNode
	leftNode.Next = rightNode

	newNode := NewBPNode(false)
	if rightNodeNew {
		newNode = rightNode
		fmt.Println("New node is at the right")
	} else {
		newNode = leftNode
		fmt.Println("New node is at the left")
	}

	rightNode.ParentNode = node.ParentNode
	leftNode.ParentNode = node.ParentNode
	fmt.Println("new node is: ", newNode)

	fmt.Println("Old parent node: ", oldParentNode)
	fmt.Println("\nRight Node", rightNode)

	rootNode, tempReturn := node.insertKeyIntoParent(rightNode)
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
		fmt.Println("newly created root", newRoot)

		newRoot.KeyPtrs = []*BPNode{node, newNode}

		node.ParentNode = newRoot
		newNode.ParentNode = newRoot
		fmt.Println("first key", newRoot.KeyPtrs[0])
		fmt.Println("last key", newRoot.KeyPtrs[1])
		return newRoot, loopAgain

	} else if !node.ParentNode.isFull() {
		loopAgain = false
		fmt.Println(loopAgain)

		// Insert into parent without splitting
		fmt.Println("Old parent node is not full, can modify direcly")
		fmt.Println("current node's parent", node.ParentNode)
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

		fmt.Println("current node", currentNode)
		fmt.Println("new added aprent node", newNode)

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
	// fmt.Println("Old parent node is not full, can modify direcly")
	fmt.Println("Node to be inserted to parent, ", insertNode)
	val := insertNode.Keys[0]
	fmt.Println("keyt[0] ", val)
	fmt.Println("insert parent, ", insertNode.ParentNode)
	fmt.Println("node parent, ", node.ParentNode)
	index := getInsertIndex(node.ParentNode.Keys, val)

	fmt.Println("Index to be inserted ", index)
	fmt.Println("\nOrig insertnode parent: ", insertNode.ParentNode)
	origKeyList := insertNode.ParentNode.Keys
	origKeyPtrsList := insertNode.ParentNode.KeyPtrs

	newKeyList := origKeyList
	fmt.Println("Orig keylist: ", newKeyList)

	fmt.Println("\nOrig key list (parent): ", origKeyList)
	fmt.Println("Orig keyptr list(parent): ", origKeyPtrsList)
	if len(newKeyList) == index {
		newKeyList = append(newKeyList, val)
	} else {
		newKeyList = append(newKeyList[:index+1], newKeyList[index:]...)
		newKeyList[index] = val
	}
	// fmt.Println("\nOrig key list (parent): ", origKeyList)
	// fmt.Println("Orig keyptr list(parent): ", origKeyPtrsList)
	newKeyPtrsList := origKeyPtrsList
	if len(newKeyList) == index {
		newKeyPtrsList = append(newKeyPtrsList, insertNode)
	} else {
		newKeyPtrsList = append(newKeyPtrsList[:index+1], newKeyPtrsList[index:]...)
		fmt.Println("Orig keyptr list(temp parent): ", newKeyPtrsList[:index+1], newKeyPtrsList[index:])
		newKeyPtrsList[index+1] = insertNode
	}

	// node.ParentNode.Keys = append(node.ParentNode.Keys, insertNode.Keys[0])

	// node.ParentNode.KeyPtrs = append(node.ParentNode.KeyPtrs, insertNode)

	node.ParentNode.Keys = newKeyList
	node.ParentNode.KeyPtrs = newKeyPtrsList
	fmt.Println("new key list (parent)", node.ParentNode.Keys)
	fmt.Println("new key PTR list (parent)", node.ParentNode.KeyPtrs)
	fmt.Println("new key first child (parent)", node.ParentNode.KeyPtrs[0])

	// node.ParentNode.RecordPtrs = append(node.ParentNode.RecordPtrs, insertNode.RecordPtrs[0])
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
	fmt.Println("\nUpdated left parent", node.ParentNode)

	newRightParentNode := NewBPNode(false)
	newRightParentNode.Keys = allKeys[int(numOfLeftKeys)+1:]
	newRightParentNode.KeyPtrs = allKeyPtrs[int(numOfLeftKeys)+1:]
	newRightParentNode.ParentNode = node.ParentNode
	fmt.Println("\nUpdated left parent", node.ParentNode)
	fmt.Println("Updated right parent", newRightParentNode)

	node.ParentNode.Next = newRightParentNode
	// insertNode.ParentNode = newRightParentNode
	insertNode.ParentNode = node.ParentNode.ParentNode
	fmt.Println("\nUpdated left parent's parent", node.ParentNode.ParentNode)
	fmt.Println("Updated right parent's parent", newRightParentNode.ParentNode)

	return newRightParentNode

}

func (tree *BPTree) Delete(key uint32) {
	fmt.Print("DELETING KEY ", key, "......")
	leafNode := tree.findLeafFromTree(key)
	keyExists := false
	for idx, k := range leafNode.Keys {
		// fmt.Println("k: ", k)
		fmt.Println("idx:k", idx, k)

		if uint32(k) == key {
			keyExists = true
		}
	}
	fmt.Println("keyExists: ", keyExists)
	if keyExists {
		tree.deleteKey(leafNode, key)
	}
}
func (tree *BPTree) deleteKey(node *BPNode, key uint32) {
	node.deleteKeyFromNode(key)
	// // If the leafNode is the root node, we need to check if the leafNode has any keys
	if tree.Root == node {
		fmt.Println("Key exists in the root! ")
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
	tempKeys := make([]uint32, len(node.Keys)+len(mergeIntoNode.Keys))
	mergeIntoKeyLen := len(mergeIntoNode.Keys)
	nodeKeyLen := len(node.Keys)
	// fmt.Println("\nmergeIntoKeyLen", mergeIntoKeyLen)
	// fmt.Println("mergeIntoKeyLen", mergeIntoNode.Keys)
	// fmt.Println("mergeIntoNodes", node)

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

func (tree *BPTree) NumLevels() int {
	cursor := tree.Root
	numLevels := 0

	if tree.Root == nil {
		return 0
	} else {
		numLevels = 1
	}

	// B+ tree is balanced, every root to leaf path has the same height
	for !cursor.IsLeaf {
		cursor = cursor.KeyPtrs[0]
		numLevels++
	}

	return numLevels
}

func (tree *BPTree) NumNodes() int {
	root := tree.Root

	// Check empty tree
	if root == nil {
		return 0
	}

	children := tree.Root.KeyPtrs

	count := 1
	for {
		if len(children) == 0 {
			break
		}

		for _, value := range children {
			// if empty child, skip.
			if value == nil {
				continue
			} else {
				count++
			}
			// if not leaf, append node's children
			if value.IsLeaf == false {
				children = append(children, value.KeyPtrs...)
			}
		}
	}

	return count
}
