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

func (node *BPNode) isFull() bool {
	return len(node.Keys) >= MAX_NUM_KEYS
}
