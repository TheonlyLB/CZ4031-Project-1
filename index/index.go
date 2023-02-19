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
		fmt.Println("No existing root, create a new root")
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
	return currNode
}

// func (tree *BPTree) findLeafFromTree(key uint32) *BPNode {
// 	fmt.Println("\nFinding leaf to insert now...")

// 	currNode := tree.Root
// 	foundChild := false
// 	potentialLeaf := NewBPNode(false)
// 	targetedLeaf := NewBPNode(false)
// 	fmt.Println("Current node:", currNode)
// 	for !currNode.IsLeaf {
// 		for keyIdx, keyVal := range currNode.Keys {
// 			fmt.Println("Debug1", keyIdx, keyVal, key)
// 			fmt.Println(currNode.Keys)
// 			if key < keyVal {
// 				fmt.Println("Debug 2")
// 				potentialLeaf = currNode.KeyPtrs[keyIdx]
// 				if potentialLeaf.IsLeaf {
// 					foundChild = true
// 					targetedLeaf = potentialLeaf
// 					break
// 				}
// 			} else {
// 				fmt.Println("next")
// 			}
// 		}

// 		if len(potentialLeaf.Keys) == 0 {
// 			potentialLeaf = currNode.KeyPtrs[len(currNode.Keys)]
// 			if potentialLeaf.IsLeaf {
// 				foundChild = true
// 			}
// 		}
// 		if !foundChild {
// 			currNode = potentialLeaf
// 			// fmt.Println("Current node:", currNode)

// 			// targetedLeaf = potentialLeaf.findLeafFromNode(key)
// 		}
// 	}
// 	fmt.Println("Node to be inserted to: ", currNode)
// 	fmt.Println("Current Keys: ", currNode.Keys)
// 	fmt.Println("Current KeysPtrs: ", currNode.KeyPtrs)
// 	return targetedLeaf
// }

// foundChild := false
// for keyIdx, keyVal := range currNode.Keys {
// 	fmt.Println("Debug 2")
// 	if key < keyVal {
// 		currNode = currNode.KeyPtrs[keyIdx]
// 		fmt.Println(currNode)
// 		foundChild = true
// 		break
// 	}
// }
// if !foundChild {
// 	fmt.Println("Debug 3")
// 	currNode = currNode.KeyPtrs[len(currNode.Keys)]
// 	fmt.Println("Debug 3.2")
// }
// fmt.Println("Debug 4")

// fmt.Println("Debug 5")

// fmt.Println("Node to be inserted to: ", currNode)
// fmt.Println("Current Keys: ", currNode.Keys)
// fmt.Println("Current KeysPtrs: ", currNode.KeyPtrs)

// return currNode

// func (node *BPNode) findLeafFromNode(key uint32) *BPNode {
// 	/* Use this function when the existing tree has more than 1 layer
// 	will return the targetted node from the base level (i.e., leaf node)
// 	*/

// 	fmt.Println("\nLocating base level leaf...")
// 	foundChild := false
// 	potentialLeaf := NewBPNode(false)
// 	targetedLeaf := NewBPNode(false)

// 	for keyIdx, keyVal := range node.Keys {
// 		if key < keyVal {
// 			potentialLeaf = node.KeyPtrs[keyIdx]
// 			if potentialLeaf.IsLeaf {
// 				foundChild = true
// 				targetedLeaf = potentialLeaf
// 				break
// 			}
// 		}
// 	}
// 	if !foundChild {
// 		potentialLeaf.findLeafFromNode(key)
// 	}

// 	return targetedLeaf
// }

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

	// if !oldParentNode.isFull() {
	// 	// fmt.Println("Old parent node is not full, can modify direcly")
	// 	// node.ParentNode.Keys = append(node.ParentNode.Keys, newRightNode.Keys[0])
	// 	// node.ParentNode.KeyPtrs = append(node.ParentNode.KeyPtrs, newRightNode)
	// 	// node.ParentNode.RecordPtrs = append(node.ParentNode.RecordPtrs, newRightNode.RecordPtrs[0])
	// 	// fmt.Println("\nUpdated parent node: ", node.ParentNode)
	// 	node.insertKeyIntoParent(newR)

	// } else {
	// 	fmt.Println("Old parent node is full, need to split the parent node")
	// 	node.insertIntoParentWithSplit(newRightNode)
	// }

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

		// node.ParentNode.Keys = append(node.ParentNode.Keys, newNode.Keys[0])

		// node.ParentNode.KeyPtrs = append(node.ParentNode.KeyPtrs, newNode)
		// node.ParentNode.RecordPtrs = append(node.ParentNode.RecordPtrs, newNode.RecordPtrs[0])
		// fmt.Println("Updated parent node: ", node.ParentNode)
		// // index := getInsertIndex(node.Keys, newKey)
		// // var (
		// // 	newKeyList    []uint32
		// // 	newKeyPtrList []*BPNode
		// // )
		// // newKeyList = node.Keys[:index]
		// // newKeyList = append(newKeyList, newKey)
		// // newKeyList = append(newKeyList, node.Keys[index:]...)
		// // node.Keys = newKeyList

		// // newKeyPtrList = node.KeyPtrs[:index]
		// // newKeyPtrList = append(newKeyPtrList, newNode)
		// // newKeyPtrList = append(newKeyPtrList, node.KeyPtrs[index:]...)
		// // node.KeyPtrs = newKeyPtrList
		// // return // Need return anth?
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
	// index := getInsertIndex(node.Keys, newKey)
	// var (
	// 	newKeyList    []uint32
	// 	newKeyPtrList []*BPNode
	// )
	// newKeyList = node.Keys[:index]
	// newKeyList = append(newKeyList, newKey)
	// newKeyList = append(newKeyList, node.Keys[index:]...)
	// node.Keys = newKeyList

	// newKeyPtrList = node.KeyPtrs[:index]
	// newKeyPtrList = append(newKeyPtrList, newNode)
	// newKeyPtrList = append(newKeyPtrList, node.KeyPtrs[index:]...)
	// node.KeyPtrs = newKeyPtrList
	// return // Need return anth?
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

// func (node *BPNode) insertIntoParentWithSplit(insertNode *BPNode) {
// 	key := insertNode.Keys[0]
// 	index := getInsertIndex(node.Keys, key)

// 	var (
// 		allKeysList []uint32
// 		allKeyPtrs  []*BPNode
// 	)
// 	allKeysList = node.Keys[:index]
// 	allKeysList = append(allKeysList, key)
// 	allKeysList = append(allKeysList, node.Keys[index:]...)

// 	allKeyPtrs = node.KeyPtrs[:index]
// 	allKeyPtrs = append(allKeyPtrs, insertNode)
// 	allKeyPtrs = append(allKeyPtrs, node.KeyPtrs[index:]...)

// 	numOfLeftKeys := math.Ceil((float64(MAX_NUM_KEYS) + 1) / 2)

// 	// Current node will be made as the left node
// 	newRightNode := NewBPNode(node.IsLeaf)
// 	newParentNode := NewBPNode(false)

// 	newRightNode.Keys = allKeysList[int(numOfLeftKeys):]
// 	newRightNode.KeyPtrs = allKeyPtrs[int(numOfLeftKeys):]

// 	node.Keys = allKeysList[:int(numOfLeftKeys)]
// 	node.KeyPtrs = allKeyPtrs[int(numOfLeftKeys):]

// 	if node.ParentNode == nil {
// 		newParentNode.Keys = append(newParentNode.Keys, newRightNode.Keys[0])
// 		newParentNode.KeyPtrs = append(newParentNode.KeyPtrs, node, newRightNode)
// 		// return parent?
// 	} else {
// 		// insert newRightNode into parent
// 		node.ParentNode.insertKeyIntoParent(newRightNode)
// 	}
// }

func (node *BPNode) isFull() bool {
	return len(node.Keys) >= MAX_NUM_KEYS
}
