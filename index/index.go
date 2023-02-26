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
	fmt.Println("Key ranges: ", node.Keys)
	fmt.Println("Val: ", val)
	for i, key := range node.Keys {
		if key == val {
			// Found existing key -> insert into linked list
			fmt.Println("Duplicate key! Append to linked list")
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
	// nodeTemp := node
	fmt.Println("Index: i", index)
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
	fmt.Println("\nOrig keylist: ", newKeyList)
	if len(newKeyList) == index {
		newKeyList = append(newKeyList, val)
	} else {
		newKeyList = append(newKeyList[:index+1], newKeyList[index:]...)
		newKeyList[index] = val
	}

	fmt.Println("Updated keylist: ", newKeyList)

	node.Keys = newKeyList

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
	fmt.Println("All key list: ", node.Keys)

	// fmt.Println("1 All recordptrslist: ", allRecordPtrs)
	// allRecordPtrs = append(allRecordPtrs, node.RecordPtrs[index:]...)
	fmt.Println("All key list: ", node.Keys)

	numOfLeftKeys := math.Ceil((float64(MAX_NUM_KEYS) + 1) / 2)
	fmt.Println("All key list: ", node.Keys)

	// Current node will be made as the left node
	newRightNode := NewBPNode(node.IsLeaf)
	// newParentNode := NewBPNode(false)

	// newRightNode.Next = node.Next
	// node.Next = newRightNode
	fmt.Println("All key list: ", node.Keys)
	fmt.Print("Nof left keys: ", numOfLeftKeys)
	fmt.Print("All record ptrs: ", allRecordPtrs)
	// allKeysListCopy := node.Keys
	// allRecordPtrsCopy := node.RecordPtrs
	fmt.Println("Right node keys: ", allKeysListCopy[int(numOfLeftKeys):])
	newRightNode.Keys = allKeysListCopy[int(numOfLeftKeys):]
	newRightNode.RecordPtrs = allRecordPtrsCopy[int(numOfLeftKeys):]
	fmt.Print("New Right Node info: ", newRightNode)

	fmt.Println("...updating current node info")
	// fmt.Println("All key list: ", allKeysList)
	// fmt.Println("All recordptrslist: ", allRecordPtrs)

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
