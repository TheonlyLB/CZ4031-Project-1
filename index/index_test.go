package index_test

import (
	"CZ4031_Project_1/index"
	"CZ4031_Project_1/storage"
	"encoding/json"
	"fmt"
	"testing"
)

const TEST_NUM = 10

func TestIndex(t *testing.T) {
	fmt.Println("==============================================================================")
	fmt.Println("Initializing mock tree....")
	fmt.Println("")

	testNodes := initRecordLLNodes()
	tree := initTree(testNodes)
	fmt.Println("testNodes", testNodes)
	fmt.Println("First test Node", testNodes[0].RecordInfo)
	fmt.Println("Initial Tree at", tree)
	fmt.Println("Initial Root node", tree.Root)
	fmt.Println(tree.Root.KeyPtrs[0].Keys)
	fmt.Println(" \n----------------------------")

	/////////////////////////////////////////////////////////////
	////// Running test code
	fmt.Println("Testing starts...")
	fmt.Println("")
	for i := 0; i < TEST_NUM; i++ {
		val := uint32(i + 5)
		fmt.Println("(Test no.", i, "of", TEST_NUM, ") Inserting value ", val)
		fmt.Println("")
		tree.Insert(testNodes[i].RecordInfo, val)
		fmt.Println("\nInsert Finished for value ", val, "! ")
		PrintTree(tree)
		fmt.Println(" \n##############################")
	}

	/////////////////////////////////////////////////////////////
	////// Final Tree
	b, _ := json.Marshal(tree.Root)

	fmt.Println(string(b))
	fmt.Println("Total number of test values: ", TEST_NUM)
	PrintTree(tree)

	fmt.Println("")
	fmt.Println("Testing finished!!")
	fmt.Println("==============================================================================")

	return
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func initRecordLLNodes() []*index.RecordLLNode {
	tempLLNodes := make([]*index.RecordLLNode, 0)
	for i := 0; i < 10; i++ {
		var r storage.RecordLocation
		r.BlockIndex = uint32(i)
		r.RecordIndex = uint8(i)
		node := &index.RecordLLNode{
			RecordInfo: &r,
			Next:       nil,
		}
		tempLLNodes = append(tempLLNodes, node)
	}
	return tempLLNodes
}

func initTree(nodes []*index.RecordLLNode) *index.BPTree {
	tree := index.NewTree()
	parent := index.NewBPNode(false)
	tree.Root = parent

	leftChild := index.NewBPNode(true)
	leftChild.Keys = []uint32{0, 1, 3}
	leftChild.RecordPtrs = nodes[:3]
	leftChild.ParentNode = tree.Root

	rightChild := index.NewBPNode(true)
	rightChild.Keys = []uint32{4, 5}
	rightChild.RecordPtrs = nodes[3:]
	rightChild.ParentNode = tree.Root

	leftChild.Next = rightChild
	parent.Keys = []uint32{rightChild.Keys[0]}
	parent.KeyPtrs = []*index.BPNode{leftChild, rightChild}
	fmt.Println(parent.KeyPtrs[0].Keys)

	return tree
}

func PrintTree(tree *index.BPTree) {
	fmt.Println("\nRoot: ", tree.Root)

	layerNo := 0
	nextLayer := true
	fmt.Println("Root,  //layer", layerNo)
	fmt.Println("        ", tree.Root.Keys)

	tempParentNodeList := make([]*index.BPNode, 0)
	tempParentNodeList = append(tempParentNodeList, tree.Root)
	tempNodeWithChildList := make([]*index.BPNode, 0)
	for nextLayer {
		// fmt.Println(tempParentNodeList)
		for i := range tempParentNodeList {
			// fmt.Println(i)
			// fmt.Println("Lenth of list", len(tempParentNodeList))

			// fmt.Println(tempParentNodeList[i].KeyPtrs)
			if len(tempParentNodeList[i].KeyPtrs) > 0 {
				for j := range tempParentNodeList[i].KeyPtrs {
					tempNodeWithChildList = append(tempNodeWithChildList, tempParentNodeList[i].KeyPtrs[j])
				}
			}
			// tempParentNodeList = tempNodeWithChildList
			// if len(tempParentNodeList) == 0 {
			// 	fmt.Println("break")
			// 	nextLayer = false
			// 	break
			// }
		}
		if len(tempParentNodeList) == 0 {
			// fmt.Println("break")
			nextLayer = false
			break
		}
		tempParentNodeList = tempNodeWithChildList

		layerNo += 1
		fmt.Println("Nodes, //layer", layerNo, "")
		for k := range tempParentNodeList {
			fmt.Println("        ", tempParentNodeList[k].Keys)
		}

		tempNodeWithChildList = nil

	}
}

// if len(tempParentNode.KeyPtrs) > 0 {
// 	layerNo += 1
// 	fmt.Println("Nodes (layer ", layerNo, "): ")
// 	for i := range tempParentNode.KeyPtrs {
// 		fmt.Println("      ", tempParentNode.KeyPtrs[i].Keys)
// 		tempNode := tempParentNode.KeyPtrs[i]
// 		if len(tempNode.KeyPtrs) > 0 {
// 			layerNo += 1
// 		} else {
// 			continue
// 		}
// 	}

// } else {
// 	nextLayer = false
// }

// layerNo += 1
// fmt.Println("Nodes (layer ", layerNo, "): ")

// for i := range tree.Root.KeyPtrs {
// 	fmt.Println("      ", tree.Root.KeyPtrs[i].Keys)
// 	tempNode := tree.Root.KeyPtrs[i]
// 	// fmt.Println("tempnode:", tempNode)
// 	if len(tempNode.KeyPtrs) > 0 {
// 		layer3 = true
// 		validnodes_l2 = append(validnodes_l2, tempNode)
// 	}
// }

// // fmt.Println(validnodes_l2)
// if layer3 {
// 	fmt.Println("Leafs (layer3)")
// 	for Idx := range validnodes_l2 {
// 		for i := range validnodes_l2[Idx].KeyPtrs {
// 			fmt.Println("      ", validnodes_l2[Idx].KeyPtrs[i].Keys)
// 		}
// 		fmt.Println("      ...........")
// 		// fmt.Println(node)

// 	}
// }
