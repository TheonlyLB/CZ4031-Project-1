package index_test

import (
	"CZ4031_Project_1/index"
	"CZ4031_Project_1/storage"
	"encoding/json"
	"fmt"
	"testing"
)

const TEST_NUM = 10

func initRecordLLNodes() []*index.RecordLLNode {
	tempLLNodes := make([]*index.RecordLLNode, 0)
	for i := 0; i < TEST_NUM; i++ {
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

func TestDelete(t *testing.T) {
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

	fmt.Println("Testing starts...")
	fmt.Println("")
	tree.Delete(5)
	tree.Delete(3)
	tree.Delete(1)
	tree.Delete(0)
	// llNodes := initRecordLLNodes()

	// parent := &index.BPNode{
	// 	IsLeaf: false,
	// }
	// leaf1 := &index.BPNode{
	// 	IsLeaf:     true,
	// 	Keys:       []uint32{2, 3},
	// 	RecordPtrs: []*index.RecordLLNode{testNodes[2], testNodes[3]},
	// 	ParentNode: parent,
	// }
	// leaf2 := &index.BPNode{
	// 	IsLeaf:     true,
	// 	Keys:       []uint32{4},
	// 	RecordPtrs: []*index.RecordLLNode{testNodes[4]},
	// 	ParentNode: parent,
	// }
	// parent.Keys = []uint32{4}
	// parent.KeyPtrs = []*index.BPNode{leaf1, leaf2}

	// leaf2.BorrowKeyFromNode(leaf1, true)
	fmt.Println(" \nTest finished!")

	fmt.Println(" \n----------------------------")

	// Print tree
	// leaf1.ParentNode = nil
	// leaf2.ParentNode = nil
	// p, _ := json.Marshal(parent)
	// t.Logf("parent: %v", string(p))

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

func PrintTree(tree *index.BPTree) {
	fmt.Println("\nRoot: ", tree.Root)

	layerNo := 0
	nextLayer := true
	fmt.Println("\nRoot,  //layer", layerNo)
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
			fmt.Println("break")

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

// func TestIndex(t *testing.T) {
// 	fmt.Println("==============================================================================")
// 	fmt.Println("Initializing mock tree....")
// 	fmt.Println("")

// 	testNodes := initRecordLLNodes()
// 	tree := initTree(testNodes)
// 	fmt.Println("testNodes", testNodes)
// 	fmt.Println("First test Node", testNodes[0].RecordInfo)
// 	fmt.Println("Initial Tree at", tree)
// 	fmt.Println("Initial Root node", tree.Root)
// 	fmt.Println(tree.Root.KeyPtrs[0].Keys)

// 	fmt.Println(" \n----------------------------")

// 	fmt.Println("Testing starts...")
// 	fmt.Println("")

// 	for i := 0; i < 7; i++ {

// 		val := uint32(i + 5)
// 		fmt.Println("Inserting value ", val)
// 		fmt.Println("")
// 		tree.Insert(testNodes[i].RecordInfo, val)
// 		fmt.Println("\nInsert Finished for value ", val, "! ")
// 		fmt.Println(" \n##############################")

// 	}
// 	b, _ := json.Marshal(tree.Root)
// 	fmt.Println(b)
// 	fmt.Println("Root: ", tree.Root)
// 	fmt.Println("Root: ", tree.Root.Keys)
// 	fmt.Println("Leafs (2nd Layer)")
// 	for i := range tree.Root.KeyPtrs {
// 		// fmt.Println(i)
// 		fmt.Println("      ", tree.Root.KeyPtrs[i].Keys)
// 	}
// 	// fmt.Println(tree.Root.KeyPtrs)

// 	fmt.Println("Leafs (3rd Layer)")
// 	// fmt.Println("      ", tree.Root.KeyPtrs[1].Next.Keys)

// 	// fmt.Println("Leafs (3rd Layer)")
// 	// for i := range tree.Root.Keys {
// 	// 	fmt.Println("      ", tree.Root.KeyPtrs[1].Keys[i])
// 	// }

// 	// tree_json := string(b)
// 	// fmt.Println("new tree: ", tree_json)
// 	// for i := range string(b) {
// 	// 	fmt.Println(i)
// 	// }

// 	fmt.Println("")
// 	fmt.Println("Testing finished!!")
// 	fmt.Println("==============================================================================")

// 	return
// }

// func TestBorrow(t *testing.T) {
// 	fmt.Println("==============================================================================")
// 	fmt.Println("Initializing mock tree....")
// 	fmt.Println("")

// 	testNodes := initRecordLLNodes()
// 	tree := initTree(testNodes)
// 	fmt.Println("testNodes", testNodes)
// 	fmt.Println("First test Node", testNodes[0].RecordInfo)
// 	fmt.Println("Initial Tree at", tree)
// 	fmt.Println("Initial Root node", tree.Root)
// 	fmt.Println(tree.Root.KeyPtrs[0].Keys)

// 	fmt.Println(" \n----------------------------")

// 	fmt.Println("Testing starts...")
// 	fmt.Println("")
// 	llNodes := initRecordLLNodes()
// 	// tree := initTree(llNodes)

// 	// b, _ := json.Marshal(llNodes)
// 	// t.Logf("nodes: %v", string(b))
// 	// var n1KeyPtr, n2KeyPtr []*index.BPNode
// 	// n1KeyPtr = append(n1KeyPtr, &index.BPNode{
// 	// 	IsLeaf:     true,
// 	// 	Keys:       []uint32{0, 1},
// 	// 	RecordPtrs: []*index.RecordLLNode{llNodes[0], llNodes[1]},
// 	// })
// 	// n1KeyPtr = append(n1KeyPtr, &index.BPNode{
// 	// 	IsLeaf:     true,
// 	// 	Keys:       []uint32{2, 3},
// 	// 	RecordPtrs: []*index.RecordLLNode{llNodes[2], llNodes[3]},
// 	// })
// 	// n1KeyPtr = append(n1KeyPtr, &index.BPNode{
// 	// 	IsLeaf:     true,
// 	// 	Keys:       []uint32{4},
// 	// 	RecordPtrs: []*index.RecordLLNode{llNodes[4]},
// 	// })

// 	// node1 := &index.BPNode{
// 	// 	IsLeaf: false,
// 	// 	Keys:   []uint32{2, 4},
// 	// 	// RecordPtrs: []*index.RecordLLNode{llNodes[3]},
// 	// 	KeyPtrs: n1KeyPtr,
// 	// }

// 	// n2KeyPtr = append(n2KeyPtr, &index.BPNode{
// 	// 	IsLeaf:     true,
// 	// 	Keys:       []uint32{5},
// 	// 	RecordPtrs: []*index.RecordLLNode{llNodes[5]},
// 	// })

// 	// n2KeyPtr = append(n2KeyPtr, &index.BPNode{
// 	// 	IsLeaf:     true,
// 	// 	Keys:       []uint32{6},
// 	// 	RecordPtrs: []*index.RecordLLNode{llNodes[6]},
// 	// })
// 	// node2 := &index.BPNode{
// 	// 	IsLeaf: false,
// 	// 	Keys:   []uint32{6},
// 	// 	// RecordPtrs: llNodes[:3],
// 	// 	KeyPtrs: n2KeyPtr,
// 	// }

// 	// node2.BorrowKeyFromNode(node1)
// 	// n1, _ := json.Marshal(node1)
// 	// n2, _ := json.Marshal(node2)
// 	// t.Logf("Node1: %v", string(n1))
// 	// t.Logf("Node2: %v", string(n2))
// 	parent := &index.BPNode{
// 		IsLeaf: false,
// 	}
// 	leaf1 := &index.BPNode{
// 		IsLeaf:     true,
// 		Keys:       []uint32{2, 3},
// 		RecordPtrs: []*index.RecordLLNode{llNodes[2], llNodes[3]},
// 		ParentNode: parent,
// 	}
// 	leaf2 := &index.BPNode{
// 		IsLeaf:     true,
// 		Keys:       []uint32{4},
// 		RecordPtrs: []*index.RecordLLNode{llNodes[4]},
// 		ParentNode: parent,
// 	}
// 	parent.Keys = []uint32{4}
// 	parent.KeyPtrs = []*index.BPNode{leaf1, leaf2}

//		leaf2.BorrowKeyFromNode(leaf1, true)
//		leaf1.ParentNode = nil
//		leaf2.ParentNode = nil
//		p, _ := json.Marshal(parent)
//		t.Logf("parent: %v", string(p))
//	}
