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

	fmt.Println("Testing starts...")
	fmt.Println("")

	for i := 0; i < 7; i++ {

		val := uint32(i + 5)
		fmt.Println("Inserting value ", val)
		fmt.Println("")
		tree.Insert(testNodes[i].RecordInfo, val)
		fmt.Println("\nInsert Finished for value ", val, "! ")
		fmt.Println(" \n##############################")

	}
	b, _ := json.Marshal(tree.Root)
	fmt.Println(b)
	fmt.Println("Root: ", tree.Root)
	fmt.Println("Root: ", tree.Root.Keys)
	fmt.Println("Leafs (2nd Layer)")
	for i := range tree.Root.KeyPtrs {
		// fmt.Println(i)
		fmt.Println("      ", tree.Root.KeyPtrs[i].Keys)
	}
	// fmt.Println(tree.Root.KeyPtrs)

	fmt.Println("Leafs (3rd Layer)")
	// fmt.Println("      ", tree.Root.KeyPtrs[1].Next.Keys)

	// fmt.Println("Leafs (3rd Layer)")
	// for i := range tree.Root.Keys {
	// 	fmt.Println("      ", tree.Root.KeyPtrs[1].Keys[i])
	// }

	// tree_json := string(b)
	// fmt.Println("new tree: ", tree_json)
	// for i := range string(b) {
	// 	fmt.Println(i)
	// }

	fmt.Println("")
	fmt.Println("Testing finished!!")
	fmt.Println("==============================================================================")

	return
}
