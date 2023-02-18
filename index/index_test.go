package index_test

import (
	"CZ4031_Project_1/index"
	"CZ4031_Project_1/storage"
	"encoding/json"
	"fmt"
	"testing"
)

const TEST_NUM = 5

func initRecordLLNodes() []*index.RecordLLNode {
	tempLLNodes := make([]*index.RecordLLNode, 0)
	for i := 0; i < 5; i++ {
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
	leftChild.Keys = []uint32{0, 1, 2}
	leftChild.RecordPtrs = nodes[:3]

	rightChild := index.NewBPNode(true)
	rightChild.Keys = []uint32{3, 4}
	rightChild.RecordPtrs = nodes[3:]

	leftChild.Next = rightChild
	parent.Keys = []uint32{rightChild.Keys[0]}
	parent.KeyPtrs = []*index.BPNode{leftChild, rightChild}

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
	fmt.Println(" \n----------------------------")

	fmt.Println("Testing starts...")
	fmt.Println("")
	tree.Insert(testNodes[0].RecordInfo, 5)
	tree.Insert(testNodes[1].RecordInfo, 6)
	b, _ := json.Marshal(tree)
	// fmt.Println(string(b))
	tree_json := string(b)
	fmt.Println(tree_json)
	// for i := range string(b) {
	// 	fmt.Println(i)
	// }

	fmt.Println("")
	fmt.Println("Testing finished!!")

	fmt.Println("==============================================================================")

	return
}
