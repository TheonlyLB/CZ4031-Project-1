package index

import "CZ4031_Project_1/storage"

const (
	MAX_NUM_KEYS int = 15 // TBD
)

type BPTree struct {
	Root  *BPNode
	Order int
}

func NewTree(order int) *BPTree {
	return &BPTree{
		Root:  nil,
		Order: order,
	}
}

type BPNode struct {
	//Node size given 64bit system (ignoring header):
	// 4 bytes * (num of Key) + 8 bytes * (num of Ptr)
	// Header such as IsLeaf, Parent are ignored.
	IsLeaf   bool
	Key      []uint32        //uint32 - 4 bytes
	Children []*BPNode       //Children[i] points to node with key < Key[i], Ptr[i+1] for key >= Key[i]
	DataPtr  []*RecordLLNode //DataPtr[i] points to the data node with key = Key[i]
	Next     *BPNode         //For leaf node only, the next leaf node if any
	Parent   *BPNode         //The parent node
}

func NewBPNode(isLeaf bool) *BPNode {
	return &BPNode{
		IsLeaf:   isLeaf,
		Parent:   nil,
		Next:     nil,
		Key:      make([]uint32, 0),
		Children: make([]*BPNode, 0),
		DataPtr:  make([]*RecordLLNode, 0),
	}
}

func (node *BPNode) isFull() bool {
	return len(node.Key) >= MAX_NUM_KEYS
}

type RecordLLNode struct {
	RecordInfo *storage.RecordLocation
	Next       *RecordLLNode
}

func (recordListNode *RecordLLNode) InsertRecordToLinkedList(recordLoc *storage.RecordLocation) {
	// Base case: not last list node
	if recordListNode.Next != nil {
		recordListNode.Next.InsertRecordToLinkedList(recordLoc)
		return
	}

	// Last node
	recordListNode.Next = &RecordLLNode{
		RecordInfo: recordLoc,
		Next:       nil,
	}
	return
}
