package index

import "CZ4031_Project_1/storage"

const (
	MAX_NUM_KEYS int = 3	// TBD
)

type BPTree struct {
	Root *BPNode
}

// Key[i] use KeyPtr[i] if isLeaf
type BPNode struct {
	IsLeaf     bool
	ParentNode *BPNode
	Next       *BPNode
	Keys       []uint32
	KeyPtrs    []*BPNode
	RecordPtrs []*RecordLLNode
}

func NewBPNode(isLeaf bool) *BPNode {
	return &BPNode{
		IsLeaf:     isLeaf,
		ParentNode: nil,
		Next:       nil,
		Keys:       make([]uint32, 0),
		KeyPtrs:    make([]*BPNode, 0),
		RecordPtrs: make([]*RecordLLNode, 0),
	}
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
