package experiments

import (
	"CZ4031_Project_1/index"
	"CZ4031_Project_1/storage"
	"fmt"
)

func Experiments(blockSize uint8) {
	/*
		 Experiment 1: store the data on disk and report:
		• the number of records;
		• the size of a record;
		• the number of records stored in a block;
		• the number of blocks for storing the data;
	*/
	fmt.Println("Loading data from tsv...\n")
	disk := storage.CreateDisk(100, blockSize)
	disk.LoadData("./data.tsv")

	fmt.Println("=== Experiment 1 ===\n")
	fmt.Printf("Number of records on disk: %d\n", disk.NumRecords)
	fmt.Printf("Size of record: %d\n", storage.RecordLength)
	fmt.Printf("Number of records in block: %d\n", disk.BlockArray[0].NumRecord) //disk.BlockSize/storage.RecordLength
	fmt.Printf("Number of blocks used: %d\n", len(disk.BlockArray))

	/*
		Experiment 2: build a B+ tree on the attribute "numVotes" by inserting the records sequentially and report:
		• the parameter n of the B+ tree;
		• the number of nodes of the B+ tree;
		• the number of levels of the B+ tree;
		• the content of the root node (only the keys);
	*/
	tree := index.NewTree()

	fmt.Println("Constructing B+ Tree...\n")
	// Inserting records
	for _, block := range disk.BlockArray {
		recordArray, _ := storage.BlockToRecord(block)

		for i, record := range recordArray {
			tree.Insert(&disk.RecordLocationArray[i], record.NumVotes) //
		}
	}
	fmt.Println("B+ Tree Constructed\n")

	fmt.Println("=== Experiment 2 ===\n")
	fmt.Printf("Order/Branching Factor n: %d\n", index.MAX_NUM_KEYS)
	fmt.Printf("Number of Nodes: %v\n", tree.NumNodes())
	fmt.Printf("Number of Levels: %v\n", tree.NumLevels())
	fmt.Println("Content of root node:\n")
	fmt.Printf("%v\n", tree.Root.Keys)
}
