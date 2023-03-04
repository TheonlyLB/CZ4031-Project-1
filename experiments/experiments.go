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
	fmt.Println("Loading data from tsv...")
	disk := storage.CreateDisk(100, blockSize)
	disk.LoadData("./data.tsv")

	fmt.Println("=== Experiment 1 ===")
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
	treeOrder := (disk.BlockSize - 5) / 12
	tree := index.NewTree(int(treeOrder))

	fmt.Println("Constructing B+ Tree...")
	totalBlocks := len(disk.BlockArray)
	// Inserting records
	for blk_no, block := range disk.BlockArray {
		if blk_no == 30000 {
			break
		}
		recordArray, _ := storage.BlockToRecord(block)

		for record_no, record := range recordArray {
			if blk_no%1000 == 0 {
				fmt.Printf("Block no: %v/%v, record no: %v \n", blk_no, totalBlocks, record_no)
			}
			tree.Insert(record.NumVotes, &disk.RecordLocationArray[record_no])
		}
	}
	fmt.Println("B+ Tree Constructed")

	// fmt.Println("=== Experiment 2 ===")
	fmt.Printf("Order/Branching Factor n: %d\n", index.MAX_NUM_KEYS)
	fmt.Printf("Tree height: %v\n", tree.GetHeight())
	fmt.Printf("Number of nodes: %v\n", tree.GetTotalNodes())
	fmt.Printf("Root Key: %v\n", tree.Root.Key)

	/*
		Experiment 3: retrieve those movies with the numVotes equal to 500 and report the following statistics
		• the number of index nodes the process accesses;
		• the number of data blocks the process accesses;
		• the average of “averageRating’s” of the records that are returned;
		• the running time of the retrieval process (please specify the method you use for measuring the running time of a piece of code)
		• the number of data blocks that would be accessed by a brute-force linear scan method (i.e., it scans the data blocks one by one) and its running time (for comparison)
	*/

	// TODO insert tree search here (Done)
	exp3Query := index.SearchConfig{
		Type:   index.ValueQuery,
		Values: []uint32{500},
	}
	recordLocationArray := tree.Search(exp3Query, true)
	fmt.Printf("No. of RecordLocations: %v\n", len(recordLocationArray))
	// testing
	// fmt.Println(disk.RetrieveRecord(*recordLocationArray[0]).AverageRating)
	// recloc := disk.RetrieveAll()
	// for i := 0; i < len(recloc); i++ {
	// 	rec := disk.RetrieveRecord(recloc[i].RdLoc)
	// 	fmt.Println("in test")
	// 	fmt.Println(recloc[i].NumVotes == rec.NumVotes)
	// 	fmt.Println(rec.AverageRating)
	// 	fmt.Println(rec.Deleted)
	// 	fmt.Println(rec.NumVotes)
	// 	fmt.Println(rec.TConst)
	// }

	// bruteforce search
	// var search = [2]uint32{500, 500}
	// start := time.Now()
	// var _, bruteBlocksAccessed = disk.BruteForceSearch(search)
	// t := time.Now()
	// elapsedBruteForce := t.Sub(start)
	// fmt.Printf("\n=== Experiment 3 ===\n")
	// fmt.Printf("Number for index nodes the process access: %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Number for data blocks the process access: %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Average of 'averageRatings' of records returned: %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Running time of retrieval initialised (difference in monotonic clock before and after the function call): %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Number of data blocks accessed by brute-force linear scan: %d\n", bruteBlocksAccessed)
	// fmt.Printf("Running time of brute-force linear scan: %v\n", elapsedBruteForce)

	/*
		Experiment 4: retrieve those movies with the attribute “numVotes” from 30,000 to 40,000, both inclusively and report the following statistics:
		• the number of index nodes the process accesses;
		• the number of data blocks the process accesses;
		• the average of “averageRating’s” of the records that are returned;
		• the running time of the retrieval process;
		• the number of data blocks that would be accessed by a brute-force linear scan method (i.e., it scans the data blocks one by one) and its running time (for comparison)
	*/
	// TODO insert tree range search here (Done)
	type SearchConfig struct {
		Type   string // RangeQuery or Value query
		Values []uint32
	}
	var valueArray []uint32
	for i := 30000; i <= 40000; i++ {
		valueArray = append(valueArray, uint32(i))
	}
	exp4Query := index.SearchConfig{
		Type:   index.RangeQuery,
		Values: valueArray,
	}
	recordLocationArray = tree.Search(exp4Query, true)

	// SearchRange(fromKey uint32, toKey uint32, verbose bool) []*storage.RecordLocation
	// // bruteforce search
	// var search2 = [2]uint32{30000, 40000}
	// start2 := time.Now()
	// var _, bruteBlocksAccessed2 = disk.BruteForceSearch(search2)
	// t2 := time.Now()
	// elapsedBruteForce2 := t2.Sub(start2)

	// fmt.Println("\n=== Experiment 4 ===\n")
	// fmt.Printf("Number for index nodes the process access: %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Number for data blocks the process access: %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Average of 'averageRatings' of records returned: %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Running time of retrieval initialised (difference in monotonic clock before and after the function call): %d\n", index.MAX_NUM_KEYS)
	// fmt.Printf("Number of data blocks accessed by brute-force linear scan: %d\n", bruteBlocksAccessed2)
	// fmt.Printf("Running time of brute-force linear scan: %v\n", elapsedBruteForce2)

	/*
		Experiment 5: delete those movies with the attribute “numVotes” equal to 1,000, update the B+ tree accordingly, and report the following statistics:
		• the number nodes of the updated B+ tree;
		• the number of levels of the updated B+ tree;
		• the content of the root node of the updated B+ tree(only the keys);
		• the running time of the process;
		• the number of data blocks that would be accessed by a brute-force linear scan method (i.e., it scans the data blocks one by one) and its running time (for comparison)
	*/
	// TODO insert tree search and delete here (Done)
	exp5Query := index.SearchConfig{
		Type:   index.ValueQuery,
		Values: []uint32{1000},
	}
	recordLocationArray = tree.Search(exp5Query, true)
	for recordLocation := 0; recordLocation < len(recordLocationArray); recordLocation++ {
		disk.DeleteRecord(*recordLocationArray[recordLocation])

		// // bruteforce search
		// var search3 = [2]uint32{1000, 1000}
		// start3 := time.Now()
		// var _, bruteBlocksAccessed3 = disk.BruteForceSearch(search3)
		// t3 := time.Now()
		// elapsedBruteForce3 := t3.Sub(start3)

		// fmt.Println("\n=== Experiment 5 ===\n")
		// fmt.Printf("Number for index nodes the process access: %d\n", index.MAX_NUM_KEYS)
		// fmt.Printf("Number for data blocks the process access: %d\n", index.MAX_NUM_KEYS)
		// fmt.Printf("Average of 'averageRatings' of records returned: %d\n", index.MAX_NUM_KEYS)
		// fmt.Printf("Running time of retrieval initialised (difference in monotonic clock before and after the function call): %d\n", index.MAX_NUM_KEYS)
		// fmt.Printf("Number of data blocks accessed by brute-force linear scan: %d\n", bruteBlocksAccessed3)
		// fmt.Printf("Running time of brute-force linear scan: %v\n", elapsedBruteForce3)
	}
}
