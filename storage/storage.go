package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/grailbio/base/tsv"
)

// 12/2/23
// delieverables for indexing grp
// func retrieveAll():
//   return [[recordLocationObject1,numVotes1],[recordLocationObject2,numVotes2],........]
// func retrieveRecord(recordLocationObject):
// 	 return recordObject
// func delete(recordLocationObject):
// 	 return bool

const (
	tConstLength        = 10
	averageRatingLength = 1
	numVotesLength      = 4
	deletedLength       = 1
	RecordLength        = tConstLength + averageRatingLength + numVotesLength + deletedLength
)

type Disk struct {
	Capacity            uint8                    //capacity in MB
	BlockSize           uint8                    //block size in bytes
	BlockIndex          uint32                   //index of current block for insertion of next record
	NumBlocks           uint32                   //num of blocks created
	RemainingBlocks     uint32                   //num of blocks that can be created on disk
	NumRecords          uint32                   //num of records inserted
	BlockArray          []Block                  // secondlevelindex
	LookUpTable         map[*byte]RecordLocation // key: record address, value: block index
	DeletedArray        []RecordLocation         //stores memory address of deleted record's recordLocation
	RecordLocationArray []RecordLocation         //array of recordLocations available
}

type Block struct {
	NumRecord        uint8  //num of records in block
	RecordValueArray []byte //byte array storing record values
}

type Record struct {
	TConst        []byte
	NumVotes      uint32 //4 bytes
	Deleted       bool   // true if deleted, 1 byte
	AverageRating uint8  //1 byte

}

type RecordLocation struct {
	BlockIndex  uint32 //index of corresponding block
	RecordIndex uint8  //index of record within corresponding block
}

// return object for retrieveall() which includes recordlocation and numvotes
type RecordLocationNumVotes struct {
	RdLoc    RecordLocation
	NumVotes uint32
}

// Creates storage structure on disk with specified capacity and block size
func CreateDisk(capacity uint8, blockSize uint8) Disk {
	diskObject := Disk{
		Capacity:    capacity,
		BlockSize:   blockSize,
		BlockIndex:  0,
		LookUpTable: map[*byte]RecordLocation{},
	}

	_, err := CreateBlock(&diskObject)
	if err != nil {
		// panic() aborts a function if it returns and error that we don't intend to handle
		panic(err)
	} else {
		fmt.Printf("Storage created on disk. Capacity: %dMB, Block Size: %db\n", diskObject.Capacity, diskObject.BlockSize)
		return diskObject
	}
}

// creates block in disk
func CreateBlock(diskObject *Disk) (uint32, error) {
	if int32(diskObject.BlockIndex) >= (int32(diskObject.Capacity)*1000000)/int32(diskObject.BlockSize) {
		// return 0 instead of -1 since diskObject.blockIndex is uint type
		return 0, errors.New("insufficient disk space for new block")
	} else {
		block := Block{
			RecordValueArray: make([]byte, diskObject.BlockSize),
		}
		diskObject.BlockArray = append(diskObject.BlockArray, block)
		diskObject.BlockIndex += 1
		return diskObject.BlockIndex - 1, nil
	}
}

// Read tsv file, load data tuples into records on disk
func (diskObject *Disk) LoadData(filepath string) {
	// open file
	f, err := os.ReadFile(filepath)
	if err != nil {
		panic(err) //"Error opening file"
	}
	// read file
	fmt.Println("Reading file....")
	r := tsv.NewReader(bytes.NewReader(f))
	tuples, err := r.ReadAll()
	// iterate over data tuples
	for _, tuple := range tuples[1:] {
		// Parse fields of data tuple to access relevant values
		tConst := tuple[0]
		averageRating, err := strconv.ParseFloat(tuple[1], 32)
		if err != nil {
			panic("Failed to parse fields of data tuple")
		}
		numVotes, err := strconv.ParseInt(tuple[2], 10, 32)
		if err != nil {
			panic("Failed to parse fields of data tuple")
		}
		// create record from each tuple
		_, err = diskObject.CreateRecord(tConst, averageRating, numVotes)
		if err != nil {
			panic("Insufficient disk storage.")
		}
	}
	fmt.Printf("%v records loaded into disk\n", len(tuples[1:]))
}

// Pack fields of data tuple into record on disk, returns the starting address of the record in the block
func (diskObject *Disk) CreateRecord(tConst string, averageRating float64, numVotes int64) (*byte, error) {
	recordObject := Record{
		TConst:   []byte(tConst),   // convert string to 10 unicode characters, 10 bytes
		NumVotes: uint32(numVotes), // numVotes values within range of uint32, 4 bytes
		// initialise 'deleted' header, 2 bytes
		AverageRating: uint8(averageRating * 10), // averageRating * 10 so it can be stored as uint8, 1 byte

	}
	// calculate blockCapacity using blockSize and recordLength
	blockCapacity := diskObject.BlockSize / (RecordLength + 2) // 2 bytes for the block header
	// retrieve current block
	currentBlock := diskObject.BlockArray[diskObject.BlockIndex-1] // index of current block is the blockindex-1
	// If current block is full
	if currentBlock.NumRecord >= blockCapacity {
		// create new block
		index, err := CreateBlock(diskObject)
		if err != nil {
			panic("Cannot create new block when current block is full")
		}
		// retrieve new block using its index, store as current block
		currentBlock = diskObject.BlockArray[index]
	}
	// convert record to bytes for storage(pack fields)
	byteRecord := RecordToBytes(recordObject)

	// copy record into block
	copy(currentBlock.RecordValueArray[currentBlock.NumRecord*RecordLength:], byteRecord)
	// retrieve address of record on disk
	recordAddress := &currentBlock.RecordValueArray[currentBlock.NumRecord*RecordLength]
	// update lookup table from address to recordLocation object
	recordLocationObject := RecordLocation{BlockIndex: diskObject.BlockIndex - 1, RecordIndex: currentBlock.NumRecord}
	diskObject.LookUpTable[recordAddress] = recordLocationObject
	// store recordLocation in recordLocationArray
	diskObject.RecordLocationArray = append(diskObject.RecordLocationArray, recordLocationObject)
	// update numrecord to reflect new record added
	diskObject.BlockArray[diskObject.BlockIndex-1].NumRecord += 1
	diskObject.NumRecords += 1
	return recordAddress, nil
}

// Converts record object into bytes (packs fields)
func RecordToBytes(recordObject Record) []byte {
	var byteRecord []byte
	// Pack tConst field
	tConstBinary := make([]byte, tConstLength)
	copy(tConstBinary, recordObject.TConst)
	byteRecord = append(byteRecord, tConstBinary...)

	// Pack averageRating
	byteRecord = append(byteRecord, recordObject.AverageRating)

	// Pack numVotes
	numVotesBinary := make([]byte, numVotesLength)
	binary.BigEndian.PutUint32(numVotesBinary, recordObject.NumVotes)
	byteRecord = append(byteRecord, numVotesBinary...)

	return byteRecord
}

// Converts bytes into record object (unpacks fields)
func BytesToRecord(byteRecord []byte) Record {
	// Unpack tConst
	tConst := byteRecord[:tConstLength]
	// Unpack averageRating
	averageRatingArray := (byteRecord[tConstLength : tConstLength+averageRatingLength])
	// To convert byte array to uint8, first convert to string, then to integer
	averageRating, _ := strconv.Atoi(string(averageRatingArray))

	// Unpack numVotes
	numVotes := binary.BigEndian.Uint32(byteRecord[tConstLength+averageRatingLength:])

	recordObject := Record{
		TConst:        tConst,
		AverageRating: uint8(averageRating),
		NumVotes:      numVotes,
	}
	return recordObject
}

// Takes record address, returns record object
func AddressToRecord(diskObject *Disk, recordAddress *byte) Record {
	location, exist := diskObject.LookUpTable[recordAddress]
	if !exist {
		errMsg := fmt.Sprintf("No record at address %v", recordAddress)
		panic(errMsg)
	}

	blockOffset := location.RecordIndex * RecordLength
	// Retrieve corresponding byte record
	byteRecord := diskObject.BlockArray[location.BlockIndex].RecordValueArray[blockOffset : blockOffset+RecordLength]

	return BytesToRecord(byteRecord)
}

// Takes block object, returns array of stored records and array of pointers to stored records
func BlockToRecord(blockObject Block) ([]Record, []*byte) {
	var recordArray []Record
	var pointerArray []*byte
	var recordObject Record
	// numrecord is the number of records in block

	for i := 0; i < int(blockObject.NumRecord); i++ {
		recordObject = BytesToRecord(blockObject.RecordValueArray[i*RecordLength : i*RecordLength+RecordLength])
		recordArray = append(recordArray, recordObject)
		pointerArray = append(pointerArray, &blockObject.RecordValueArray[i*RecordLength])
	}
	return recordArray, pointerArray
}

// Takes in a recordLocation instance and returns the record corresponding to that recordLocation
func (diskObject *Disk) RetrieveRecord(recordLocationObject RecordLocation) Record {
	var interestedBlock Block
	var recordObject Record
	var recordArray []Record
	interestedBlock = diskObject.BlockArray[recordLocationObject.BlockIndex]
	recordArray, _ = BlockToRecord(interestedBlock)
	recordObject = recordArray[recordLocationObject.RecordIndex]
	// fmt.Println("Retrieved record")
	return recordObject
}

// REVIEW after AddrToRecord,recordToBytes are implemented
// Deletes record given address to record
// change the input from address to recordlocation
func (diskObject *Disk) DeleteRecord(recordLocationObject RecordLocation) {

	var recordObject Record
	var recordArray []Record
	var byteRecord []byte
	// retrieve block using block index
	interestedBlock := diskObject.BlockArray[recordLocationObject.BlockIndex]

	// retrieve recordObject
	// recordObject, err := addressToRecord(address)
	// if err != nil {
	// 	panic("Unable to delete record as recordObject could not be formed from address")
	// }
	recordArray, _ = BlockToRecord(interestedBlock)
	recordObject = recordArray[recordLocationObject.RecordIndex]

	// set deleted flag to true
	recordObject.Deleted = true

	// convert new recordObject
	byteRecord = RecordToBytes(recordObject)

	// copy back into block
	copy(interestedBlock.RecordValueArray[recordLocationObject.RecordIndex*RecordLength:], byteRecord)

	// append to deletedArray
	diskObject.DeletedArray = append(diskObject.DeletedArray, recordLocationObject)

	// remove from recordLocationArray
	for i := 0; i < int(len(diskObject.RecordLocationArray)); i++ {
		if diskObject.RecordLocationArray[i] == recordLocationObject {
			diskObject.RecordLocationArray = append(diskObject.RecordLocationArray[:i], diskObject.RecordLocationArray[i+1:]...)
		}
	}
	fmt.Println("Record Deleted")
	return
}

func (diskObject *Disk) RetrieveAll() []RecordLocationNumVotes {
	var recrdLocArray = diskObject.RecordLocationArray
	var res []RecordLocationNumVotes
	for index := 0; index < int(len(recrdLocArray)); index++ {
		var recrdObj = diskObject.RetrieveRecord(recrdLocArray[index])
		recrdLocNumVotes := RecordLocationNumVotes{RdLoc: recrdLocArray[index], NumVotes: recrdObj.NumVotes}
		res = append(res, recrdLocNumVotes)
	}
	return res
}

// Brute force solution for getting records given range of numVotes
// hardcoded numvotes size
func (diskObject *Disk) BruteForceSearch(rangeNumVotes [2]uint32) ([]Record, int) {
	var resRecords []Record
	var numBlocksAccessed int = 0
	for i := 0; i < int(diskObject.BlockIndex); i++ {
		numBlocksAccessed++
		var curBlock = diskObject.BlockArray[i]
		for j := uint8(0); j < curBlock.NumRecord; j++ {
			var curRecLoc = RecordLocation{BlockIndex: uint32(i), RecordIndex: j}
			// check if current record location is in deleted array
			if diskObject.isInDeletedArray(curRecLoc) {
				continue
			}
			// check if satisfies condition
			if diskObject.RetrieveRecord(curRecLoc).NumVotes >= rangeNumVotes[0] && diskObject.RetrieveRecord(curRecLoc).NumVotes <= rangeNumVotes[1] {
				// append to result
				resRecords = append(resRecords, diskObject.RetrieveRecord(curRecLoc))
			}
		}
	}
	return resRecords, numBlocksAccessed
}

func (diskObject *Disk) isInDeletedArray(recordLocation RecordLocation) bool {
	for k := 0; k < len(diskObject.DeletedArray); k++ {
		if diskObject.DeletedArray[k].BlockIndex == recordLocation.BlockIndex && diskObject.DeletedArray[k].RecordIndex == recordLocation.RecordIndex {
			return true
		}
	}
	return false
}
