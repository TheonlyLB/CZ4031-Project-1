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
	recordLength        = tConstLength + averageRatingLength + numVotesLength
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

// Creates storage structure on disk with specified capacity and block size
func createDisk(capacity uint8, blockSize uint8) Disk {
	diskObject := Disk{
		Capacity:    capacity,
		BlockSize:   blockSize,
		BlockIndex:  0,
		LookUpTable: map[*byte]RecordLocation{},
	}

	_, err := createBlock(&diskObject)
	if err != nil {
		// panic() aborts a function if it returns and error that we don't intend to handle
		panic(err)
	} else {
		fmt.Printf("Storage created on disk. Capacity: %dMB, Block Size: %db\n", diskObject.Capacity, diskObject.BlockSize)
		return diskObject
	}
}

// creates block in disk
func createBlock(diskObject *Disk) (uint32, error) {
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
func (diskObject *Disk) loadData(filepath string) {
	// open file
	f, err := os.ReadFile(filepath)
	if err != nil {
		panic("Error opening file")
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
		_, err = diskObject.createRecord(tConst, averageRating, numVotes)
		if err != nil {
			panic("Insufficient disk storage.")
		}
	}
	fmt.Printf("%v\n records loaded into disk", len(tuples[1:]))
}

// Pack fields of data tuple into record on disk, returns the starting address of the record in the block
func (diskObject *Disk) createRecord(tConst string, averageRating float64, numVotes int64) (*byte, error) {
	recordObject := Record{
		TConst:   []byte(tConst),   // convert string to 10 unicode characters, 10 bytes
		NumVotes: uint32(numVotes), // numVotes values within range of uint32, 4 bytes
		// initialise 'deleted' header, 2 bytes
		AverageRating: uint8(averageRating * 10), // averageRating * 10 so it can be stored as uint8, 1 byte

	}

	// calculate blockCapacity using blockSize and recordLength
	blockCapacity := diskObject.BlockSize / (recordLength + 2) // 2 bytes for the block header
	// calculate index of current block
	diskObject.BlockIndex = diskObject.NumBlocks - 1
	// retrieve current block
	currentBlock := diskObject.BlockArray[diskObject.BlockIndex]

	// If current block is full
	if currentBlock.NumRecord >= blockCapacity {
		// create new block
		index, err := createBlock(diskObject)
		if err != nil {
			panic("Cannot create new block when current block is full")
		}
		// retrieve new block using its index, store as current block
		currentBlock = diskObject.BlockArray[index]
	}
	// convert record to bytes for storage(pack fields)
	byteRecord := recordToBytes(recordObject)

	// copy record into block
	copy(currentBlock.RecordValueArray[currentBlock.NumRecord*recordLength:], byteRecord)
	// retrieve address of record on disk
	recordAddress := &currentBlock.RecordValueArray[currentBlock.NumRecord*recordLength]
	// update lookup table from address to recordLocation object
	recordLocationObject := RecordLocation{BlockIndex: diskObject.BlockIndex, RecordIndex: currentBlock.NumRecord}
	diskObject.LookUpTable[recordAddress] = recordLocationObject
	// store recordLocation in recordLocationArray
	diskObject.RecordLocationArray = append(diskObject.RecordLocationArray, recordLocationObject)
	currentBlock.NumRecord += 1

	return recordAddress, nil
}

// Converts record object into bytes (packs fields)
func recordToBytes(recordObject Record) []byte {
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
func bytesToRecord(byteRecord []byte) Record {
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
func addressToRecord(diskObject *Disk, recordAddress *byte) Record {
	location, exist := diskObject.LookUpTable[recordAddress]
	if !exist {
		errMsg := fmt.Sprintf("No record at address %v", recordAddress)
		panic(errMsg)
	}

	blockOffset := location.RecordIndex * recordLength
	// Retrieve corresponding byte record
	byteRecord := diskObject.BlockArray[location.BlockIndex].RecordValueArray[blockOffset : blockOffset+recordLength]

	return bytesToRecord(byteRecord)
}

// Takes block object, returns array of stored records and array of pointers to stored records
func blockToRecord(blockObject Block) ([]Record, []*byte) {
	var recordArray []Record
	var pointerArray []*byte
	var recordObject Record

	for i := 0; i < int(blockObject.NumRecord); i++ {
		recordObject = bytesToRecord(blockObject.RecordValueArray[i*recordLength : i*recordLength+recordLength])
		recordArray = append(recordArray, recordObject)
		pointerArray = append(pointerArray, &blockObject.RecordValueArray[i*recordLength])
	}

	return recordArray, pointerArray
}

// Takes in a recordLocation instance and returns the record corresponding to that recordLocation
func (diskObject *Disk) retrieveRecord(recordLocationObject RecordLocation) Record {
	var interestedBlock Block
	var recordObject Record
	var recordArray []Record
	interestedBlock = diskObject.BlockArray[recordLocationObject.BlockIndex]
	recordArray, _ = blockToRecord(interestedBlock)
	recordObject = recordArray[recordLocationObject.RecordIndex]
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
	recordArray, _ = blockToRecord(interestedBlock)
	recordObject = recordArray[recordLocationObject.RecordIndex]

	// set deleted flag to true
	recordObject.Deleted = true

	// convert new recordObject
	byteRecord = recordToBytes(recordObject)

	// copy back into block
	copy(interestedBlock.RecordValueArray[recordLocationObject.RecordIndex*recordLength:], byteRecord)

	// append to deletedArray
	diskObject.DeletedArray = append(diskObject.DeletedArray, recordLocationObject)

	// remove from recordLocationArray
	for i := 0; i < int(len(diskObject.RecordLocationArray)); i++ {
		if diskObject.RecordLocationArray[i] == recordLocationObject {
			diskObject.RecordLocationArray = append(diskObject.RecordLocationArray[:i], diskObject.RecordLocationArray[i+1:]...)
		}
	}

	return
}

func (diskObject *Disk) retrieveAll() []RecordLocation {
	return diskObject.RecordLocationArray
}
