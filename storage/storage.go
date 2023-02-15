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

type disk struct {
	capacity            uint8                    //capacity in MB
	blockSize           uint8                    //block size in bytes
	blockIndex          uint32                   //index of current block for insertion of next record
	numBlocks           uint32                   //num of blocks created
	remainingBlocks     uint32                   //num of blocks that can be created on disk
	numRecords          uint32                   //num of records inserted
	blockArray          []block                  //secondlevelindex
	lookUpTable         map[*byte]recordLocation //key: record address, value: block index
	deletedArray        []recordLocation         //stores memory address of deleted record's recordLocation
	recordLocationArray []recordLocation         //array of recordLocations available
}

type block struct {
	numRecord        uint8  //num of records in block
	recordValueArray []byte //byte array storing record values
}

type record struct {
	tConst        []byte
	numVotes      uint32 //4 bytes
	deleted       bool   // true if deleted, 1 byte
	averageRating uint8  //1 byte

}

type recordLocation struct {
	blockIndex  uint32 //index of corresponding block
	recordIndex uint8  //index of record within corresponding block
}

// Creates storage structure on disk with specified capacity and block size
func createDisk(capacity uint8, blockSize uint8) disk {
	diskObject := disk{
		capacity:    capacity,
		blockSize:   blockSize,
		blockIndex:  0,
		lookUpTable: map[*byte]recordLocation{},
	}

	_, err := createBlock(&diskObject)
	if err != nil {
		// panic() aborts a function if it returns and error that we don't intend to handle
		panic(err)
	} else {
		fmt.Printf("Storage created on disk. Capacity: %dMB, Block Size: %db\n", diskObject.capacity, diskObject.blockSize)
		return diskObject
	}
}

// creates block in disk
func createBlock(diskObject *disk) (uint32, error) {
	if int32(diskObject.blockIndex) >= (int32(diskObject.capacity)*1000000)/int32(diskObject.blockSize) {
		// return 0 instead of -1 since diskObject.blockIndex is uint type
		return 0, errors.New("insufficient disk space for new block")
	} else {
		block := block{
			recordValueArray: make([]byte, diskObject.blockSize),
		}
		diskObject.blockArray = append(diskObject.blockArray, block)
		diskObject.blockIndex += 1
		return diskObject.blockIndex - 1, nil
	}
}

// Read tsv file, load data tuples into records on disk
func (diskObject *disk) loadData(filepath string) {
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
func (diskObject *disk) createRecord(tConst string, averageRating float64, numVotes int64) (*byte, error) {
	recordObject := record{
		tConst:   []byte(tConst),   // convert string to 10 unicode characters, 10 bytes
		numVotes: uint32(numVotes), // numVotes values within range of uint32, 4 bytes
		// initialise 'deleted' header, 2 bytes
		averageRating: uint8(averageRating * 10), // averageRating * 10 so it can be stored as uint8, 1 byte

	}

	// calculate blockCapacity using blockSize and recordLength
	blockCapacity := diskObject.blockSize / (recordLength + 2) // 2 bytes for the block header
	// calculate index of current block
	diskObject.blockIndex = diskObject.numBlocks - 1
	// retrieve current block
	currentBlock := diskObject.blockArray[diskObject.blockIndex]

	// If current block is full
	if currentBlock.numRecord >= blockCapacity {
		// create new block
		index, err := createBlock(diskObject)
		if err != nil {
			panic("Cannot create new block when current block is full")
		}
		// retrieve new block using its index, store as current block
		currentBlock = diskObject.blockArray[index]
	}
	// convert record to bytes for storage(pack fields)
	byteRecord := recordToBytes(recordObject)

	// copy record into block
	copy(currentBlock.recordValueArray[currentBlock.numRecord*recordLength:], byteRecord)
	// retrieve address of record on disk
	recordAddress := &currentBlock.recordValueArray[currentBlock.numRecord*recordLength]
	// update lookup table from address to recordLocation object
	recordLocationObject := recordLocation{blockIndex: diskObject.blockIndex, recordIndex: currentBlock.numRecord}
	diskObject.lookUpTable[recordAddress] = recordLocationObject
	// store recordLocation in recordLocationArray
	diskObject.recordLocationArray = append(diskObject.recordLocationArray, recordLocationObject)
	currentBlock.numRecord += 1

	return recordAddress, nil
}

// Converts record object into bytes (packs fields)
func recordToBytes(recordObject record) []byte {
	var byteRecord []byte
	// Pack tConst field
	tConstBinary := make([]byte, tConstLength)
	copy(tConstBinary, recordObject.tConst)
	byteRecord = append(byteRecord, tConstBinary...)

	// Pack averageRating
	byteRecord = append(byteRecord, recordObject.averageRating)

	// Pack numVotes
	numVotesBinary := make([]byte, numVotesLength)
	binary.BigEndian.PutUint32(numVotesBinary, recordObject.numVotes)
	byteRecord = append(byteRecord, numVotesBinary...)

	return byteRecord
}

// Converts bytes into record object (unpacks fields)
func bytesToRecord(byteRecord []byte) record {
	// Unpack tConst
	tConst := byteRecord[:tConstLength]

	// Unpack averageRating
	averageRatingArray := (byteRecord[tConstLength : tConstLength+averageRatingLength])
	// To convert byte array to uint8, first convert to string, then to integer
	averageRating, _ := strconv.Atoi(string(averageRatingArray))

	// Unpack numVotes
	numVotes := binary.BigEndian.Uint32(byteRecord[tConstLength+averageRatingLength:])

	recordObject := record{
		tConst:        tConst,
		averageRating: uint8(averageRating),
		numVotes:      numVotes,
	}

	return recordObject
}

// Takes record address, returns record object
func addressToRecord(diskObject *disk, recordAddress *byte) record {
	location, exist := diskObject.lookUpTable[recordAddress]
	if !exist {
		errMsg := fmt.Sprintf("No record at address %v", recordAddress)
		panic(errMsg)
	}

	blockOffset := location.recordIndex * recordLength
	// Retrieve corresponding byte record
	byteRecord := diskObject.blockArray[location.blockIndex].recordValueArray[blockOffset : blockOffset+recordLength]

	return bytesToRecord(byteRecord)
}

// Takes block object, returns array of stored records and array of pointers to stored records
func blockToRecord(blockObject block) ([]record, []*byte) {
	var recordArray []record
	var pointerArray []*byte
	var recordObject record

	for i := 0; i < int(blockObject.numRecord); i++ {
		recordObject = bytesToRecord(blockObject.recordValueArray[i*recordLength : i*recordLength+recordLength])
		recordArray = append(recordArray, recordObject)
		pointerArray = append(pointerArray, &blockObject.recordValueArray[i*recordLength])
	}

	return recordArray, pointerArray
}

// Takes in a recordLocation instance and returns the record corresponding to that recordLocation
func (diskObject *disk) retrieveRecord(recordLocationObject recordLocation) record {
	var interestedBlock block
	var recordObject record
	var recordArray []record
	interestedBlock = diskObject.blockArray[recordLocationObject.blockIndex]
	recordArray, _ = blockToRecord(interestedBlock)
	recordObject = recordArray[recordLocationObject.recordIndex]
	return recordObject
}

// REVIEW after AddrToRecord,recordToBytes are implemented
// Deletes record given address to record
// change the input from address to recordlocation
func (diskObject *disk) deleteRecords(recordLocationObject recordLocation) {

	var interestedBlock block
	var recordObject record
	var recordArray []record
	var byteRecord []byte
	// retrieve block using block index
	interestedBlock = diskObject.blockArray[recordLocationObject.blockIndex]

	// retrieve recordObject
	// recordObject, err := addressToRecord(address)
	// if err != nil {
	// 	panic("Unable to delete record as recordObject could not be formed from address")
	// }
	recordArray, _ = blockToRecord(interestedBlock)
	recordObject = recordArray[recordLocationObject.recordIndex]

	// set deleted flag to true
	recordObject.deleted = true

	// convert new recordObject
	byteRecord = recordToBytes(recordObject)

	// copy back into block
	copy(interestedBlock.recordValueArray[recordLocationObject.recordIndex*recordLength:], byteRecord)

	// append to deletedArray
	diskObject.deletedArray = append(diskObject.deletedArray, recordLocationObject)

	// remove from recordLocationArray
	for i := 0; i < int(len(diskObject.recordLocationArray)); i++ {
		if diskObject.recordLocationArray[i] == recordLocationObject {
			diskObject.recordLocationArray = append(diskObject.recordLocationArray[:i], diskObject.recordLocationArray[i+1:]...)
		}
	}

	return
}

func (diskObject *disk) retrieveAll() []recordLocation {
	return diskObject.recordLocationArray
}
