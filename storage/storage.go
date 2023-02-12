package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
)

/*
TO DO (ZON):

Functions:
recordsToBytes
bytesToRecords

*/

const (
	tConstLength    = 10
	avgRatingLength = 1
)
// included deletedArray and recordSize ben
type disk struct {
	capacity         uint8  //capacity in MB
	blockSize        uint8  //block size in bytes
	blockIndex       uint32 //index of current block for insertion of next record
	numBlocks        uint32 //num of blocks created
	remainingBlocks  uint32 //num of blocks that can be created on disk
	numRecords       uint32 //num of records inserted
	secondLevelIndex []block
	lookUpTable      map[*byte]recordLocation // key: record address, value: block index
	deletedArray     []*recordLocation //stores memory address of deleted record's recordLocation
	recordSize       uint8 //record size in bytes
}
// revised block struct
// included recordIndex ben
type block struct {
	recordIndex      uint16  // index of current record for insertion 
	recordValueArray []byte //byte array storing record values
}
// included deleted bool ben
type record struct {
	// include a header for deleted bool
	deleted       bool // true if deleted
	tConst        [tConstLength]byte
	averageRating uint8  //1 byte
	numVotes      []byte //length not initialised
}

type recordLocation struct {
	blockIndex   uint32 //index of corresponding block
	recordIndex  uint8  //index of record within corresponding block
	recordLength uint8  // length of record in bytes
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
		diskObject.secondLevelIndex = append(diskObject.secondLevelIndex, block)
		diskObject.blockIndex += 1
		return diskObject.blockIndex - 1, nil
	}

}
// Doesnt seem to be used, especially when there is already a writerecord that handles that creation
// Pack fields of data tuple into record
func createRecord(tConst string, averageRating float64, numVotes int64) record {

	recordObject := record{}

	// pack tConst field
	// convert string to unicode
	recordObject.tConst = tConst
	// pack averageRating field
	recordObject.averageRating = uint8(averageRating * 10)
	// assign numvotes to instance of record
	recordObject.numVotes = 
	// //Assign variable length of numVotes field with Golang slice
	// var numVotesLength int
	// if numVotes <= 255 {
	// 	numVotesLength = 1
	// } else if 255 < numVotes && numVotes <= 65535 {
	// 	numVotesLength = 2
	// } else if 65535 < numVotes && numVotes <= 4294967295 {
	// 	numVotesLength = 4
	// }

	numVotesSlice := make([]byte, numVotesLength)
	// pack numVotes field
	recordObject.numVotes = numVotesSlice

	return recordObject
}

// TO BE TESTED
// WriteRecord Write record into the virtual disk, with packing into bytes
// Return the starting address of the record in the block, and error if any.
func (diskObject *disk) WriteRecord(recordObject *record) (*byte, error) {

	// Record validations
	if recordObject.numVotes == 0 {
		panic("numVotes can't be zero")
	}
	// REVIEW max value of tconst
	if len([]rune(recordObject.tConst)) > TconstSize {
		panic("tconst size is too long")
	}
	
	if recordObject.averageRating > 3.4e+38 {
		panic("averageRating is too big")
	}
	// calc blockCapacity using blockSize and recordSize
	blockCapacity := diskObject.blockSize / (diskObject.recordSize + 2) // REVIEW 2 bytes for the block header
	
	// retrieve current block
	currentBlock = diskObject.secondLevelIndex[diskObject.blockIndex]

	// if current block has recordIndex > blockCapcity
	if int(currentBlock.recordIndex)>=blockCapacity{
		// REVIEW need to create block and should return the blockObject while allocated the memory
		_, err := createBlock(&diskObject)
		if err!=nil{
			panic("Error in WriteRecord, unable to createBlock when block is full")
		}
		currentBlock = diskObject.secondLevelIndex[diskObject.blockIndex]
	}
	// currentblock is ready for us to insert record

	// convert record to bytes to be stored
	recordInBytes := RecordToBytes(record)

	// copy record into block
	copy(currentBlock.recordValueArray[currentBlock.recordIndex*diskObject.recordSize],recordInBytes)
	// retrieve recordAddress
	recordAddress := &currentBlock.recordValueArray[currentBlock.recordIndex*diskObject.recordSize]
	// update lookuptable from address to recordlocation object
	diskObject.lookUpTable[recordAddress] = recordLocation{blockIndex: diskObject.blockIndex, recordIndex: int(currentBlock.recordIndex)}

	currentBlock.numRecord += 1
	currentBlock.recordIndex+=1
	return recordAddress, nil
}

// LoadRecords Load records from tsv file into disk
// dir is the relative file path
func (diskObject *disk) LoadRecords(dir string) {
	fmt.Println("Loading records from file....")
	// open file
	f, err := os.ReadFile(dir)
	if err != nil {
		panic("Error opening data file")
	}

	r := tsv.NewReader(bytes.NewReader(f))

	records, err := r.ReadAll()

	for _, rec := range records[1:] {


		avgRating, err := strconv.ParseFloat(rec[1], 32)
		if err != nil {
			panic("avgRating can't fit into float32")
		}

		numVotes, err := strconv.ParseUint(rec[2], 10, 32)
		if err != nil {
			fmt.Printf("%v", rec[2])
			panic("numVotes can't fit into int32")
		}

		recordObject := record{
			Tconst:        rec[0],
			AverageRating: float32(avgRating),
			NumVotes:      uint32(numVotes),
		}

		_, err = diskObject.WriteRecord(&record)
		if err != nil {
			panic("Loading interrupted, not enough disk storage! Consider increasing capacity of the virtual disk")
		}
	}
	fmt.Printf("Records loaded into virtal disk, total: %v\n", len(records[1:]))
}

// Unpack record into data tuple
func unpackRecord(bytes []byte) record {
	// Unpack tconst
	tconst := string(bytes[:tConstLength])

	// Unpack averageRating
	avgRating := binary.BigEndian.Uint16(bytes[tConstLength : tConstLength+avgRatingLength])
	avgRatingF := float32(avgRating) / 10

	// Unpack numVotes
	numVotes := binary.BigEndian.Uint32(bytes[tConstLength+avgRatingLength:])

	recordObject := record{
		tConst:        tconst,
		averageRating: avgRatingF,
		numVotes:      numVotes,
	}

	return recordObject
}

// Finds record using its starting addr in a block
func AddrToRecord(disk *VirtualDisk, addr *byte) Record {
	loc, exist := disk.LuTable[addr]
	if !exist {
		errMsg := fmt.Sprintf("Record can't be located with addr: %v", addr)
		panic(errMsg)
	}

	blockOffset := loc.Index * RecordSize
	bin := disk.Blocks[loc.BlockIndex].Content[blockOffset : blockOffset+RecordSize]

	return BytesToRecord(bin)
}

// Finds record using its block
func BlockToRecords(block Block) ([]Record, []*byte) {
	var records []Record
	var pointers []*byte
	var record Record

	for i := 0; i < int(block.NumRecord); i++ {
		record = BytesToRecord(block.Content[i*RecordSize : i*RecordSize+RecordSize])
		records = append(records, record)
		pointers = append(pointers, &block.Content[i*RecordSize])
	}

	return records, pointers
}
// REVIEW after AddrToRecord,recordToBytes are implemented
// Deletes record given address to record
func (diskObject *disk) DeleteRecords(address *byte){
	// retrieve recordLocationObj
	recordLocationObject = diskObject.lookUpTable[address]

	// retrieve block using block index
	interestedBlock = diskObject.secondLevelIndex[recordLocationObject.blockIndex]

	// retrieve recordObject
	recordObject,err = AddrToRecord(address)
	if err!=nil{
		panic("Unable to delete record as recordObject could not be formed from address")
	}

	// set deleted flag to true
	recordObject.deleted = true

	// convert new recordObject
	bytes,err = recordToBytes(recordObject)
	if err!=nil{
		panic("Unable to delete record as byte array could not be formed")
	}

	// copy back into block
	copy(interestedBlock.recordValueArray[recordLocationObject.recordIndex*diskObject.recordSize],bytes)

	// append to deletedArray
	append(diskObject.deletedArray,recordLocation)

	return true
}