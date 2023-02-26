package main

import "CZ4031_Project_1/experiments"

func main() {
	// run experiments.go
	experiments.Experiments(200)
}

/*
	// initialise disk
	fmt.Println("Initialising disk and record")
	var diskObject storage.Disk
	var recordLocation storage.RecordLocation
	diskObject = storage.CreateDisk(100, 100)
	diskObject.CreateRecord("hello", 1.0, 1)
	// test retrieve all
	fmt.Println("----Test RetrieveAll()----")
	fmt.Println(diskObject.RetrieveAll())
	// test retrieve recordlocation
	fmt.Println("----Test RetrieveRecord----")
	fmt.Println(diskObject.RetrieveRecord(recordLocation))
	// // test delete record
	fmt.Println("----Test DeleteRecord----")
	fmt.Println(diskObject.RetrieveAll())
	diskObject.DeleteRecord(recordLocation)
	fmt.Println(diskObject.RetrieveAll())
}
*/
