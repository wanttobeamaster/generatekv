package main

import (
	"fmt"

	myWriter "github.com/wanttobeamaster/generatekv/writer"
	myReader "github.com/wanttobeamaster/generatekv/reader"
)

const boltWriterPath = "./bolt_writer"
const levelWriterPath = "./level_writer"
const rocksWritePath = "./rocks_writer"
const kvDataPath = "key16_value16_10m.txt"

func main() {
	fmt.Println("[Info]: Begin test Set performance!")

	kvPairs , dataLen , dataSize := myReader.Reader(kvDataPath)
	if dataLen == 0 {
		fmt.Println("[Error]: Reader data error")
		return
	}

	BoltDriver , err := myWriter.NewBoltDriver(boltWriterPath)
	if err != nil {
		fmt.Println("[Error]: Create BoltDriver when writer failed with error: " , err)
		return
	}

//	LevelDriver , err := myWriter.NewLevelDriver(levelWriterPath)
//	if err != nil {
//		fmt.Println("[Error]: Create LevelDriver when writer failed with error: " , err)
//		return
//	}

	RockDriver , err := myWriter.NewRocksDBDriver(rocksWritePath)
	if err != nil {
		fmt.Println("[Error]: Create rocksDriver when writer failed with error: " , err)
		return
	}

	if err := BoltDBSet(BoltDriver , kvPairs , dataLen , dataSize); err != nil {
		fmt.Println("[Error]: " , err)
	}
	
	if err := BoltDBGet(BoltDriver , kvPairs , dataLen); err != nil {
		fmt.Println("[Error]: " , err)
	}

	if err := BoltDBScan(BoltDriver , kvPairs , dataLen , dataSize); err != nil {
		fmt.Println("[Error]: " , err)
	}

//	if err := GoLevelDBSet(LevelDriver , kvPairs , dataLen , dataSize); err != nil {
//		fmt.Println("[Error]: " , err)
//	}
	
	if err := GoRocksDBSet(RockDriver , kvPairs , dataLen , dataSize); err != nil {
		fmt.Println("[Error]: " , err)
	}

	if err := GoRocksDBScan(RockDriver , kvPairs , dataLen , dataSize); err != nil {
		fmt.Println("[Error]: " , err)
	}

	if err := GoRocksDBGet(RockDriver , kvPairs , dataLen , dataSize); err != nil {
		fmt.Println("[Error]: " , err)
	}

	fmt.Println("[Info]: Finish test Set performance!")
}


func BoltDBSet(driver *myWriter.BoltDriver , kvPairs [][]string , dataLen int , dataSize int) error {
	return driver.Set(kvPairs , dataLen , dataSize)
}

func BoltDBGet(driver *myWriter.BoltDriver , kvPairs [][]string , dataLen int) error {
	return driver.Get(kvPairs , dataLen) 
}

func BoltDBScan(driver *myWriter.BoltDriver , kvPairs [][]string , dataLen int , dataSize int) error {
	return driver.Scan(kvPairs , dataLen , dataSize)
}

func GoLevelDBSet(driver *myWriter.LevelDriver , kvPairs [][]string , dataLen int , dataSize int) error {
	return driver.Set(kvPairs , dataLen , dataSize)
}

func GoRocksDBSet(driver *myWriter.RocksDBDriver , kvPairs [][]string , dataLen int , dataSize int) error {
	return driver.Set(kvPairs , dataLen , dataSize)
}

func GoRocksDBGet(driver *myWriter.RocksDBDriver , kvPairs [][]string , dataLen int , dataSize int) error {
	return driver.Get(kvPairs , dataLen , dataSize)
}

func GoRocksDBScan(driver *myWriter.RocksDBDriver , kvPairs [][]string , dataLen int , dataSize int) error {
	return driver.Scan(kvPairs , dataLen , dataSize)
}
