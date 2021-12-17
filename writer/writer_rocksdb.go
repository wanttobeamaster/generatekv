package writer

import (
	"fmt"
	"time"

	rocksdb "github.com/tecbot/gorocksdb"
	util "github.com/wanttobeamaster/generatekv/util"
)

type RocksDBDriver struct {
	DB *rocksdb.DB
}

func NewRocksDBDriver(pathName string) (*RocksDBDriver, error) {
	bbto := rocksdb.NewDefaultBlockBasedTableOptions()
	// bbto.SetBlockCache(rocksdb.NewLRUCache(3 << 10))
	opts := rocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	db, err := rocksdb.OpenDb(opts, pathName)
	if err != nil {
		return nil, err
	}

	rocksdbDriver := new(RocksDBDriver)
	rocksdbDriver.DB = db

	return rocksdbDriver, nil
}

func (driver *RocksDBDriver) Set(kvPairs [][]string, size int, dataSize int) error {
	var PutRows int = 0

	startTime := time.Now().Unix()

	fmt.Println("[Info]: GoRocksDB Set start time: ", startTime)

	for i := 0; i < size; i++ {
		wo := rocksdb.NewDefaultWriteOptions()
		// wo.SetSync(true)
		err := driver.DB.Put(wo, []byte(kvPairs[i][0]), []byte(kvPairs[i][1]))
		if err != nil {
			return err
		} else {
			PutRows++
		}
	}

	endTime := time.Now().Unix()
	fmt.Println("[Info]: GoRocksDB SET end time: ", endTime)

	TPS := float64(PutRows) / float64(endTime-startTime)
	fmt.Printf("[Info]: GoRocksDB Put [%d] Byte data\n", dataSize)
	fmt.Printf("[Info]: GoRocksDB Put Rows: %d\n", PutRows)
	fmt.Printf("[Info]: GoRocksDB Put TPS: %v w/sec\n", TPS)

	return nil
}

func (driver *RocksDBDriver) Scan(kvPairs [][]string, size int, dataSize int) error {
	var GetTimes int = 0

	startTime := time.Now().Unix()
	fmt.Println("[Info]: GoRocksDB Scan start time: ", startTime)

	for i := 0; i+util.ScanStep < size; i++ {
		ro := rocksdb.NewDefaultReadOptions()
		ro.SetFillCache(false)
		it := driver.DB.NewIterator(ro)

		defer it.Close()

		it.Seek([]byte(kvPairs[i][0]))
		
		times := 200
		for it := it; it.Valid() && times >= 0; it.Next() {
			times--
		}

		GetTimes++

	}

	endTime := time.Now().Unix()
	fmt.Println("[Info]: GoRocksDB Scan end time: ", endTime)

	TPS := float64(GetTimes) / float64(endTime-startTime)
	fmt.Printf("[Info]: GoRocksDB Scan Rows: %d\n", GetTimes)
	fmt.Printf("[Info]: GoRocksDB Scan TPS: %v w/sec\n", TPS)

	return nil
}

func (driver *RocksDBDriver) Get(kvPairs [][]string, size int, dataSize int) error {
	var GetRows int = 0

	startTime := time.Now().Unix()

	fmt.Println("[Info]: GoRocksDB Get start time: ", startTime)

	for i := 0; i < size; i++ {
		ro := rocksdb.NewDefaultReadOptions()
		ro.SetFillCache(false)
		_, err := driver.DB.Get(rocksdb.NewDefaultReadOptions(), []byte(kvPairs[i][0]))
		if err != nil {
			return err
		} else {
			GetRows++
		}
	}

	endTime := time.Now().Unix()
	fmt.Println("[Info]: GoRocksDB Get end time: ", endTime)

	TPS := float64(GetRows) / float64(endTime-startTime)
	fmt.Printf("[Info]: GoRocksDB Get Rows: %d\n", GetRows)
	fmt.Printf("[Info]: GoRocksDB Get TPS: %v w/sec\n", TPS)

	return nil
}
