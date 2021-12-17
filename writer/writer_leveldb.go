package writer

import (
	"fmt"
	"time"

	leveldb "github.com/pingcap/goleveldb/leveldb"
)


type LevelDriver struct {
	DB *leveldb.DB
}

func NewLevelDriver(pathname string) (*LevelDriver , error) {
	db , err := leveldb.OpenFile(pathname, nil)
	if err != nil {
		fmt.Println("[Error]: Open level DB file failed with error: " , err)
		return nil , err
	}

	levelDriver := new(LevelDriver)
	levelDriver.DB = db
	return levelDriver , nil
}

func (driver *LevelDriver) Set(kvPairs [][]string , size int , dataSize int) error {
	var PutRows int = 0
	
	startTime := time.Now().Unix()

	fmt.Println("[Info]: GoLevelDB Set start time:" ,startTime)

	for i := 0; i < size; i++ {
		err := driver.DB.Put([]byte(kvPairs[i][0]) , []byte(kvPairs[i][1]) , nil)
		if err != nil {
			return nil
		} else {
			PutRows++
		}
	}


	endTime := time.Now().Unix()
	fmt.Println("[Info]: GoLevelDB SET end time: " , endTime)

	TPS := float64(PutRows) / float64(endTime - startTime)
	fmt.Printf("[Info]: GoLevelDB Put [%d] Byte data\n" , dataSize)
	fmt.Printf("[Info]: GoLevelDB Put Rows: %d\n" , PutRows)
	fmt.Printf("[Info]: GoLevelDB Put TPS: %v w/sec\n" , TPS)

	return nil
}

