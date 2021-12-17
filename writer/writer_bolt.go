package writer

import (
	"fmt"
	"time"

	boltdb "github.com/boltdb/bolt"
	util "github.com/wanttobeamaster/generatekv/util"
)

const BucketName = "test"

type BoltDriver struct {
	DB *boltdb.DB
}

func NewBoltDriver(filepath string)(*BoltDriver , error) {
	DB , err := boltdb.Open(filepath , 0666 , nil)
	if err != nil {
		return nil , err
	}

	boltDriver := new(BoltDriver)
	boltDriver.DB = DB
	return boltDriver , nil
}

func (driver *BoltDriver) Set(kvPairs [][]string , size int , dataSize int) error {
	var PutRows int = 0
	
	startTime := time.Now().Unix()
	
	fmt.Println("[Info]: BoltDB SET start time: " , startTime)

	for i := 0; i < size; i++ {
		err := driver.DB.Update(func (tx *boltdb.Tx) error {
			bucket , err := tx.CreateBucketIfNotExists([]byte(BucketName))
			if err != nil {
				return err
			}

			err = bucket.Put([]byte(kvPairs[i][0]) , []byte(kvPairs[i][1]))
			if err != nil {
				fmt.Println("[Error]: PUT " , i , "th row failed! ")
				return err
			}else {
				PutRows++
			}

			return nil
		})

		if err != nil{
			return err
		}
		
	}

	endTime := time.Now().Unix()
	fmt.Println("[Info]: BoltDB SET end time: " , endTime)

	TPS := float64(PutRows) / float64(endTime - startTime)
	fmt.Printf("[Info]: BoltDB Put [%d] Byte data\n" , dataSize)
	fmt.Printf("[Info]: BoltDB Put Rows: %d\n" , PutRows)
	fmt.Printf("[Info]: BoltDB Put TPS: %v w/sec\n" , TPS)
	
	return nil
}

func (driver *BoltDriver) Scan(kvPairs [][]string , size int , dataSize int) error {
	var GetTimes int = 0

	startTime := time.Now().Unix()
	fmt.Println("[Info]: BoltDB Scan start time: " , startTime)

	err := driver.DB.Update(func(tx *boltdb.Tx) error {
		bucket , err := tx.CreateBucketIfNotExists([]byte(BucketName))
		if err != nil {
			return fmt.Errorf("Create bucket faile with error: %s" , err)
		}

		cursor := bucket.Cursor()

		for i := 0; i + util.ScanStep < size; i++ {
			times := 200
			for k , _ := cursor.Seek([]byte(kvPairs[i][0])); k != nil && times >= 0; k , _ = cursor.Next() {
				times--
			}

			GetTimes++
		}

		return nil
	})

	if err != nil {
		return err
	}

	endTime := time.Now().Unix()
	fmt.Println("[Info]: BoltDB Scan end time: " , endTime)

	TPS := float64(GetTimes) / float64(endTime - startTime)
	fmt.Printf("[Info]: BoltDB Scan Rows: %d\n" , GetTimes)
	fmt.Printf("[Info]: BoltDB Scan TPS: %v w/sec\n" , TPS)
	
	return nil
}

func (driver *BoltDriver) Get(kvPairs [][]string , size int) error {
	var GetRows int = 0

	startTime := time.Now().Unix()
	fmt.Println("[Info]: BoltDB Get start time: " , startTime)

	for i := 0; i < size; i++ {
		err := driver.DB.Update(func (tx *boltdb.Tx) error {
			bucket , err := tx.CreateBucketIfNotExists([]byte(BucketName))
			if err != nil {
				return fmt.Errorf("Create bucket failed when Get: %v" , err)
			}

			bucket.Get([]byte(kvPairs[i][0]))
			GetRows++

			return nil
		})

		if err != nil {
			return err
		}
	}


	endTime := time.Now().Unix()
	fmt.Println("[Info]: BoltDB Get end time: " , endTime)

	TPS := float64(GetRows) / float64(endTime - startTime)
	fmt.Printf("[Info]: BoltDB Get Rows: %d\n" , GetRows)
	fmt.Printf("[Info]: BoltDB Get TPS: %v w/sec\n" , TPS)
	
	return nil
}
