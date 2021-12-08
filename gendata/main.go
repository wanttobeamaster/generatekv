package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	myreader "github.com/wanttobeamaster/generatedata/reader"
)

var SplitString = "=========="

func main(){


	myreader.Reader("sada")
	return

	dataPath := "KeyValueData.txt"
	if IsExist(dataPath) {
		fmt.Println("[Error]: Key Value data is already generated!")
		return
	}

	file , err := os.Open("./data.txt")
	if err != nil {
		fmt.Println("[Error]: Open file failed with error: " , err)
		return
	}
	defer file.Close()

	data, err := ioutil.ReadFile("./data.txt")
	if err != nil {
		fmt.Println("[Error]: Open file failed with error: " , err)
		return
	}

	writeFile , err := os.OpenFile(dataPath , os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0600 )
	if err != nil {
		fmt.Println("[Error]: Open write file failed with error: " , err)
		return
	}

	// 清除回车字符
	data = DeleteCR(data)

	// 写文件
	dataSize := WriteKvFile(writeFile , data)
	io.WriteString(writeFile , strconv.Itoa(dataSize) + "\n")
	fmt.Println("[Info]: Generate data finish!")
}

//判断文件是否存在
func IsExist(fileAddr string) bool {
	_ , err := os.Stat(fileAddr)
	if err != nil {
		return false
	}
	return true
}

func DeleteCR(data []byte) []byte {
	res := make([]byte , 0 , len(data))
	for i := 0; i < len(data); i++ {
		if data[i] != '\n' {
			res = append(res , data[i])
		}
	}

	return res
}

// 写文件
func WriteKvFile(file *os.File , data []byte) int {
	var totalSize int = 0
	var startIdx int = 0
	keySize := 8			// 8B
	valueSize := 1024		// 1KB
	var LimitSize int = 1 * 1024 * 1024 * 1024 // 1GB
	
	dataSize := len(data)
	maxIndex := dataSize - 1024
	for {
		if data[startIdx] == '\n' || data[startIdx] == ' ' {
			startIdx++
			continue
		} 
	
		if startIdx > maxIndex {
			break
		}

		key := data[startIdx : startIdx + keySize]
		value := data[startIdx :startIdx + valueSize]
		io.WriteString(file , string(key))
		io.WriteString(file , SplitString)
		io.WriteString(file , string(value))
		io.WriteString(file , "\n")

		totalSize += len(key) + len(value)
		if totalSize >= LimitSize {
			break
		}
	}

	return totalSize
}
