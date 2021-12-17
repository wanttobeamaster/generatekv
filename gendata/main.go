package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	util "github.com/wanttobeamaster/generatekv/util"
)

func main() {
	dataPath := "KeyValueData.txt"
	dataPathDiffValue := "KeyValueDataDiffValue.txt"
	if IsExist(dataPath) || IsExist(dataPathDiffValue){
		fmt.Println("[Error]: Key Value data is already generated!")
		return
	}

	file, err := os.Open("./data.txt")
	if err != nil {
		fmt.Println("[Error]: Open file failed with error: ", err)
		return
	}
	defer file.Close()

	data, err := ioutil.ReadFile("./data.txt")
	if err != nil {
		fmt.Println("[Error]: Open file failed with error: ", err)
		return
	}
	fmt.Println("[Info]: len(data)=", len(data))

	writeFile, err := os.OpenFile(dataPath, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0600)
	writeFile1 , err1 := os.OpenFile(dataPathDiffValue , os.O_APPEND | os.O_WRONLY | os.O_CREATE , 0600)
	if err != nil || err1 != nil {
		fmt.Println("[Error]: Open file faile with error: " , err , err1)
		return
	}

	// 清除回车字符
	data = DeleteCR(data)

	// 写文件
	WriteKvFile(writeFile,  writeFile1 , data)
	fmt.Println("[Info]: Generate data finish!")
}

//判断文件是否存在
func IsExist(fileAddr string) bool {
	_, err := os.Stat(fileAddr)
	if err != nil {
		return false
	}
	return true
}

func DeleteCR(data []byte) []byte {
	res := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] != '\n' {
			res = append(res, data[i])
		}
	}

	return res
}

// 写文件
func WriteKvFile(file *os.File, file1 *os.File , data []byte) int {
	var totalSize int = 0
	var startIdx int = 0

	dataSize := len(data)
	maxIndex := dataSize - util.ValueSize
	for {
		if data[startIdx] == '\n' || data[startIdx] == ' ' {
			startIdx++
			continue
		}

		if startIdx > maxIndex {
			fmt.Println("[Info]: startIdx > maxIndex")
			break
		}

		key := data[startIdx : startIdx + util.KeySize]
		value := data[startIdx : startIdx + util.ValueSize]
		valueDiff := data[startIdx + util.KeySize : startIdx + util.KeySize + util.ValueSize + util.KeySize]

		io.WriteString(file, string(key))
		io.WriteString(file, util.SplitString)
		io.WriteString(file, string(value))
		io.WriteString(file, "\n")

		io.WriteString(file1 , string(key))
		io.WriteString(file1 , util.SplitString)
		io.WriteString(file1 , string(valueDiff))
		io.WriteString(file1 , "\n")

		totalSize += len(key) + len(value)
		if totalSize >= util.LimitSize {
			fmt.Println("[Info]: Current Size = ", totalSize)
			break
		}
		startIdx++
	}

	return totalSize
}
