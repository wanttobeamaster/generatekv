package reader

import (
	"fmt"
	"io"
	"os"
	"bufio"
	"strings"

	"github.com/wanttobeamaster/generatekv/util"
)

func Reader(filename string)([][]string , int , int) {
	file , err := os.Open(filename)
	if err != nil {
		fmt.Println("[Error]: Open file failed in Reader with error: " , err)
		return nil , 0 , 0
	}
	defer file.Close()
	
	lineReader := bufio.NewReader(file)
	res := [][]string{}
	dataSize := 0

	for {
		data , _ , err := lineReader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("[Error]: RaderLine failed in Reader with error: " , err)
			return nil , 0 , 0
		}
		
		str := string(data)
		if splitRes := strings.Split(str , util.SplitString); len(splitRes) == 2 {
			key , value := splitRes[0] , splitRes[1]
			res = append(res , []string{key , value})
			dataSize += len(key) + len(value)
		}
	}

	return res , len(res) , dataSize
}
