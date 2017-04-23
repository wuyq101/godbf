// examples for godbf
// author: wuyq101
// +build ignore
package main

import (
	"fmt"
	"github.com/wuyq101/godbf"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func main() {
	//open an dbf file
	// using gbk decoder
	gbk := simplifiedchinese.GBK.NewDecoder()
	decoder := func(b []byte) ([]byte, error) {
		return gbk.Bytes(b)
	}
	db, err := godbf.NewDBFTable("SJSHQ.DBF", decoder)
	if err != nil {
		fmt.Println("error: ", err)
		return
	}
	// read all records
	rs, err := db.GetAllRecords()
	if err != nil {
		fmt.Println("error while get all records: ", err)
		return
	}
	// print some records
	for i := 0; i < 10 && i < len(rs); i++ {
		fmt.Printf("record %d: %+v \n", i, rs[i])
	}
	// user defined struct
	var stocks []*Stock
	err = db.Unmarshal(&stocks)
	if err != nil {
		fmt.Println("dbf file unmarshal error: ", err)
		return
	}
	//print some records
	for i := 0; i < 20 && i < len(stocks); i++ {
		fmt.Printf("stock %d: %+v \n", i, stocks[i])
	}
}

type Stock struct {
	Name          string  `dbf:"HQZQJC"`
	Code          string  `dbf:"HQZQDM"`
	PreClosePrice float64 `dbf:"HQZRSP"`
	OpenPrice     float64 `dbf:"HQJRKP"`
	CurrentPrice  float64 `dbf:"HQZJCJ"`
	Volumn        int64   `dbf:"HQCJSL"`
}
