// examples for godbf, pasing show2003.dbf
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
	file := "show2003.dbf"
	db, err := godbf.NewDBFTable(file, decoder)
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
	fmt.Printf("total record %d\n", len(rs))
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
	S1  string  `dbf:"S1"`
	S2  string  `dbf:"S2"`
	S3  float64 `dbf:"S3"`
	S4  float64 `dbf:"S4"`
	S5  int64   `dbf:"S5"`
	S6  float64 `dbf:"S6"`
	S7  float64 `dbf:"S7"`
	S8  float64 `dbf:"S8"`
	S9  float64 `dbf:"S9"`
	S10 float64 `dbf:"S10"`
	S11 int64   `dbf:"S11"`
	S13 float64 `dbf:"S13"`
	S15 int64   `dbf:"S15"`
	S16 float64 `dbf:"S16"`
	S17 int64   `dbf:"S17"`
	S18 float64 `dbf:"S18"`
	S19 int64   `dbf:"S19"`
	S21 int64   `dbf:"S21"`
	S22 float64 `dbf:"S22"`
	S23 int64   `dbf:"S23"`
	S24 float64 `dbf:"S24"`
	S25 int64   `dbf:"S25"`
	S26 float64 `dbf:"S26"`
	S27 int64   `dbf:"S27"`
	S28 float64 `dbf:"S28"`
	S29 int64   `dbf:"S29"`
	S30 float64 `dbf:"S30"`
	S31 int64   `dbf:"S31"`
	S32 float64 `dbf:"S32"`
	S33 int64   `dbf:"S33"`
}
