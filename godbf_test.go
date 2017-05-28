package godbf

import (
	"fmt"
	"testing"

	"golang.org/x/text/encoding/simplifiedchinese"
)

func TestNewDBFTable(t *testing.T) {
	file := "example/SJSHQ.DBF"
	db, err := NewDBFTable(file, gbkDecoder)
	if err != nil {
		fmt.Printf("err: %+v", err)
		return
	}
	//	fmt.Printf("dbf: %+v", db)
	rs, err := db.GetAllRecords()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(rs))
	//	for _, r := range rs {
	//		fmt.Printf("%v\n", r)
	//	}
}

var gbk = simplifiedchinese.GBK.NewDecoder()

func gbkDecoder(b []byte) ([]byte, error) {
	return gbk.Bytes(b)
}

type Stock struct {
	Name          string  `dbf:"HQZQJC"`
	Code          string  `dbf:"HQZQDM"`
	PreClosePrice float64 `dbf:"HQZRSP"`
	OpenPrice     float64 `dbf:"HQJRKP"`
	CurrentPrice  float64 `dbf:"HQZJCJ"`
	Volumn        int64   `dbf:"HQCJSL"`
}

func BenchmarkUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, _ := NewDBFTable("example/SJSHQ.DBF", gbkDecoder)
		var holder []*Stock
		db.Unmarshal(&holder)
	}
}

func BenchmarkGetAllRecords(b *testing.B) {
	db, _ := NewDBFTable("example/SJSHQ.DBF", gbkDecoder)
	for i := 0; i < b.N; i++ {
		db.GetAllRecords()
	}
}

func TestUnmarshal(t *testing.T) {
	val := "HQZQDM"
	fmt.Printf("%s %d\n", val, len(val))
	db, err := NewDBFTable("example/SJSHQ.DBF", gbkDecoder)
	if err != nil {
		t.Fatal(err)
	}
	var holder []*Stock
	fmt.Printf("before %+v cap %d \n", holder, cap(holder))
	err = db.Unmarshal(&holder)
	if err != nil {
		t.Fatal(err)
	}
	//	fmt.Printf("after %q cap %d \n", holder, cap(holder))
	for i := 0; i < 10 && i < len(holder); i++ {
		fmt.Printf("record %d: %+v \n", i, holder[i])
	}
}
