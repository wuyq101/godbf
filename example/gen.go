// +build ignore
// generate go struct from dbf file.
// author: wuyq101
package main

import (
	"flag"
	"fmt"

	"github.com/wuyq101/godbf"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func main() {
	var (
		file     string
		encoding string
		name     string
	)
	flag.StringVar(&file, "file", "", "the dbf file to parse")
	flag.StringVar(&encoding, "encoding", "", "the charset encoding to read the file, support value: gbk")
	flag.StringVar(&name, "name", "", "the struct name")
	flag.Parse()

	if len(file) == 0 || len(name) == 0 {
		usage()
		return
	}

	if len(encoding) > 0 && encoding != "gbk" {
		fmt.Println("only support encoding for gbk right now.")
		return
	}

	var decoder func([]byte) ([]byte, error)
	if encoding == "gbk" {
		gbk := simplifiedchinese.GBK.NewDecoder()
		decoder = func(b []byte) ([]byte, error) {
			return gbk.Bytes(b)
		}
	}
	db, err := godbf.NewDBFTable(file, decoder)
	if err != nil {
		fmt.Printf("parse dbf file %s, error: %s", file, err.Error())
		return
	}
	fmt.Printf("parse dbf file %s finished.\n", file)
	fmt.Printf("there are %d fields, generate struct: \n", len(db.Fields))

	code := fmt.Sprintf("type %s struct {\n", name)

	for i := 0; i < len(db.Fields); i++ {
		f := db.Fields[i]
		v := f.FieldName
		t := "string"
		if f.FieldType == 'N' || f.FieldType == 'F' {
			if f.FieldDecimalCount > 0 {
				t = "float64"
			} else {
				t = "int64"
			}
		}
		buf := fmt.Sprintf("\t%s\t%s\t`dbf:\"%s\"`\n", v, t, v)
		code += buf
	}
	code += "}"
	fmt.Println("-----------------------------------\n")
	fmt.Println(code)
	fmt.Println("\n-----------------------------------")
}

func usage() {
	desc := `Usage of gen:
	gen -file=xxxx.dbf -name=Stock [-encoding=gbk]
It will generate go struct code, and print it to std, like:

---------------
type Stock struct {
	HQZQDM string ` + "`" + `dbf:"HQZQDM"` + "`" + `
	HQZRSP float64 ` + "`" + `dbf:"HQZRSP"` + "`" + `
}
---------------

Flags:
  -file
    the dbf file to parse
  -name
    the struct name, required
  -encoding
    the charset encoding to read the file, support value: gbk
`
	fmt.Println(desc)
}
