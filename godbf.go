package godbf

import (
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// The .dbf file extension represents the dBASE database file.
// The file type was introduced in 1983 with the introduction of dBASE II.
// The file structure has evolved over the years to include many more features and capabilities and has introduced various other files
// to help support data storage and manipulation. The current .dbf file level is called Level 7.
// The .dbf format is supported by a number of database products.

// DBFHeader the first part of dbf file.
// file format definition: https://en.wikipedia.org/wiki/.dbf
type DBFHeader struct {
	FileSignature         byte      // 0 byte
	UpdateDate            time.Time // 1-3 bytes YYMMDD
	NumberOfRecords       uint32    // 4-7 bytes 32-bit number
	NumberOfBytesInHeader uint16    // 8-9 bytes 16-bit number
	LengthOfRecord        uint16    // 10-11 bytes 16-bit number
}

// DBFField dbf field
// start from byte 32
type DBFField struct {
	FieldName         string // 0-10 bytes
	FieldType         byte   // 11 byte 'C,D,F,L,N'
	FieldLength       uint8  // 16 byte
	FieldDecimalCount uint8  // 17 byte
	offset            int
}

// DBFRecord in map, support int64, float64, string value
type DBFRecord struct {
	IsDeleted bool
	Data      map[string]interface{}
}

// DBFTable open a .dbf file will return a dbf table
type DBFTable struct {
	Header         *DBFHeader
	Fields         []*DBFField
	numberOfFields int
	decoder        Decoder
	body           []byte
}

// Decoder for transform text encoding
type Decoder func([]byte) ([]byte, error)

// NewDBFTable will open the file with user provided decoder, and return a dbf table
func NewDBFTable(file string, decoder Decoder) (*DBFTable, error) {
	body, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return NewDBFTableByContent(body, decoder)
}

// NewDBFTableByContent dbf content to create a dbf table
func NewDBFTableByContent(body []byte, decoder Decoder) (*DBFTable, error) {
	db := &DBFTable{}
	db.decoder = decoder
	db.body = body
	// parse header
	header, err := parseHeader(body)
	if err != nil {
		return nil, err
	}
	db.Header = header
	//parse fields
	db.numberOfFields = int((header.NumberOfBytesInHeader - 1 - 32) / 32)
	fields, err := parseFields(body, db.numberOfFields, decoder)
	if err != nil {
		return nil, err
	}
	db.Fields = fields
	return db, nil
}

// NumberOfRecords get number of records in the dbf file.
func (db *DBFTable) NumberOfRecords() (int, error) {
	if db.Header == nil {
		return -1, errors.New("get number of records before parse dbf file header")
	}
	return int(db.Header.NumberOfRecords), nil
}

//GetRecord get one record index by row
func (db *DBFTable) GetRecord(row int) (*DBFRecord, error) {
	if db.Header == nil {
		return nil, errors.New("get records before parse dbf file header")
	}
	if row < 0 || row >= int(db.Header.NumberOfRecords) {
		return nil, fmt.Errorf("row index invalid: %d", row)
	}
	L := len(db.body)
	offset := row*int(db.Header.LengthOfRecord) + int(db.Header.NumberOfBytesInHeader)
	if offset+int(db.Header.LengthOfRecord) > L {
		return nil, fmt.Errorf("dbf file corrupted: no more records for row %d", row)
	}
	r := &DBFRecord{}
	s := db.body[offset : offset+int(db.Header.LengthOfRecord)]
	// 0x2A deleted 0x20 not deleted
	r.IsDeleted = s[0] == '*'
	s = s[1:]
	r.Data = make(map[string]interface{})
	for _, f := range db.Fields {
		data := s[f.offset : f.offset+int(f.FieldLength)]
		switch f.FieldType {
		case 'C':
			v, err := db.stringValue(data)
			if err != nil {
				return nil, err
			}
			r.Data[f.FieldName] = v
		case 'F', 'N':
			tmp := strings.TrimSpace(string(data))
			if f.FieldDecimalCount > 0 {
				// '-.---'
				if "-.---" == tmp {
					r.Data[f.FieldName] = float64(0.0)
					continue
				}
				v, err := strconv.ParseFloat(tmp, 64)
				if err != nil {
					return nil, err
				}
				r.Data[f.FieldName] = v
			} else {
				// "-"
				if "-" == tmp {
					r.Data[f.FieldName] = int64(0)
					continue
				}
				v, err := strconv.ParseInt(tmp, 10, 64)
				if err != nil {
					return nil, err
				}
				r.Data[f.FieldName] = v
			}
		}
	}
	return r, nil
}

func (db *DBFTable) stringValue(data []byte) (string, error) {
	for i := len(data) - 1; i >= 0; i-- {
		if data[i] != 0x00 {
			data = data[0 : i+1]
			break
		}
	}
	if db.decoder == nil {
		return string(data), nil
	}
	tmp, err := db.decoder(data)
	if err != nil {
		return "", err
	}
	return string(tmp), nil
}

// GetAllRecords will read all records
func (db *DBFTable) GetAllRecords() ([]*DBFRecord, error) {
	count := int(db.Header.NumberOfRecords)
	rs := make([]*DBFRecord, count)
	for i := 0; i < count; i++ {
		r, err := db.GetRecord(i)
		if err != nil {
			return nil, err
		}
		rs[i] = r
	}
	return rs, nil
}

// Unmarshal return all records in the user defined type.
// support tag for `dbf:"xxx"`, "xxx" is the field name
// support data type: string int64 float64
// holder type: *[]*A
func (db *DBFTable) Unmarshal(holder interface{}) error {
	if holder == nil {
		return errors.New("v is nil")
	}
	if db.Header == nil {
		return errors.New("unmarshal before parse header")
	}
	v := reflect.ValueOf(holder)
	if v.Kind() != reflect.Ptr && v.Elem().Kind() != reflect.Slice {
		return errors.New("holder should be a ptr to slice")
	}
	v.Elem().Set(reflect.MakeSlice(v.Elem().Type(), int(db.Header.NumberOfRecords), int(db.Header.NumberOfRecords)))
	// item type
	itemType := reflect.TypeOf(holder).Elem().Elem().Elem()
	dbfFieldIndex := make(map[string]int)
	fieldIndex := make(map[string]int)
	for i := 0; i < itemType.NumField(); i++ {
		f := itemType.Field(i)
		tag := f.Tag.Get("dbf")
		if len(tag) > 0 {
			fieldIndex[tag] = i
			for j := 0; j < len(db.Fields); j++ {
				if strings.Compare(db.Fields[j].FieldName, tag) == 0 {
					dbfFieldIndex[tag] = j
					break
				}
			}
		}
	}
	//set to slice
	for i := 0; i < int(db.Header.NumberOfRecords); i++ {
		tmp := reflect.New(itemType)
		sliceV := v.Elem().Index(i)
		sliceV.Set(tmp)
		offset := i*int(db.Header.LengthOfRecord) + int(db.Header.NumberOfBytesInHeader)
		s := db.body[offset+1 : offset+int(db.Header.LengthOfRecord)]
		for tag, fi := range fieldIndex {
			f := db.Fields[dbfFieldIndex[tag]]
			data := s[f.offset : f.offset+int(f.FieldLength)]
			switch f.FieldType {
			case 'C':
				val, err := db.stringValue(data)
				if err != nil {
					return err
				}
				tmp.Elem().Field(fi).SetString(val)
			case 'N', 'F':
				str := strings.TrimSpace(string(data))
				if f.FieldDecimalCount > 0 {
					if "-.---" == str {
						tmp.Elem().Field(fi).SetFloat(float64(0))
						continue
					}
					val, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return err
					}
					tmp.Elem().Field(fi).SetFloat(val)
				} else {
					if "-" == str {
						tmp.Elem().Field(fi).SetInt(int64(0))
						continue
					}
					val, err := strconv.ParseInt(str, 10, 64)
					if err != nil {
						return err
					}
					tmp.Elem().Field(fi).SetInt(val)
				}
			}
		}
	}
	return nil
}

func parseFields(body []byte, numberOfFields int, decoder Decoder) ([]*DBFField, error) {
	L := len(body)
	fields := make([]*DBFField, numberOfFields)
	var (
		name  string
		tp    byte
		start int
	)
	for i := 0; i < numberOfFields; i++ {
		offset := i*32 + 32
		if offset+31 >= L {
			return nil, errors.New("dbf file fields invalid")
		}
		s := body[offset : offset+32]
		// field name
		end := 10
		for end > 0 && s[end-1] == 0x00 {
			end--
		}
		if decoder == nil {
			name = string(s[0:end])
		} else {
			tmp, err := decoder(s[0:end])
			if err != nil {
				return nil, errors.New("dbf field name decoding err " + err.Error())
			}
			name = string(tmp)
		}
		// field type
		tp = s[11]
		fd := &DBFField{
			FieldName:   name,
			FieldType:   tp,
			FieldLength: s[16],
			offset:      start,
		}
		fields[i] = fd
		if tp == 'N' || tp == 'F' {
			fd.FieldDecimalCount = s[17]
		}
		start += int(fd.FieldLength)
	}
	return fields, nil
}

func parseHeader(body []byte) (*DBFHeader, error) {
	if len(body) < 12 {
		return nil, errors.New("dbf file header invalid")
	}
	h := &DBFHeader{}
	h.FileSignature = body[0]
	year, month, day := int(body[1]), time.Month(body[2]), int(body[3])
	h.UpdateDate = time.Date(1900+year, month, day, 0, 0, 0, 0, time.Local)
	h.NumberOfRecords = uint32(uint32(body[4]) | (uint32(body[5]) << 8) | (uint32(body[6]) << 16) | (uint32(body[7]) << 24))
	h.NumberOfBytesInHeader = uint16(body[8]) | (uint16(body[9]) << 8)
	h.LengthOfRecord = uint16(body[10]) | (uint16(body[11]) << 8)
	return h, nil
}
