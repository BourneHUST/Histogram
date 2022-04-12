package Histogram

import "strings"

type BasicStatistics struct {
	NumOfNULL     int
	TotalRows     uint64
	MAX           interface{}
	MIN           interface{}
	AverageLength uint64
	NDV           uint64
}

func NewBasicStatistics() *BasicStatistics {
	BS := new(BasicStatistics)
	return BS
}
func (BS *BasicStatistics) GetMAX() interface{} {
	return BS.MAX
}
func (BS *BasicStatistics) GetMIN() interface{} {
	return BS.MIN
}
func (BS *BasicStatistics) GetNULL() int {
	return BS.NumOfNULL
}
func (BS *BasicStatistics) GetAverageLength() uint64 {
	return BS.AverageLength
}
func (BS *BasicStatistics) GetNDV() interface{} {
	return BS.NDV
}
func (BS *BasicStatistics) GetRows() uint64 {
	return BS.TotalRows
}
func (BS *BasicStatistics) Gather(value interface{}, Type string) {

	BS.TotalRows += 1

	Types := strings.Split(Type, " ")

	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			value1 := value.(uint64)
			value2 := BS.MIN.(uint64)
			value3 := BS.MAX.(uint64)
			if value1 < value2 {
				BS.MIN = value1
			}
			if value1 > value3 {
				BS.MAX = value1
			}
		} else {
			value1 := value.(int64)
			value2 := BS.MIN.(int64)
			value3 := BS.MAX.(int64)
			if value1 < value2 {
				BS.MIN = value1
			}
			if value1 > value3 {
				BS.MAX = value1
			}
		}
		BS.AverageLength += 8

	} else if Types[0] == "FLOAT" {
		value1 := value.(float64)
		value2 := BS.MIN.(float64)
		value3 := BS.MAX.(float64)
		if value1 < value2 {
			BS.MIN = value1
		}
		if value1 > value3 {
			BS.MAX = value1
		}
		BS.AverageLength += 8
	} else {
		value1 := value.(string)
		value2 := BS.MIN.(string)
		value3 := BS.MAX.(string)
		if Types[0] == "DECIMAL" {
			if DigitLess(value1, value2) {
				BS.MIN = value1
			}
			if DigitLess(value3, value2) {
				BS.MAX = value1
			}
		} else {
			if value1 < value2 {
				BS.MIN = value1
			}
			if value1 > value3 {
				BS.MAX = value1
			}
		}
		BS.AverageLength += uint64(len(value1))
	}
}
