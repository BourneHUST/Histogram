package Histogram

import (
	"strconv"
	"strings"
	"time"
)

type HybridBucket struct {
	CumFrequency  uint64
	EndpointValue interface{}
	RepeatCount   uint64
}
type HybridHistogram struct {
	BS             *BasicStatistics
	Buckets        []HybridBucket
	LastUpdateTime time.Time
	Type           string

	FreqCount uint64
	FreqCum   uint64
	RowsCount uint64
	NDV       uint64
}
type TotalHybridHistogram struct {
	TotalHy map[string]HybridHistogram
}

func (HY *HybridHistogram) GetEqualCardinality(value interface{}) uint64 {

	Types := strings.Split(HY.Type, " ")
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := strconv.ParseUint(value.(string), 10, 64)
			for i := 0; i < len(HY.Buckets); i++ {
				if Value1 == HY.Buckets[i].EndpointValue.(uint64) {
					return HY.Buckets[i].RepeatCount
				}
			}
			return (HY.BS.TotalRows - HY.FreqCum) / (HY.BS.NDV - HY.FreqCount)
		} else {
			Value1, _ := strconv.ParseInt(value.(string), 10, 64)
			for i := 0; i < len(HY.Buckets); i++ {
				if Value1 == HY.Buckets[i].EndpointValue.(int64) {
					return HY.Buckets[i].RepeatCount
				}
			}
			return (HY.BS.TotalRows - HY.FreqCum) / (HY.BS.NDV - HY.FreqCount)
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := strconv.ParseFloat(value.(string), 64)
		for i := 0; i < len(HY.Buckets); i++ {
			if Value1 == HY.Buckets[i].EndpointValue.(float64) {
				return HY.Buckets[i].RepeatCount
			}
		}
		return (HY.BS.TotalRows - HY.FreqCum) / (HY.BS.NDV - HY.FreqCount)
	} else {
		Value1, _ := value.(string)
		for i := 0; i < len(HY.Buckets); i++ {
			if Value1 == HY.Buckets[i].EndpointValue.(string) {
				return HY.Buckets[i].RepeatCount
			}
		}

		return (HY.BS.TotalRows - HY.FreqCum) / (HY.BS.NDV - HY.FreqCount)
	}
}

func (HY *HybridHistogram) GetLessEqualCardinality(value interface{}) uint64 {
	Types := strings.Split(HY.Type, " ")
	var i int
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := strconv.ParseUint(value.(string), 10, 64)
			for i = 0; i < len(HY.Buckets); i++ {
				if Value1 >= HY.Buckets[i].EndpointValue.(uint64) {
					continue
				}
				break
			}
			if i == 0 {
				return 0
			}
			return uint64((float64(HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
		} else {
			Value1, _ := strconv.ParseInt(value.(string), 10, 64)
			for i = 0; i < len(HY.Buckets); i++ {
				if Value1 >= HY.Buckets[i].EndpointValue.(int64) {
					continue
				}
				break
			}
			if i == 0 {
				return 0
			}
			return uint64((float64(HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := strconv.ParseFloat(value.(string), 64)
		for i = 0; i < len(HY.Buckets); i++ {
			if Value1 >= HY.Buckets[i].EndpointValue.(float64) {
				continue
			}
			break
		}
		if i == 0 {
			return 0
		}
		return uint64((float64(HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
	} else {
		Value1, _ := value.(string)
		for i = 0; i < len(HY.Buckets); i++ {
			if Value1 >= HY.Buckets[i].EndpointValue.(string) {
				continue
			}
			break
		}
		if i == 0 {
			return 0
		}
		return uint64((float64(HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
	}
}

func (HY *HybridHistogram) GetGreaterEqualCardinality(value interface{}) uint64 {
	Types := strings.Split(HY.Type, " ")
	var i int
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := strconv.ParseUint(value.(string), 10, 64)
			for i = 0; i < len(HY.Buckets); i++ {
				if Value1 > HY.Buckets[i].EndpointValue.(uint64) {
					continue
				}
				break
			}
			if i == 0 {
				return 0
			}
			return uint64((float64(HY.RowsCount-HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
		} else {
			Value1, _ := strconv.ParseInt(value.(string), 10, 64)
			for i = 0; i < len(HY.Buckets); i++ {
				if Value1 > HY.Buckets[i].EndpointValue.(int64) {
					continue
				}
				break
			}
			if i == 0 {
				return 0
			}
			return uint64((float64(HY.RowsCount-HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := strconv.ParseFloat(value.(string), 64)
		for i = 0; i < len(HY.Buckets); i++ {
			if Value1 > HY.Buckets[i].EndpointValue.(float64) {
				continue
			}
			break
		}
		if i == 0 {
			return 0
		}
		return uint64((float64(HY.RowsCount-HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
	} else {
		Value1, _ := value.(string)
		for i = 0; i < len(HY.Buckets); i++ {
			if Value1 > HY.Buckets[i].EndpointValue.(string) {
				continue
			}
			break
		}
		if i == 0 {
			return 0
		}
		return uint64((float64(HY.RowsCount-HY.Buckets[i-1].CumFrequency) / float64(HY.RowsCount)) * float64(HY.BS.TotalRows))
	}
}
