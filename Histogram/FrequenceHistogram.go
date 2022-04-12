package Histogram

import (
	"strconv"
	"strings"
	"time"
)

type FrequencyHistogram struct {
	BS             *BasicStatistics
	DistinctValues []interface{}
	Buckets        map[interface{}]int
	LastUpdateTime time.Time
	BktSize        int
	Type           string
}
type TotalFrequencyHistogram struct {
	TotalFH map[string]FrequencyHistogram
}

func (FH *FrequencyHistogram) GetEqualCardinality(value interface{}) uint64 {
	Types := strings.Split(FH.Type, " ")
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := strconv.ParseUint(value.(string), 10, 64)
			for i := 0; i < len(FH.DistinctValues); i++ {
				if Value1 == FH.DistinctValues[i].(uint64) {
					return uint64(FH.Buckets[Value1])
				}
			}
			return 0
		} else {
			Value1, _ := strconv.ParseInt(value.(string), 10, 64)
			for i := 0; i < len(FH.DistinctValues); i++ {
				if Value1 == FH.DistinctValues[i].(int64) {
					return uint64(FH.Buckets[Value1])
				}
			}
			return 0
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := strconv.ParseFloat(value.(string), 64)
		for i := 0; i < len(FH.DistinctValues); i++ {
			if Value1 == FH.DistinctValues[i].(float64) {
				return uint64(FH.Buckets[Value1])
			}
		}
		return 0
	} else {
		Value1, _ := value.(string)
		for i := 0; i < len(FH.DistinctValues); i++ {
			if Value1 == FH.DistinctValues[i].(string) {
				return uint64(FH.Buckets[Value1])
			}
		}
		return 0
	}
}
func (FH *FrequencyHistogram) GetLessEqualCardinality(value interface{}) uint64 {
	Types := strings.Split(FH.Type, " ")
	count := 0
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := strconv.ParseUint(value.(string), 10, 64)
			for i := 0; i < len(FH.DistinctValues); i++ {
				if FH.DistinctValues[i].(uint64) <= Value1 {
					count += FH.Buckets[FH.DistinctValues[i]]
					continue
				}
				break
			}
			return uint64(count)
		} else {
			Value1, _ := strconv.ParseInt(value.(string), 10, 64)
			for i := 0; i < len(FH.DistinctValues); i++ {
				if FH.DistinctValues[i].(int64) <= Value1 {
					count += FH.Buckets[FH.DistinctValues[i]]
					continue
				}
				break
			}
			return uint64(count)
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := strconv.ParseFloat(value.(string), 64)
		for i := 0; i < len(FH.DistinctValues); i++ {
			if FH.DistinctValues[i].(float64) <= Value1 {
				count += FH.Buckets[FH.DistinctValues[i]]
				continue
			}
			break
		}
		return uint64(count)
	} else {
		Value1, _ := value.(string)
		for i := 0; i < len(FH.DistinctValues); i++ {
			if FH.DistinctValues[i].(string) <= Value1 {
				count += FH.Buckets[FH.DistinctValues[i]]
				continue
			}
			break
		}
		return uint64(count)
	}
}

func (FH *FrequencyHistogram) GetGreaterEqualCardinality(value interface{}) uint64 {
	Types := strings.Split(FH.Type, " ")
	count := 0
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := strconv.ParseUint(value.(string), 10, 64)
			for i := 0; i < len(FH.DistinctValues); i++ {
				if Value1 > FH.DistinctValues[i].(uint64) {
					continue
				}
				count += FH.Buckets[FH.DistinctValues[i]]
			}
			return uint64(count)
		} else {
			Value1, _ := strconv.ParseInt(value.(string), 10, 64)
			for i := 0; i < len(FH.DistinctValues); i++ {
				if Value1 > FH.DistinctValues[i].(int64) {
					continue
				}
				count += FH.Buckets[FH.DistinctValues[i]]
			}
			return uint64(count)
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := strconv.ParseFloat(value.(string), 64)
		for i := 0; i < len(FH.DistinctValues); i++ {
			if Value1 > FH.DistinctValues[i].(float64) {
				continue
			}
			count += FH.Buckets[FH.DistinctValues[i]]
		}
		return uint64(count)
	} else {
		Value1, _ := value.(string)
		for i := 0; i < len(FH.DistinctValues); i++ {
			if Value1 > FH.DistinctValues[i].(string) {
				continue
			}
			count += FH.Buckets[FH.DistinctValues[i]]
		}
		return uint64(count)
	}
}

func (FH *FrequencyHistogram) ShowHistogram() {

}
