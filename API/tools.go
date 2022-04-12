package API

import (
	"fmt"
	"github.com/BourneHUST/Histogram/Histogram"
	"github.com/BourneHUST/Histogram/config"
	"github.com/BourneHUST/Histogram/database"
	"github.com/BourneHUST/Histogram/statistics"
	"log"
	"strings"
)

type Statistics struct {
	Connector           *database.Connector
	NDV                 map[string]uint64
	FrequencyHistograms Histogram.TotalFrequencyHistogram
	HybridHistograms    Histogram.TotalHybridHistogram
}

func NewStatistics(Connector *database.Connector) *Statistics {
	BS := new(Statistics)

	BS.Connector = Connector

	BS.NDV = make(map[string]uint64)
	BS.HybridHistograms.TotalHy = make(map[string]Histogram.HybridHistogram)
	BS.FrequencyHistograms.TotalFH = make(map[string]Histogram.FrequencyHistogram)

	return BS
}
func (BS *Statistics) GatherNDV(Database, Table, Column string) {
	err, NDV := statistics.GatherNDV(BS.Connector, Database, Table, Column)
	if err != nil {
		log.Println(err.Error())
		return
	} else {
		BS.NDV[Database+"."+Table+"."+Column] = NDV
	}
}

func (BS *Statistics) GatherHistogram(BucketSize int, Database, Table, Column string) {
	statistics.BuildHistogram(BS.Connector, BucketSize, Database, Table, Column, BS.FrequencyHistograms, BS.HybridHistograms)
}
func (BS *Statistics) GetNDV(Database, Table, Column string) uint64 {

	col := Database + "." + Table + "." + Column

	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]

	if ok1 {
		return his1.BS.NDV
	} else if ok2 {
		return his2.BS.NDV
	}
	ndv, ok := BS.NDV[col]
	if ok {
		return ndv
	} else {
		//startTime := time.Now().UnixNano()
		BS.GatherNDV(Database, Table, Column)
		//endTime := time.Now().UnixNano()
		//seconds := float64((endTime - startTime) / 1e6)
		//	fmt.Println("NDV时间:", seconds*1.0/1000, "秒")
		ans, _ := BS.NDV[col]
		return ans
	}

}
func (BS *Statistics) Equal(Database, Table, Column, value string) uint64 {
	conf := config.NewConfig()

	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]
	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}
	if ok1 {
		return his1.GetEqualCardinality(value)
	} else {
		return his2.GetEqualCardinality(value)
	}
}
func (BS *Statistics) Range(Database, Table, Column, value string) uint64 {

	conf := config.NewConfig()

	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]

	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}
	if ok1 {
		l := value[0]
		r := value[len(value)-1]

		lr := strings.Split(value[1:len(value)-1], ",")
		if lr[0] == "~" && lr[1] == "~" {
			return his1.BS.TotalRows
		} else if lr[0] == "~" {
			if r == ']' {
				return his1.GetLessEqualCardinality(lr[1])
			} else {
				return his1.GetLessEqualCardinality(lr[1]) - his1.GetEqualCardinality(lr[1])
			}
		} else if lr[1] == "~" {
			if l == '[' {
				return his1.GetGreaterEqualCardinality(lr[0])
			} else {
				return his1.GetGreaterEqualCardinality(lr[0]) - his1.GetEqualCardinality(lr[0])
			}
		} else {
			ans := his1.GetLessEqualCardinality(lr[1]) - his1.GetLessEqualCardinality(lr[0]) + his1.GetEqualCardinality(lr[0])
			if l == '(' {
				ans -= his1.GetEqualCardinality(lr[0])
			}
			if r == ')' {
				ans -= his1.GetEqualCardinality(lr[1])
			}
			return ans
		}

	} else {
		l := value[0]
		r := value[len(value)-1]

		lr := strings.Split(value[1:len(value)-1], ",")
		if lr[0] == "~" && lr[1] == "~" {
			return his1.BS.TotalRows
		} else if lr[0] == "~" {
			if r == ']' {
				return his2.GetLessEqualCardinality(lr[1])
			} else {
				return his2.GetLessEqualCardinality(lr[1]) - his2.GetEqualCardinality(lr[1])
			}
		} else if lr[1] == "~" {
			if l == '[' {
				return his2.GetGreaterEqualCardinality(lr[0])
			} else {
				return his2.GetGreaterEqualCardinality(lr[0]) - his2.GetEqualCardinality(lr[0])
			}
		} else {
			ans := his2.GetLessEqualCardinality(lr[1]) - his2.GetLessEqualCardinality(lr[0]) + his2.GetEqualCardinality(lr[0])
			if l == '(' {
				ans -= his2.GetEqualCardinality(lr[0])
			}
			if r == ')' {
				ans -= his2.GetEqualCardinality(lr[1])
			}
			return ans
		}
	}
}
func (BS *Statistics) MAX(Database, Table, Column string) interface{} {
	conf := config.NewConfig()
	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]
	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		fmt.Println("xxx")
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}
	if ok1 {
		return his1.BS.GetMAX()

	} else {
		return his2.BS.GetMAX()
	}

}
func (BS *Statistics) MIN(Database, Table, Column string) interface{} {
	conf := config.NewConfig()
	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]

	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}

	if ok1 {
		return his1.BS.GetMIN()

	} else {
		return his2.BS.GetMIN()
	}
}
func (BS *Statistics) NULL(Database, Table, Column string) int {
	conf := config.NewConfig()
	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]

	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}

	if ok1 {
		return his1.BS.GetNULL()

	} else {
		return his2.BS.GetNULL()
	}
}
func (BS *Statistics) AverageLength(Database, Table, Column string) uint64 {
	conf := config.NewConfig()
	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]

	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}

	if ok1 {
		return his1.BS.GetAverageLength()

	} else {
		return his2.BS.GetAverageLength()
	}
}
func (BS *Statistics) ROWS(Database, Table, Column string) uint64 {
	conf := config.NewConfig()
	col := Database + "." + Table + "." + Column
	his1, ok1 := BS.FrequencyHistograms.TotalFH[col]
	his2, ok2 := BS.HybridHistograms.TotalHy[col]

	if !ok1 && !ok2 {
		BS.GatherHistogram(conf.MaxBucketSize, Database, Table, Column)
		his1, ok1 = BS.FrequencyHistograms.TotalFH[col]
		his2, ok2 = BS.HybridHistograms.TotalHy[col]
	}

	if ok1 {
		return his1.BS.GetRows()

	} else {
		return his2.BS.GetRows()
	}
}
