package statistics

import (
	"database/sql"
	"log"
	"sort"
	"strings"

	"github.com/BourneHUST/Histogram/Histogram"
	"github.com/BourneHUST/Histogram/config"
	"github.com/BourneHUST/Histogram/database"
)

func DoSamples(res *sql.Rows, Type string, TableRows uint64, BucketSize int, Database, Table, Column string, FrequencyHistograms Histogram.TotalFrequencyHistogram, HybridHistograms Histogram.TotalHybridHistogram) {

	conf := config.NewConfig()

	S := Histogram.NewSamples()
	S.Type = Type
	var value []byte
	i := 0

	for res.Next() && i < 1 { //init BS
		err := res.Scan(&value)
		if err != nil {
			//log.Println(err.Error())
			return
		}
		S.SampleRows += 1
		if len(value) == 0 {
			S.BS.NumOfNULL += 1

			continue
		}

		Types := strings.Split(Type, " ")

		var TypeValue interface{}
		if Types[0] == "INTEGER" {
			if Types[1] == "UNSIGNED" {
				TypeValue = database.NullUint(value)
			} else {
				TypeValue = database.NullInt(value)
			}

		} else if Types[0] == "FLOAT" {
			TypeValue = database.NullFloat(value)

		} else {
			TypeValue = database.NullString(value)

		}

		S.BS.MAX = TypeValue
		S.BS.MIN = TypeValue

		if _, ok := S.Freq[TypeValue]; ok {
			S.Freq[TypeValue]++
		} else {
			S.Freq[TypeValue] = 1
		}
		i++
	}

	for res.Next() {
		err := res.Scan(&value)
		if err != nil {
			log.Printf(err.Error())
			return
		}
		S.SampleRows += 1
		if len(value) == 0 {
			S.BS.NumOfNULL += 1
			continue
		}

		Types := strings.Split(Type, " ")

		var TypeValue interface{}
		if Types[0] == "INTEGER" {
			if Types[1] == "UNSIGNED" {
				TypeValue = database.NullUint(value)
			} else {
				TypeValue = database.NullInt(value)
			}

		} else if Types[0] == "FLOAT" {
			TypeValue = database.NullFloat(value)

		} else {
			TypeValue = database.NullString(value)

		}

		S.BS.Gather(TypeValue, Type)

		if _, ok := S.Freq[TypeValue]; ok {
			S.Freq[TypeValue]++
		} else {
			S.DistinctValues = append(S.DistinctValues, TypeValue)
			S.Freq[TypeValue] = 1
		}
	}

	S.BS.TotalRows = TableRows
	S.BS.NDV = TableRows
	S.BS.AverageLength = S.BS.AverageLength / S.SampleRows
	S.BS.NumOfNULL = (1.0 * S.BS.NumOfNULL / int(S.SampleRows)) * int(TableRows)

	sort.Sort(S)

	if BucketSize < conf.MinBucketSize {
		BucketSize = conf.MinBucketSize
	} else if BucketSize > conf.MaxBucketSize {
		BucketSize = conf.MaxBucketSize
	}

	HistogramType := S.ConfirmHistogramType(BucketSize)
	if HistogramType == "FrequencyHistogram" || HistogramType == "TopNFrequencyHistogram" {
		//startTime := time.Now().UnixNano()
		His := S.BuildFrequencyHistogram(BucketSize)
		//endTime := time.Now().UnixNano()
		//seconds := float64((endTime - startTime) / 1e6)
		//	fmt.Println("建立Frequency直方图所用时间：  ", seconds*1.0/1000, "秒")
		Name := Database + "." + Table + "." + Column
		FrequencyHistograms.TotalFH[Name] = His
	} else {
		//startTime := time.Now().UnixNano()
		His := S.BuildHybridHistogram(BucketSize)
		//endTime := time.Now().UnixNano()
		//	seconds := float64((endTime - startTime) / 1e6)
		//fmt.Println("建立Hybrid直方图所用时间：  ", seconds*1.0/1000, "秒")
		Name := Database + "." + Table + "." + Column
		HybridHistograms.TotalHy[Name] = His
	}
}
