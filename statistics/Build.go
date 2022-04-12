package statistics

import (
	"fmt"
	"git.woa.com/woodyuan/woodSE/Histogram"
	"git.woa.com/woodyuan/woodSE/database"
)
import "git.woa.com/woodyuan/woodSE/config"

func BuildHistogram(Connector *database.Connector, BucketSize int, Database, Table, Column string, FrequencyHistograms Histogram.TotalFrequencyHistogram, HybridHistograms Histogram.TotalHybridHistogram) {

	conf := config.NewConfig()

	IndexInfo, err := Connector.ShowIndex(Database, Table)
	if err != nil {
		//log.Println(err.Error())
		return
	}
	IndexRows, err := IndexInfo.GetTableIndexRow(Column)

	if err != nil {
		//log.Println(err.Error())
	} else {
		indexs := IndexInfo.FindIndex(IndexRows.KeyName)
		if (IndexRows.NonUnique == 0 && len(indexs) <= 1) || (IndexRows.KeyName == "PRIMARY" && len(indexs) <= 1) {
			//log.Println("Sampling to build histogram")
			SampleBuild(Connector, BucketSize, Database, Table, Column, FrequencyHistograms, HybridHistograms)
			return
		}
	}

	samplingQuery := fmt.Sprintf("select `%s` from `%s`.`%s` %s", Column, Database, Table, "")
	res, err := Connector.Conn.Query(samplingQuery)
	if err != nil {
		//log.Println(err.Error())
		return
	}

	columnTypes, err := res.ColumnTypes()
	if err != nil {
		//log.Println(err.Error())
		return
	}

	td, err := Connector.ShowColumns(Database, Table)
	if err != nil {
		//	fmt.Println(err.Error())
		return
	}

	TypeCode := database.TypeCheck(columnTypes)
	if TypeCode == -1 {
		//log.Println("Mysql date type ", columnTypes[0].DatabaseTypeName(), " should not build Histogram")
		return
	} else if TypeCode == 0 {
		length := database.EstimateLength(td.MysqlType(Column))
		if length > conf.MaxLength {
			//log.Println("Column Length greater than 64 is not allowed to build histogram")
			return
		}
	} else {
		//log.Printf("Build Histogram for %s.%s.%s", Database, Table, Column)
	}

	length := database.EstimateLength(td.MysqlType(Column))

	k := conf.TopK(length)

	_, err = Connector.Conn.Exec("USE `" + Database + "`")
	if err != nil {
		//log.Println(err.Error())
		return
	}

	l := New(k)

	var Type string
	if database.IsUnsigned(td.MysqlType(Column)) {
		Type = database.SavingType(columnTypes, true)
	} else {
		Type = database.SavingType(columnTypes, false)
	}

	l.Type = Type

	//startTime := time.Now().UnixNano()
	l.Build(res)
	//endTime := time.Now().UnixNano()
	//seconds := float64((endTime - startTime) / 1e6)
	//	fmt.Println("统计NDV,TOP N Freq所用时间：  ", seconds*1.0/1000, "秒")

	if l.BS.NDV <= uint64(conf.MaxBucketSize) {
		BucketSize = int(l.BS.NDV) + 1
	}

	//l.Show()

	if BucketSize < conf.MinBucketSize {
		BucketSize = conf.MinBucketSize
	} else if BucketSize > conf.MaxBucketSize {
		BucketSize = conf.MaxBucketSize
	}

	HistogramType := l.ConfirmHistogramType(BucketSize)
	if HistogramType == "FrequencyHistogram" || HistogramType == "TopNFrequencyHistogram" {
		//	startTime := time.Now().UnixNano()
		His := l.BuildFrequencyHistogram(BucketSize)
		//endTime := time.Now().UnixNano()
		//seconds := float64((endTime - startTime) / 1e6)
		//fmt.Println("建立Frequency直方图所用时间：  ", seconds*1.0/1000, "秒")
		Name := Database + "." + Table + "." + Column
		FrequencyHistograms.TotalFH[Name] = His
	} else {
		//	startTime := time.Now().UnixNano()
		His := l.BuildHybridHistogram(BucketSize, conf.HybridSampleSize)
		//endTime := time.Now().UnixNano()
		//seconds := float64((endTime - startTime) / 1e6)
		//	fmt.Println("建立Hybrid直方图所用时间：  ", seconds*1.0/1000, "秒")
		Name := Database + "." + Table + "." + Column
		HybridHistograms.TotalHy[Name] = His
	}

}
func SampleBuild(Connector *database.Connector, BucketSize int, Database, Table, Column string, FrequencyHistograms Histogram.TotalFrequencyHistogram, HybridHistograms Histogram.TotalHybridHistogram) {
	conf := config.NewConfig()
	_, err := Connector.Conn.Exec("USE `" + Database + "`")
	if err != nil {
		//log.Println(err.Error())
		return
	}

	res, TableRows, err := Connector.SamplingData(Database, Table, Column, conf.HybridSampleSize)
	if err != nil {
		//log.Println(err.Error())
		return
	}

	columnTypes, err := res.ColumnTypes()
	if err != nil {
		//log.Println(err.Error())
		return
	}

	td, err := Connector.ShowColumns(Database, Table)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	TypeCode := database.TypeCheck(columnTypes)
	if TypeCode == -1 {
		//log.Println("Mysql date type ", columnTypes[0].DatabaseTypeName(), " should not build Histogram")
		return
	} else if TypeCode == 0 {
		length := database.EstimateLength(td.MysqlType(Column))
		if length > conf.MaxLength {
			//log.Println("Column Length greater than 64 is not allowed to build histogram")
			return
		}
	} else {
		//log.Printf("Build Histogram for %s.%s.%s", Database, Table, Column)
	}

	var Type string
	if database.IsUnsigned(td.MysqlType(Column)) {
		Type = database.SavingType(columnTypes, true)
	} else {
		Type = database.SavingType(columnTypes, false)
	}

	//startTime := time.Now().UnixNano()
	DoSamples(res, Type, TableRows, BucketSize, Database, Table, Column, FrequencyHistograms, HybridHistograms)
	//endTime := time.Now().UnixNano()
	//	seconds := float64((endTime - startTime) / 1e6)
	//fmt.Println("采样10w行数据建立直方图所用时间：  ", seconds*1.0/1000, "秒")

}
