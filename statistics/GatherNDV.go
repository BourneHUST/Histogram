package statistics

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/BourneHUST/Histogram/config"
	"github.com/BourneHUST/Histogram/database"
)

func GatherNDV(Connector *database.Connector, Database, Table, Column string) (error, uint64) {
	conf := config.NewConfig()
	_, err := Connector.Conn.Exec("USE `" + Database + "`")
	if err != nil {
		return err, 0
	}

	var tableRows uint64
	tableStatus, err := Connector.ShowTableStatus(Table)
	if err != nil {
		return err, 0
	}
	if len(tableStatus.Rows) == 0 {
		return nil, 0
	} else {
		tableRows, _ = strconv.ParseUint(string(tableStatus.Rows[0].Rows), 10, 64)
		if tableRows == 0 {
			return nil, 0
		}
	}

	SampleSize := conf.NDVSampleSize
	factor := float64(SampleSize) / float64(tableRows)

	where := fmt.Sprintf("where RAND() <= %f LIMIT %d", factor, SampleSize)
	if factor >= 1 || conf.AutoSampling == 0 {
		where = ""
	}
	Query := fmt.Sprintf("select `%s` from `%s`.`%s` %s", Column, Database, Table, where)
	res, err := Connector.Conn.Query(Query)
	if err != nil {
		return err, 0
	}

	columnTypes, err := res.ColumnTypes()
	if err != nil {
		return err, 0
	}

	td, err := Connector.ShowColumns(Database, Table)
	if err != nil {
		return err, 0
	}

	TypeCode := database.TypeCheck(columnTypes)
	if TypeCode == -1 {
		log.Println("Mysql date type ", columnTypes[0].DatabaseTypeName(), " should not gather NDV")
		return nil, 0
	} else {
		log.Printf("Gather NDV for %s.%s.%s", Database, Table, Column)
	}

	var Type string
	if database.IsUnsigned(td.MysqlType(Column)) {
		Type = database.SavingType(columnTypes, true)
	} else {
		Type = database.SavingType(columnTypes, false)
	}

	NDVCounter, errr := NDVcounter()
	if errr != nil {
		return errr, 0
	}

	var value []byte
	for res.Next() {
		err := res.Scan(&value)
		if err != nil {
			log.Println(err.Error())
		}
		if len(value) == 0 {
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
			NDVCounter.Add(integerHash(database.NullUint(value)))
		} else if Types[0] == "FLOAT" {
			TypeValue = database.NullFloat(value)
			NDVCounter.Add(StringHash(database.NullString(value)))
		} else {
			TypeValue = database.NullString(value)
			NDVCounter.Add(StringHash(TypeValue.(string)))
		}
	}
	return nil, NDVCounter.Count()
}
