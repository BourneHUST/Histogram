package database

import (
	"database/sql"
	"fmt"
	//"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	//"strconv"
	//	"time"
)

func (db *Connector) SamplingData(Database, Table, Column string, SampleSize int) (*sql.Rows, uint64, error) {
	//var err error

	tableStatus, err := db.ShowTableStatus(Table)
	if err != nil {
		return nil, 0, err
	}
	if len(tableStatus.Rows) == 0 {
		return nil, 0, fmt.Errorf("Table %s has no rows", Table)
	}

	tableRows, err := strconv.ParseUint(string(tableStatus.Rows[0].Rows), 10, 64)
	if tableRows == 0 || err != nil {
		return nil, 0, fmt.Errorf("Table %s has no rows", Table)
	}

	factor := float64(SampleSize) / float64(tableRows)

	where := fmt.Sprintf("where RAND() <= %f LIMIT %d", factor, SampleSize)
	if factor >= 1 {
		where = ""
	}
	samplingQuery := fmt.Sprintf("select `%s` from `%s`.`%s` %s", Column, Database, Table, where)

	res, err := db.Conn.Query(samplingQuery)
	if err != nil {
		return nil, 0, err
	}

	return res, tableRows, nil

}
