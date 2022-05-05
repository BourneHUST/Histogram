package main

import (
	"database/sql"
	"log"

	"github.com/BourneHUST/Histogram/API"
	"github.com/BourneHUST/Histogram/config"
	"github.com/BourneHUST/Histogram/database"
)

// NewConnector 创建新连接
func NewConnector(dsn *config.Dsn) (*database.Connector, error) {

	/*go func() {
		http.ListenAndServe("0.0.0.0:8080", nil)
	}()*/

	conn, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, err
	}
	connector := &database.Connector{
		Addr: dsn.Addr,
		User: dsn.User,
		Pass: dsn.Password,
		Conn: conn,
	}
	return connector, err
}

func main() {
	//f, _ := os.OpenFile("fmt.log", os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND,0755)

	//os.Stdout = f

	//os.Stderr = f

	dsn := config.Dsn{}
	dsn.Password = "1999528"
	dsn.User = "root"
	dsn.Addr = "127.0.0.1"
	dsn.Host = "3306"
	Connector, err := NewConnector(&dsn)
	if err != nil {
		log.Panicln(err.Error())
	}
	S := API.NewStatistics(Connector)
	S.GatherHistogram(2000, "employees", "salaries", "salary")
	//fmt.Println(S.GetNDV("airportdb", "booking", "flight_id"))*/

	/*dsn := config.Dsn{}
	dsn.Password = "123456"
	dsn.User = "root"
	dsn.Addr = "10.249.50.200"
	dsn.Host = "15246"
	Connector, err := NewConnector(&dsn)
	if err != nil {
		log.Panicln(err.Error())
	}
	S := API.NewStatistics(Connector)
	S.GatherHistogram(1000,"TPCC", "item", "i_id")*/

	/*tables := []string{"customer", "district", "history", "item", "new_orders", "order_line", "orders", "stock", "warehouse"}

	_, err = Connector.Conn.Exec("USE `" + "TPCC" + "`")
	if err != nil {
		log.Println(err.Error())
		return
	}

	columns := make(map[string][]string)
	for i := 0; i < len(tables); i++ { //获取所有表的所有列名
		Query := fmt.Sprintf("select * from `%s`.`%s` limit 1", "TPCC", tables[i])
		res, err := Connector.Conn.Query(Query)
		if err != nil {
			log.Println(err.Error())
			return
		}

		columnTypes, err := res.ColumnTypes()
		if err != nil {
			log.Println(err.Error())
			return
		}
		for j := 0; j < len(columnTypes); j++ {
			columns[tables[i]] = append(columns[tables[i]], columnTypes[j].Name())
		}
	}

	ndvs := make(map[string]int)
	for i := 0; i < len(tables); i++ {
		for j := 0; j < len(columns[tables[i]]); j++ {
			Query := fmt.Sprintf("select count(distinct(`%s`)) from `%s`.`%s` ", columns[tables[i]][j], "TPCC", tables[i])
			res, err := Connector.Conn.Query(Query)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			var ndv int
			for res.Next() {
				err = res.Scan(&ndv)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
			}
			ndvs[tables[i]+"."+columns[tables[i]][j]] = ndv
			fmt.Printf("%s 的ndv是：%d \n", tables[i]+"."+columns[tables[i]][j], ndv)
		}

	}*/

}
