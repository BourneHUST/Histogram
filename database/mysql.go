package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type Connector struct {
	Addr string
	User string
	Pass string
	Conn *sql.DB
}

// QueryResult 数据库查询返回值
type QueryResult struct {
	Rows      *sql.Rows
	Error     error
	Warning   *sql.Rows
	QueryCost float64
}

// TableDesc show columns from rental;
type TableDesc struct {
	Name       string
	DescValues []TableDescValue
}

// TableDescValue 含有每一列的属性
type TableDescValue struct {
	Field      string // 列名
	Type       string // 数据类型
	Collation  []byte // 字符集
	Null       string // 是否有NULL（NO、YES）
	Key        string // 键类型
	Default    []byte // 默认值
	Extra      string // 其他
	Privileges string // 权限
	Comment    string // 备注
}

// NewTableDesc 初始化一个*TableDesc
func NewTableDesc(tableName string) *TableDesc {
	return &TableDesc{
		Name:       tableName,
		DescValues: make([]TableDescValue, 0),
	}
}

// ShowColumns 获取 DB 中所有的 columns
func (db *Connector) ShowColumns(Database, tableName string) (*TableDesc, error) {
	tbDesc := NewTableDesc(tableName)

	// 执行 show create table
	res, err := db.Conn.Query(fmt.Sprintf("show full columns from `%s`.`%s`", Database, tableName))
	if err != nil {
		return nil, err
	}

	// columns info
	tc := TableDescValue{}
	columnFields := make([]interface{}, 0)
	fields := map[string]interface{}{
		"Field":      &tc.Field,
		"Type":       &tc.Type,
		"Collation":  &tc.Collation,
		"Null":       &tc.Null,
		"Key":        &tc.Key,
		"Default":    &tc.Default,
		"Extra":      &tc.Extra,
		"Privileges": &tc.Privileges,
		"Comment":    &tc.Comment,
	}
	cols, err := res.Columns()
	if err != nil {
		log.Panicln(err.Error())
	}
	var colByPass []byte
	for _, col := range cols {
		if _, ok := fields[col]; ok {
			columnFields = append(columnFields, fields[col])
		} else {
			columnFields = append(columnFields, &colByPass)
		}
	}
	// 获取值
	for res.Next() {
		err := res.Scan(columnFields...)
		if err != nil {
			log.Println(err.Error())
		}
		tbDesc.DescValues = append(tbDesc.DescValues, tc)
	}
	res.Close()
	return tbDesc, err
}
func (td *TableDesc) MysqlType(column string) string {
	for i := 0; i < len(td.DescValues); i++ {
		if td.DescValues[i].Field == column {
			return td.DescValues[i].Type
		}
	}
	return ""
}
func (td *TableDesc) Collation(column string) string {
	for i := 0; i < len(td.DescValues); i++ {
		if td.DescValues[i].Field == column {
			return string(td.DescValues[i].Collation)
		}
	}
	return ""
}
func CaseSensitive(Collation string) bool {
	parts := strings.Split(Collation, "_")
	for i := 0; i < len(parts); i++ {
		if parts[i] == "bin" || parts[i] == "cs" {
			return true
		}
	}
	return false
}

func IsUnsigned(ColumnType string) bool {
	context := strings.Split(ColumnType, " ")
	for i := 0; i < len(context); i++ {
		if strings.ToLower(context[i]) == "unsigned" {
			return true
		}
	}
	return false
}

func IsStringType(Type string) bool {
	Type1 := "char"
	Type2 := "varchar"
	i := 0
	Type = strings.ToLower(Type)
	for i = 0; i < 4 && i < len(Type); i++ {
		if Type1[i] == Type[i] {
			continue
		} else {
			break
		}
	}
	if i != 4 {
		for i = 0; i < 7 && i < len(Type); i++ {
			if Type2[i] == Type[i] {
				continue
			} else {
				break
			}
		}
		if i != 7 {
			return false
		} else {
			return true
		}
	} else {
		return true
	}

}
func EstimateLength(Type string) int {
	context := strings.Split(Type, " ")
	for i := 0; i < len(context); i++ {
		var str string
		if IsStringType(context[i]) {
			index := 0
			for index = 0; index < len(context[i]); index++ {
				if context[i][index] == '(' {
					str = context[i][index+1 : len(context[i])-1]
					break
				}
			}
			if len(str) == 0 {
				return 1
			}
			ans, _ := strconv.Atoi(str)
			return ans
		}
	}
	return 1
}
func TypeCheck(ColumnTypes []*sql.ColumnType) int8 {
	switch ColumnTypes[0].DatabaseTypeName() {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
		"DECIMAL", "DATE", "TIME", "YEAR", "DATETIME", "TIMESTAMP", "ENUM", "SET":
		return 1
	case "BlOB", "TEXT", "JSON":
		return -1
	case "CHAR", "VARCHAR", "NVARCHAR":
		return 0
	default:
		return -1
	}
}
func GetTypeSize(ColumnTypes []*sql.ColumnType) int {
	switch ColumnTypes[0].DatabaseTypeName() {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "YEAR":
		return 8
	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		return 20
	default:
		return 64
	}
}
func ConfirmType(ColumnTypes []*sql.ColumnType) string {
	switch ColumnTypes[0].DatabaseTypeName() {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR":
		return "INT"
	case "DATE", "TIME", "DATETIME", "TIMESTAMP", "FLOAT", "DOUBLE", "DECIMAL", "CHAR", "VARCHAR":
		return "STRING"
	default:
		return "STRING"
	}
}
func SavingType(ColumnTypes []*sql.ColumnType, isUnsigned bool) string {

	var ans string
	switch ColumnTypes[0].DatabaseTypeName() {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR":
		if isUnsigned {
			ans = "INTEGER UNSIGNED"
		} else {
			ans = "INTEGER SIGNED"
		}
	case "CHAR", "VARCHAR", "TIME", "NVARCHAR":
		ans = "STRING"
	case "DATE", "DATETIME", "TIMESTAMP":
		ans = "TIME"
	case "DECIMAL":
		ans = "DECIMAL"
	case "FLOAT", "DOUBLE":
		ans = "FLOAT"
	default:
		return "STRING"
	}
	return ans
}

// NullString null able string
func NullString(buf []byte) string {
	if buf == nil {
		return "NULL"
	}
	return string(buf)
}

func NullUint(buf []byte) uint64 {
	if buf == nil {
		return 0
	}
	ans, _ := strconv.ParseUint(NullString(buf), 10, 64)
	return ans
}

// NullFloat null able float
func NullFloat(buf []byte) float64 {
	if buf == nil {
		return 0
	}
	f, _ := strconv.ParseFloat(string(buf), 64)
	return f
}

// NullInt null able int
func NullInt(buf []byte) int64 {
	if buf == nil {
		return 0
	}
	i, _ := strconv.ParseInt(string(buf), 10, 64)
	return i
}

type TableIndexInfo struct {
	TableName string
	Rows      []TableIndexRow
}

// TableIndexRow 用以存放show index 之后获取的每一条 index 信息
type TableIndexRow struct {
	Table        string // 表名
	NonUnique    int    // 0：unique key，1：not unique
	KeyName      string // index的名称，如果是主键则为 "PRIMARY"
	SeqInIndex   int    // 该列在索引中的位置。计数从 1 开始
	ColumnName   string // 列名
	Collation    string // A or Null
	Cardinality  int    // 索引中唯一值的数量，"ANALYZE TABLE" 可更新该值
	SubPart      int    // 索引前缀字节数
	Packed       int
	Null         string // 表示该列是否可以为空，如果可以为 'YES'，反之''
	IndexType    string // BTREE, FULLTEXT, HASH, RTREE
	Comment      string
	IndexComment string
	Visible      string
	Expression   []byte
}

// NewTableIndexInfo 构造 TableIndexInfo
func NewTableIndexInfo(tableName string) *TableIndexInfo {
	return &TableIndexInfo{
		TableName: tableName,
		Rows:      make([]TableIndexRow, 0),
	}
}

// ShowIndex show Index
func (db *Connector) ShowIndex(Database, tableName string) (*TableIndexInfo, error) {
	tbIndex := NewTableIndexInfo(tableName)

	if Database == "" || tableName == "" {
		return nil, fmt.Errorf("database('%s') or table('%s') name should not empty", Database, tableName)
	}

	// 执行 show create table
	res, err := db.Conn.Query(fmt.Sprintf("show index from `%s`.`%s`", Database, tableName))
	if err != nil {
		return nil, err
	}

	// columns info
	ti := TableIndexRow{}
	indexFields := make([]interface{}, 0)
	fields := map[string]interface{}{
		"Table":         &ti.Table,
		"Non_unique":    &ti.NonUnique,
		"Key_name":      &ti.KeyName,
		"Seq_in_index":  &ti.SeqInIndex,
		"Column_name":   &ti.ColumnName,
		"Collation":     &ti.Collation,
		"Cardinality":   &ti.Cardinality,
		"Sub_part":      &ti.SubPart,
		"Packed":        &ti.Packed,
		"Null":          &ti.Null,
		"Index_type":    &ti.IndexType,
		"Comment":       &ti.Comment,
		"Index_comment": &ti.IndexComment,
		"Visible":       &ti.Visible,
		"Expression":    &ti.Expression,
	}
	cols, err := res.Columns()

	var colByPass []byte
	for _, col := range cols {
		if _, ok := fields[col]; ok {
			indexFields = append(indexFields, fields[col])
		} else {
			indexFields = append(indexFields, &colByPass)
		}
	}
	// 获取值
	for res.Next() {
		err := res.Scan(indexFields...)
		if err != nil {
			//log.Println(err.Error())
		}
		tbIndex.Rows = append(tbIndex.Rows, ti)
	}
	res.Close()
	return tbIndex, err
}

func (ti *TableIndexInfo) GetTableIndexRow(Column string) (TableIndexRow, error) {
	for i := 0; i < len(ti.Rows); i++ {
		if ti.Rows[i].ColumnName == Column {
			return ti.Rows[i], nil
		}
	}
	return TableIndexRow{}, fmt.Errorf("Column %s is not index", Column)
}

// TableStatInfo 用以保存 show table status 之后获取的table信息
type TableStatInfo struct {
	Name string
	Rows []tableStatusRow
}

// tableStatusRow 用于 show table status value
// use []byte instead of string, because []byte allow to be null, string not
type tableStatusRow struct {
	Name         string // 表名
	Engine       []byte // 该表使用的存储引擎
	Version      []byte // 该表的 .frm 文件版本号
	RowFormat    []byte // 该表使用的行存储格式
	Rows         []byte // 表行数, InnoDB 引擎中为预估值，甚至可能会有40%~50%的数值偏差
	AvgRowLength []byte // 平均行长度

	// MyISAM: Data_length 为数据文件的大小，单位为 bytes
	// InnoDB: Data_length 为聚簇索引分配的近似内存量，单位为 bytes, 计算方式为聚簇索引数量乘以 InnoDB 页面大小
	// 其他不同的存储引擎中该值的意义可能不尽相同
	DataLength []byte

	// MyISAM: Max_data_length 为数据文件长度的最大值。这是在给定使用的数据指针大小的情况下，可以存储在表中的数据的最大字节数
	// InnoDB: 未使用
	// 其他不同的存储引擎中该值的意义可能不尽相同
	MaxDataLength []byte

	// MyISAM: Index_length 为 index 文件的大小，单位为 bytes
	// InnoDB: Index_length 为非聚簇索引分配的近似内存量，单位为 bytes，计算方式为非聚簇索引数量乘以 InnoDB 页面大小
	// 其他不同的存储引擎中该值的意义可能不尽相同
	IndexLength []byte

	DataFree      []byte // 已分配但未使用的字节数
	AutoIncrement []byte // 下一个自增值
	CreateTime    []byte // 创建时间
	UpdateTime    []byte // 最近一次更新时间，该值不准确
	CheckTime     []byte // 上次检查时间
	Collation     []byte // 字符集及排序规则信息
	Checksum      []byte // 校验和
	CreateOptions []byte // 创建表的时候的时候一切其他属性
	Comment       []byte // 注释
}

func newTableStat(tableName string) *TableStatInfo {
	return &TableStatInfo{
		Name: tableName,
		Rows: make([]tableStatusRow, 0),
	}
}

// ShowTableStatus 执行 show table status
func (db *Connector) ShowTableStatus(tableName string) (*TableStatInfo, error) {
	// 初始化struct
	tbStatus := newTableStat(tableName)

	// 执行 show table status
	res, err := db.Conn.Query(fmt.Sprintf("show table status where name = '%s'", tbStatus.Name))
	if err != nil {
		return tbStatus, err
	}

	// columns info
	ts := tableStatusRow{}
	statusFields := make([]interface{}, 0)
	fields := map[string]interface{}{
		"Name":            &ts.Name,
		"Engine":          &ts.Engine,
		"Version":         &ts.Version,
		"Row_format":      &ts.RowFormat,
		"Rows":            &ts.Rows,
		"Avg_row_length":  &ts.AvgRowLength,
		"Data_length":     &ts.DataLength,
		"Max_data_length": &ts.MaxDataLength,
		"Index_length":    &ts.IndexLength,
		"Data_free":       &ts.DataFree,
		"Auto_increment":  &ts.AutoIncrement,
		"Create_time":     &ts.CreateTime,
		"Update_time":     &ts.UpdateTime,
		"Check_time":      &ts.CheckTime,
		"Collation":       &ts.Collation,
		"Checksum":        &ts.Checksum,
		"Create_options":  &ts.CreateOptions,
		"Comment":         &ts.Comment,
	}
	cols, err := res.Columns()
	if err != nil {
		return nil, err
	}
	var colByPass []byte
	for _, col := range cols {
		if _, ok := fields[col]; ok {
			statusFields = append(statusFields, fields[col])
		} else {
			statusFields = append(statusFields, &colByPass)
		}
	}
	// 获取值
	for res.Next() {
		err := res.Scan(statusFields...)
		if err != nil {
			fmt.Printf(err.Error())

		}
		tbStatus.Rows = append(tbStatus.Rows, ts)
	}
	res.Close()
	return tbStatus, err
}
func (tbIndex *TableIndexInfo) FindIndex(value string) []TableIndexRow {
	var result []TableIndexRow
	if tbIndex == nil {
		return result
	}

	value = strings.ToLower(value)

	for _, index := range tbIndex.Rows {
		if strings.ToLower(index.KeyName) == value {
			result = append(result, index)
		}
	}
	return result

}
