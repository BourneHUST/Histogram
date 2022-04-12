package Histogram

import "strings"

func DigitLess(value1 string, value2 string) bool {
	if value1[0] == '-' && value2[0] == '-' {
		content1 := value1[1:]
		content2 := value1[1:]

		contexts1 := strings.Split(content1, ".")
		contexts2 := strings.Split(content2, ".")
		if len(contexts1[0]) > len(contexts2[0]) {
			return true
		} else if len(contexts1[0]) < len(contexts2[0]) {
			return false
		} else {
			if contexts1[0] > contexts2[0] {
				return true
			} else if contexts1[0] < contexts2[0] {
				return false
			} else {
				if len(contexts1) > len(contexts2) {
					return true
				} else if len(contexts1) < len(contexts2) {
					return false
				} else {
					return contexts1[1] > contexts2[1]
				}
			}
		}
	} else if value1[0] == '-' {
		return true
	} else if value2[0] == '-' {
		return false
	} else {

		contexts1 := strings.Split(value1, ".")
		contexts2 := strings.Split(value2, ".")

		if len(contexts1[0]) > len(contexts2[0]) {
			return false
		} else if len(contexts1[0]) < len(contexts2[0]) {
			return true
		} else {
			if contexts1[0] > contexts2[0] {
				return false
			} else if contexts1[0] < contexts2[0] {
				return true
			} else {
				if len(contexts1) > len(contexts2) {
					return false
				} else if len(contexts1) < len(contexts2) {
					return true
				} else {
					return contexts1[1] < contexts2[1]
				}
			}
		}
	}
}
func (His FrequencyHistogram) Len() int { return len(His.Buckets) }
func (His FrequencyHistogram) Less(i, j int) bool {
	Types := strings.Split(His.Type, " ")
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := His.DistinctValues[i].(uint64)
			Value2, _ := His.DistinctValues[j].(uint64)
			return Value1 < Value2
		} else {
			Value1, _ := His.DistinctValues[i].(int64)
			Value2, _ := His.DistinctValues[j].(int64)
			return Value1 < Value2
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := His.DistinctValues[i].(float64)
		Value2, _ := His.DistinctValues[j].(float64)
		return Value1 < Value2
	} else {
		Value1, _ := His.DistinctValues[i].(string)
		Value2, _ := His.DistinctValues[j].(string)
		if Types[0] == "DECIMAL" {
			return DigitLess(Value1, Value2)
		}
		return Value1 < Value2
	}
}
func (His FrequencyHistogram) Swap(i, j int) {
	His.DistinctValues[i], His.DistinctValues[j] = His.DistinctValues[j], His.DistinctValues[i]
}

func (rows CmpRows) Len() int { return rows.RowsCount }
func (rows CmpRows) Less(i, j int) bool {
	Types := strings.Split(rows.Type, " ")
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := rows.Rows[i].(uint64)
			Value2, _ := rows.Rows[j].(uint64)
			return Value1 < Value2
		} else {
			Value1, _ := rows.Rows[i].(int64)
			Value2, _ := rows.Rows[j].(int64)
			return Value1 < Value2
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := rows.Rows[i].(float64)
		Value2, _ := rows.Rows[j].(float64)
		return Value1 < Value2
	} else {
		Value1, _ := rows.Rows[i].(string)
		Value2, _ := rows.Rows[j].(string)
		if Types[0] == "DECIMAL" {
			return DigitLess(Value1, Value2)
		}
		return Value1 < Value2
	}
}
func (rows CmpRows) Swap(i, j int) {
	rows.Rows[i], rows.Rows[j] = rows.Rows[j], rows.Rows[i]
}

func (rows Samples) Len() int { return len(rows.DistinctValues) }
func (rows Samples) Less(i, j int) bool {
	Types := strings.Split(rows.Type, " ")
	if Types[0] == "INTEGER" {
		if Types[1] == "UNSIGNED" {
			Value1, _ := rows.DistinctValues[i].(uint64)
			Value2, _ := rows.DistinctValues[j].(uint64)
			return Value1 < Value2
		} else {
			Value1, _ := rows.DistinctValues[i].(int64)
			Value2, _ := rows.DistinctValues[j].(int64)
			return Value1 < Value2
		}
	} else if Types[0] == "FLOAT" {
		Value1, _ := rows.DistinctValues[i].(float64)
		Value2, _ := rows.DistinctValues[j].(float64)
		return Value1 < Value2
	} else {
		Value1, _ := rows.DistinctValues[i].(string)
		Value2, _ := rows.DistinctValues[j].(string)
		if Types[0] == "DECIMAL" {
			return DigitLess(Value1, Value2)
		}
		return Value1 < Value2
	}
}
func (rows Samples) Swap(i, j int) {
	rows.DistinctValues[i], rows.DistinctValues[j] = rows.DistinctValues[j], rows.DistinctValues[i]
}
