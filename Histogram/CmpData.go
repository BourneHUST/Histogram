package Histogram

type Samples struct {
	DistinctValues []interface{}
	Freq           map[interface{}]int
	SampleRows     uint64
	BS             *BasicStatistics
	Type           string
}

func NewSamples() *Samples {
	ans := new(Samples)
	ans.Freq = make(map[interface{}]int)
	ans.BS = NewBasicStatistics()
	return ans
}

type CmpRows struct {
	Type      string
	Rows      []interface{}
	RowsCount int
}
