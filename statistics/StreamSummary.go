package statistics

import (
	"github.com/BourneHUST/Histogram/Histogram"
	"github.com/BourneHUST/Histogram/hyperloglog"
)

type TopNBucket struct {
	Freq      int
	Next      *TopNBucket
	Pre       *TopNBucket
	child     *synopsis
	childTail *synopsis
}
type synopsis struct {
	ID     interface{}
	Error  int
	Father *TopNBucket
	Next   *synopsis
	Pre    *synopsis
}
type TopNList struct {
	head       *TopNBucket
	m          map[interface{}]*synopsis
	BS         *Histogram.BasicStatistics
	size       int
	Type       string
	NDVCounter *hyperloglog.HyperLogLogPlus
}

func NewBucket(Freq int) *TopNBucket {
	ans := new(TopNBucket)
	ans.Freq = Freq
	return ans
}
func New(K int) TopNList {
	l := TopNList{}
	l.head = new(TopNBucket)
	l.m = make(map[interface{}]*synopsis)
	l.BS = Histogram.NewBasicStatistics()
	l.size = K
	l.NDVCounter, _ = NDVcounter()
	return l
}
func (l *TopNList) GetFreq(ID interface{}) int {
	return l.m[ID].Father.Freq
}

func (l *TopNList) GetError(ID interface{}) int {
	return l.m[ID].Error
}
