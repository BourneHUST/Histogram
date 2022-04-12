package statistics

import (
	"sort"
	"time"

	"github.com/BourneHUST/Histogram/Histogram"
)

func (l *TopNList) BuildFrequencyHistogram(BucketSize int) Histogram.FrequencyHistogram {
	His := Histogram.FrequencyHistogram{}
	His.Buckets = make(map[interface{}]int)
	His.BS = l.BS
	His.BktSize = BucketSize
	His.Type = l.Type
	His.LastUpdateTime = time.Now()
	count := 0
	temp := l.head.Next
	for temp.Next != nil {
		temp = temp.Next
	}
	for temp != l.head && count < BucketSize {
		ch := temp.child
		for ch != nil && count < BucketSize {
			count++
			His.DistinctValues = append(His.DistinctValues, ch.ID)
			His.Buckets[ch.ID] = ch.Father.Freq - ch.Error
			ch = ch.Next
		}
		temp = temp.Pre
	}
	sort.Sort(His)
	return His
}
func (l *TopNList) BuildHybridHistogram(BucketSize int, SampleSize int) Histogram.HybridHistogram {
	His := Histogram.HybridHistogram{}
	His.BS = l.BS
	His.LastUpdateTime = time.Now()
	His.Type = l.Type

	TypeRows := Histogram.CmpRows{}
	TypeRows.Type = l.Type

	TotalRows := 0
	ndv := 0
	temp := l.head.Next

	for temp.Next != nil {
		temp = temp.Next
	}
	for temp != l.head && ndv < SampleSize {
		ch := temp.child
		for ch != nil && ndv < SampleSize {
			ndv++
			TypeRows.Rows = append(TypeRows.Rows, ch.ID)
			TotalRows += ch.Father.Freq - ch.Error
			ch = ch.Next
		}
		temp = temp.Pre
	}
	TypeRows.RowsCount = ndv

	sort.Sort(TypeRows)

	popCount := 0
	popFreq := 0
	for i := 0; i < ndv; i++ {
		freq := l.GetFreq(TypeRows.Rows[i]) - l.GetError(TypeRows.Rows[i])
		if freq <= TotalRows/BucketSize {
			continue
		}
		popCount++
		popFreq += freq
	}

	His.FreqCount = uint64(popCount)
	His.FreqCum = uint64(popFreq)
	His.RowsCount = uint64(TotalRows)
	His.NDV = uint64(ndv)

	rowCnt := 0
	bktRows := 0
	bktCnt := 0
	cumFreq := 0
	bktsize := 0

	for {
		rowCnt += 1
		freq := l.GetFreq(TypeRows.Rows[rowCnt-1]) - l.GetError(TypeRows.Rows[rowCnt-1])

		if bktRows == 0 {
			if BucketSize-1 <= popCount {
				bktsize = (TotalRows - freq) / (BucketSize - 1)
			} else {
				bktsize = (TotalRows - popFreq - freq) / (BucketSize - 1 - popCount)
			}
		}
		bktRows += freq
		cumFreq += freq
		if bktRows >= bktsize || ndv-rowCnt <= BucketSize-bktCnt || rowCnt == 1 || rowCnt == ndv {
			if rowCnt == ndv {
				return His
			}
			if bktCnt == BucketSize && rowCnt != ndv { //if buckets run out，but still remains rows
				maxFre := freq
				maxIndex := rowCnt - 1
				rowCnt++
				for rowCnt <= ndv {
					freq = l.GetFreq(TypeRows.Rows[rowCnt-1]) - l.GetError(TypeRows.Rows[rowCnt-1])
					if freq > maxFre {
						maxFre = freq
						maxIndex = rowCnt - 1
					}
				}
				bkt := Histogram.HybridBucket{}
				bkt.CumFrequency = uint64(TotalRows)
				bkt.EndpointValue = TypeRows.Rows[maxIndex]
				bkt.RepeatCount = uint64(maxFre)
				His.Buckets = append(His.Buckets, bkt)
				return His
			}
			bktRows = 0
			bktCnt += 1
			bkt := Histogram.HybridBucket{}
			bkt.CumFrequency = uint64(cumFreq)
			bkt.EndpointValue = TypeRows.Rows[rowCnt-1]
			bkt.RepeatCount = uint64(freq)
			His.Buckets = append(His.Buckets, bkt)
		}
	}

}
