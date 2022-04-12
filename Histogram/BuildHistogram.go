package Histogram

import (
	"fmt"
	"time"
)

func (S *Samples) ConfirmHistogramType(BucketSize int) string {
	if len(S.DistinctValues) <= BucketSize {
		return "FrequencyHistogram"
	} else {

		Fre := 0
		for i := len(S.DistinctValues) - 1; i >= len(S.DistinctValues)-BucketSize; i-- {
			Fre += S.Freq[S.DistinctValues[i]]
		}
		if float64(Fre)/float64(S.SampleRows)*100.0-(1-(1/float64(BucketSize)))*100.0 >= 0 {
			fmt.Println(float64(Fre) / float64(S.SampleRows))
			fmt.Println((1 - (1 / float64(BucketSize))))
			return "TopNFrequencyHistogram"
		} else {
			return "HybridHistogram"
		}
	}
}
func (S *Samples) BuildFrequencyHistogram(BucketSize int) FrequencyHistogram {
	His := FrequencyHistogram{}
	His.Buckets = make(map[interface{}]int)
	His.BS = S.BS
	His.BktSize = BucketSize
	His.Type = S.Type
	His.LastUpdateTime = time.Now()

	His.DistinctValues = S.DistinctValues
	for k, v := range S.Freq {
		His.Buckets[k] = v
	}
	return His
}
func (S *Samples) BuildHybridHistogram(BucketSize int) HybridHistogram {
	His := HybridHistogram{}
	His.BS = S.BS
	His.LastUpdateTime = time.Now()
	His.Type = S.Type

	ndv := len(S.DistinctValues)
	var TotalRows int
	TotalRows = int(S.SampleRows)
	popCount := 0
	popFreq := 0
	for i := 0; i < ndv; i++ {
		freq := S.Freq[S.DistinctValues[i]]
		if freq <= TotalRows/BucketSize {
			continue
		}
		popCount++
		popFreq += freq
	}

	His.FreqCount = uint64(popCount)
	His.FreqCum = uint64(popFreq)
	His.RowsCount = S.SampleRows
	His.NDV = uint64(ndv)

	rowCnt := 0
	bktRows := 0
	bktCnt := 0
	cumFreq := 0
	bktsize := 0

	for {
		rowCnt += 1
		freq := S.Freq[S.DistinctValues[rowCnt-1]]

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
					freq := S.Freq[S.DistinctValues[rowCnt-1]]
					if freq > maxFre {
						maxFre = freq
						maxIndex = rowCnt - 1
					}
				}
				bkt := HybridBucket{}
				bkt.CumFrequency = uint64(TotalRows)
				bkt.EndpointValue = S.DistinctValues[maxIndex]
				bkt.RepeatCount = uint64(maxFre)
				His.Buckets = append(His.Buckets, bkt)
				return His
			}
			bktRows = 0
			bktCnt += 1
			bkt := HybridBucket{}
			bkt.CumFrequency = uint64(cumFreq)
			bkt.EndpointValue = S.DistinctValues[rowCnt-1]
			bkt.RepeatCount = uint64(freq)
			His.Buckets = append(His.Buckets, bkt)
		}
	}

}
