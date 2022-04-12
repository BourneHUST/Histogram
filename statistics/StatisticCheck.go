package statistics

func (l *TopNList) ConfirmHistogramType(BucketSize int) string {
	if l.BS.NDV <= uint64(BucketSize) {
		return "FrequencyHistogram"
	} else {
		count := 0
		Fre := 0
		temp := l.head.Next
		for temp.Next != nil {
			temp = temp.Next
		}
		for temp != l.head && count < BucketSize {
			ch := temp.child
			for ch != nil && count < BucketSize {
				count++
				Fre += ch.Father.Freq - ch.Error
				ch = ch.Next
			}
			temp = temp.Pre
		}
		if float64(Fre)/float64(l.BS.TotalRows)*100.0-(1-(1/float64(BucketSize)))*100.0 >= 0 {
			return "TopNFrequencyHistogram"
		} else {
			return "HybridHistogram"
		}
	}
}
