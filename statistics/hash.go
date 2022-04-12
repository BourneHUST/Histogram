package statistics

import (
	"hash/fnv"

	"github.com/BourneHUST/Histogram/hyperloglog"
)

func StringHash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func integerHash(key uint64) uint64 {
	key = (^key) + (key << 21) // key = (key << 21) - key - 1;
	key = key ^ (key >> 24)
	key = (key + (key << 3)) + (key << 8) // key * 265
	key = key ^ (key >> 14)
	key = (key + (key << 2)) + (key << 4) // key * 21
	key = key ^ (key >> 28)
	key = key + (key << 31)
	return key
}

func NDVcounter() (*hyperloglog.HyperLogLogPlus, error) {
	return hyperloglog.NewPlus(15)
}
