package cirrotimes

import "time"
import bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"

type CirroTimesDevopsSingleHost12hr struct {
	CirroTimesDevops
}

func NewCirroTimesDevopsSingleHost12hr(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCirroTimesDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*CirroTimesDevops)
	return &CirroTimesDevopsSingleHost12hr{
		CirroTimesDevops: *underlying,
	}
}

func (c *CirroTimesDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	c.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
