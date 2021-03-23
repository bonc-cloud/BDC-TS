package cirrotimes

import (
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"time"
)

// OpenTSDBDevopsSingleHost produces OpenTSDB-specific queries for the devops single-host case.
type CirroTimesDevopsSingleHost struct {
	CirroTimesDevops
}

func NewCirroTimesDevopsSingleHost(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCirroTimesDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*CirroTimesDevops)
	return &CirroTimesDevopsSingleHost{
		CirroTimesDevops: *underlying,
	}
}

func (c *CirroTimesDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	c.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}