package cirrotimes

import (
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"time"
)

type CirroTimesDevops8Hosts struct {
	CirroTimesDevops
}

func NewCirroTimesDevops8Hosts(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCirroTimesDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*CirroTimesDevops)
	return &CirroTimesDevops8Hosts{
		CirroTimesDevops: *underlying,
	}
}

func (c *CirroTimesDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	c.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}

