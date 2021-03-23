package cirrotimes

import (
	"time"
)
import bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"

type CirroTimesIotSingleHost struct {
	CirroTimesIot
}

func NewCirroTimesIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewCirroTimesIotCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*CirroTimesIot)
	return &CirroTimesIotSingleHost{
		CirroTimesIot: *underlying,
	}
}

func (c *CirroTimesIotSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	c.AverageTemperatureDayByHourOneHome(q)
	return q
}
