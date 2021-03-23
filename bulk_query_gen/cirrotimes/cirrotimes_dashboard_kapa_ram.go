package cirrotimes

import (
	"fmt"
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"strconv"
	"time"
)

type CirroTimesKapaRam struct {
	CirroTimesDashboard
}

func NewCirroTimesKapaRam(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewCirroTimesDashboardCommon(dbConfig,queriesFullRange, queryInterval, scaleVar).(*CirroTimesDashboard)
	return &CirroTimesKapaRam{
		CirroTimesDashboard: *underlying,
	}
}

func (c *CirroTimesKapaRam) Dispatch(i int) bulkQuerygen.Query {
	q, interval := c.CirroTimesDashboard.DispatchCommon(i)
	startTimestamp := interval.StartUnixNano() / 1e6
	endTimestamp := interval.EndUnixNano() / 1e6

	q.Sql = []byte(Select + UsePercent + From + SgPrefix + Wildcard + Separator + Ram + Separator +
		KapaSuf + Where + Time + Gte + strconv.FormatInt(startTimestamp, 10) + And + Time + Lt + strconv.FormatInt(endTimestamp, 10))
	humanLabel := fmt.Sprintf("CirroTimes kapa ram,time %s", interval.Duration())

	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	return q
}

