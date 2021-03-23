package cirrotimes

import (
	"fmt"
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"strconv"
	"time"
)

type CirroTimesKapaLoad struct {
	CirroTimesDashboard
}

func NewCirroTimesKapaLoad(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewCirroTimesDashboardCommon(dbConfig,queriesFullRange, queryInterval, scaleVar).(*CirroTimesDashboard)
	return &CirroTimesKapaLoad{
		CirroTimesDashboard: *underlying,
	}
}

func (c *CirroTimesKapaLoad) Dispatch(i int) bulkQuerygen.Query {
	q, interval := c.CirroTimesDashboard.DispatchCommon(i)
	/*sgNum := strconv.Itoa(int(xxhash.Sum64String(System) % (uint64(SgNum))))*/
	startTimestamp := interval.StartUnixNano() / 1e6
	endTimestamp := interval.EndUnixNano() / 1e6

	q.Sql = []byte(Select + KapaLoad + From + SgPrefix + Wildcard + Separator + System + Separator +
		KapaSuf + Where + Time + Gte + strconv.FormatInt(startTimestamp, 10) + And + Time + Lt + strconv.FormatInt(endTimestamp, 10))
	humanLabel := fmt.Sprintf("CirroTimes kapa load,time %s", interval.Duration())

	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	return q
}

