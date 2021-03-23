package cirrotimes

import (
	"fmt"
	"github.com/caict-benchmark/BDC-TS/bulk_data_gen/dashboard"
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"math/rand"
	"time"
)

// TimescaleDevops produces Timescale-specific queries for all the devops query types.
type CirroTimesDashboard struct {
	bulkQuerygen.CommonParams
	ClustersCount int
	bulkQuerygen.TimeWindow
}

// NewTimescaleDevops makes an TimescaleDevops object ready to generate Queries.
func NewCirroTimesDashboardCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}
	clustersCount := scaleVar / dashboard.ClusterSize //ClusterSizes[len(dashboard.ClusterSizes)/2]
	if clustersCount == 0 {
		clustersCount = 1
	}
	return &CirroTimesDashboard{
		CommonParams:  *bulkQuerygen.NewCommonParams(interval, scaleVar),
		ClustersCount: clustersCount,
		TimeWindow:    bulkQuerygen.TimeWindow{interval.Start, duration},
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *CirroTimesDashboard) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	return q
}

func (d *CirroTimesDashboard) DispatchCommon(i int) (*SessionQuery, *bulkQuerygen.TimeInterval) {
	q := NewSessionQuery() // from pool
	var interval bulkQuerygen.TimeInterval
	if bulkQuerygen.TimeWindowShift > 0 {
		interval = d.TimeWindow.SlidingWindow(&d.AllInterval)
	} else {
		interval = d.AllInterval.RandWindow(d.Duration)
	}
	return q, &interval
}

func (d *CirroTimesDashboard) GetRandomClusterId() string {
	return fmt.Sprintf("%d", rand.Intn(d.ClustersCount-1)+1)
}


