package cirrotimes

import (
	"fmt"
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"math/rand"
	"strconv"
	"time"
)

// OpenTSDBDevops produces OpenTSDB-specific queries for all the devops query types.
type CirroTimesDevops struct {
	bulkQuerygen.CommonParams
}


// NewOpenTSDBDevops makes an OpenTSDBDevops object ready to generate Queries.
func newCirroTimesDevopsCommon(interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return &CirroTimesDevops{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (c *CirroTimesDevops) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(c, i, q, c.ScaleVar)
	return q
}

func (c *CirroTimesDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query) {
	c.maxCPUUsageHourByMinuteNHosts(q, 1, time.Hour)
}

func (c *CirroTimesDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query) {
	c.maxCPUUsageHourByMinuteNHosts(q, 2, time.Hour)
}

func (c *CirroTimesDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query) {
	c.maxCPUUsageHourByMinuteNHosts(q, 4, time.Hour)
}

func (c *CirroTimesDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query) {
	c.maxCPUUsageHourByMinuteNHosts(q, 8, time.Hour)
}

func (c *CirroTimesDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query) {
	c.maxCPUUsageHourByMinuteNHosts(q, 16, time.Hour)
}

func (d *CirroTimesDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q, 32, time.Hour)
}

func (c *CirroTimesDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query) {
	c.maxCPUUsageHourByMinuteNHosts(q, 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (c *CirroTimesDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	/*sgNum := strconv.Itoa(int(xxhash.Sum64String(Cpu) % (uint64(SgNum))))*/
	interval := c.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(c.ScaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}
	startTimestamp := interval.StartUnixNano() / 1e6
	endTimestamp := interval.EndUnixNano() / 1e6
	var devicePrefix = SgPrefix + Wildcard + Separator + Cpu + Separator
	var devices string
	for index, hostname := range hostnames {
		device := devicePrefix + SingleQuotationMark + hostname + SingleQuotationMark + NineSW
		devices = devices + device
		if index != len(hostnames)-1 {
			devices = devices + Comma
		}
	}


	humanLabel := fmt.Sprintf("CirroTimes max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*SessionQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Sql = []byte(Select + MaxUse + From + devices + GroupBy +
		LBracket + strconv.FormatInt(startTimestamp, 10) + Comma + strconv.FormatInt(endTimestamp, 10) + RBracket1M)
}

func (d *CirroTimesDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	humanLabel := "CirroTimes mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*SessionQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.Sql = []byte(fmt.Sprintf("select time_bucket(3600000000000,time) as time1hour,avg(usage_user) from cpu where time >=%d and time < %d group by time1hour,hostname order by time1hour", interval.StartUnixNano(), interval.EndUnixNano()))
}
