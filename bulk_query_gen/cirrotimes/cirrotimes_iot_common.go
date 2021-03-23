package cirrotimes

import (
	"fmt"
	bulkDataGenIot "github.com/caict-benchmark/BDC-TS/bulk_data_gen/iot"
	bulkQuerygen "github.com/caict-benchmark/BDC-TS/bulk_query_gen"
	"math/rand"
	"strconv"
	"time"
)

// TimescaleDevops produces Timescale-specific queries for all the devops query types.
type CirroTimesIot struct {
	bulkQuerygen.CommonParams
}

// NewTimescaleDevops makes an TimescaleDevops object ready to generate Queries.
func NewCirroTimesIotCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &CirroTimesIot{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *CirroTimesIot) Dispatch(i int) bulkQuerygen.Query {
	q := NewSessionQuery() // from pool
	bulkQuerygen.IotDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *CirroTimesIot) AverageTemperatureDayByHourOneHome(q bulkQuerygen.Query) {
	d.averageTemperatureDayByHourNHomes(q, 1, time.Hour*6)
}

// averageTemperatureHourByMinuteNHomes populates a Query with a query that looks like:
// SELECT avg(temperature) from air_condition_room where (home_id = '$HHOME_ID_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1h)
func (d *CirroTimesIot) averageTemperatureDayByHourNHomes(qi bulkQuerygen.Query, nHomes int, timeRange time.Duration) {
	/*sgNum := strconv.Itoa(int(xxhash.Sum64String(AirConditionRoom) % (uint64(SgNum))))*/
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nHomes]

	homes := []string{}
	for _, n := range nn {
		homes = append(homes, fmt.Sprintf(bulkDataGenIot.SmartHomeIdFormat, n))
	}

	startTimestamp := interval.StartUnixNano() / 1e6
	endTimestamp := interval.EndUnixNano() / 1e6
	var devicePrefix = SgPrefix + Wildcard + Separator + AirConditionRoom + Separator
	var devices string
	for index, hostname := range homes {
		device := devicePrefix + Wildcard + Separator + hostname + Separator + Wildcard
		devices = devices + device
		if index != len(homes)-1 {
			devices = devices + Comma
		}
	}

	humanLabel := fmt.Sprintf("CirroTimes mean temperature, rand %4d homes, rand %s by 1h", nHomes, timeRange)
	q := qi.(*SessionQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.Sql = []byte(Select + AvgTem + From + devices + GroupBy +
		LBracket + strconv.FormatInt(startTimestamp, 10) + Comma + strconv.FormatInt(endTimestamp, 10) + RBracket1H)

}

