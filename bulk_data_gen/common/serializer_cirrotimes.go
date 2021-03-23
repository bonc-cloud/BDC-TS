package common

import (
	"encoding/json"
	"github.com/apache/iotdb-client-go/client"
	"github.com/cespare/xxhash"
	"io"
	"strconv"
)

type SerializerCirrotimes struct {
	sgNum int64
}

const (
	Separator = "."
	SingleQuotationMark = "\""
)

func NewSerializerCirrotimes(sgNum int64) *SerializerCirrotimes {
	return &SerializerCirrotimes{sgNum}
}

// SerializerCirrotimes writes Point data to the given writer, conforming to the
// Cirrotimes wire protocol.
//
// This function writes output that looks like:
// device:<device>,timestamp:<timestamp>,measurements:[measurements],values:[values],dataTypes:[dataTypes]
//
// For example:
// "device":"root.sg_1.d_1","timestamp":1514766055000,"measurements":["s1","s2"],"values":[255,1431],"dataTypes":[2,2]
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (s *SerializerCirrotimes) SerializePoint(w io.Writer, p *Point) (err error) {
	type writePoint struct {
		Device    string            `json:"device"`
		Timestamp int64             `json:"timestamp"`
		Measurements []string       `json:"measurements"`
		Values    []interface{}     `json:"values"`
		DataTypes []client.TSDataType `json:"dataTypes"`
	}

	encoder := json.NewEncoder(w)
	wp := writePoint{}
	wp.Timestamp = p.Timestamp.UTC().UnixNano() / 1e6
	wp.Device = string(p.MeasurementName) // will be re-used
	for i := 0; i < len(p.TagKeys); i++ {
		// so many allocs..
		wp.Device = wp.Device + Separator + SingleQuotationMark + string(p.TagValues[i]) + SingleQuotationMark
	}
	sgNum := strconv.Itoa(int(xxhash.Sum64String(wp.Device)%(uint64(s.sgNum))))
	wp.Device = "root.sg_" + sgNum + Separator + wp.Device
	for i := 0; i < len(p.FieldKeys); i++ {
		wp.Measurements = append(wp.Measurements, string(p.FieldKeys[i]))
		switch p.FieldValues[i].(type) {
		case int64:
			wp.DataTypes = append(wp.DataTypes, client.INT64)
		case float64:
			wp.DataTypes = append(wp.DataTypes, client.DOUBLE)
		case float32:
			wp.DataTypes = append(wp.DataTypes, client.FLOAT)
		case int32:
			wp.DataTypes = append(wp.DataTypes, client.INT32)
		case bool:
			wp.DataTypes = append(wp.DataTypes, client.BOOLEAN)
		case []byte:
			wp.DataTypes = append(wp.DataTypes, client.TEXT)
		case int:
			p.FieldValues[i] = int64(p.FieldValues[i].(int))
			wp.DataTypes = append(wp.DataTypes, client.INT64)
		}
		wp.Values = append(wp.Values, p.FieldValues[i])
	}
	encoder.Encode(wp)
	return err
}

func (s *SerializerCirrotimes) SerializeSize(w io.Writer, points int64, values int64) error {
	return nil
}
