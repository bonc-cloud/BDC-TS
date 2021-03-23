package cirrotimes

import (
	"fmt"
	"sync"
)

var SessionQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &SessionQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			Sql:              []byte{},
		}
	},
}

type SessionQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	Sql              []byte
}

var SgNum int64 = 20

func (q *SessionQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *SessionQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *SessionQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.Sql = q.Sql[:0]

	SessionQueryPool.Put(q)
}

func (q *SessionQuery) String() string {
	return fmt.Sprintf("Sql: \"%s\"", q.Sql)
}

func NewSessionQuery() *SessionQuery {
	return SessionQueryPool.Get().(*SessionQuery)
}

