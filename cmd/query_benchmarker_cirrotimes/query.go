package main

import "fmt"

// Query holds CirroTimeS SQL query, typically decoded from the program's
// input.
type Query struct {
	HumanLabel       []byte
	HumanDescription []byte
	Sql              []byte
	ID               int64
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Sql)
}
