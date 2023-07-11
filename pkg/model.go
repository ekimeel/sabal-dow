package main

import (
	"database/sql"
	"time"
)

type DayOfWeek struct {
	Id          uint32          `json:"id"`
	PointId     uint32          `json:"pointId"`
	LastUpdated time.Time       `json:"lastUpdated"`
	Start       time.Time       `json:"start"`
	End         time.Time       `json:"end"`
	Evaluations int             `json:"evaluations"`
	DayOfWeek   time.Weekday    `json:"dayOfWeek"`
	Mean        sql.NullFloat64 `json:"mean"`
	Max         sql.NullFloat64 `json:"max"`
	Min         sql.NullFloat64 `json:"min"`
	Sum         sql.NullFloat64 `json:"sum"`
	Count       int             `json:"count"`
	StdDev      sql.NullFloat64 `json:"stdDev"`
}
