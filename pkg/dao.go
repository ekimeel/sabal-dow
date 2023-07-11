package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

var (
	singletonDao               *dao
	onceDao                    sync.Once
	sqlInsert                  = "INSERT INTO plugin_day_of_week (point_id, last_updated, start_time, end_time, evals, day_of_week, mean, max, min, sum, count, std_dev) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlSelectAllByPoint        = "SELECT point_id, last_updated, start_time, end_time, evals, day_of_week, mean, max, min, sum, count, std_dev FROM plugin_day_of_week WHERE point_id = ?"
	sqlSelectByPointAndWeekday = "SELECT id, point_id, day_of_week, last_updated, start_time, end_time, evals, mean, max, min, sum, count, std_dev FROM plugin_day_of_week WHERE point_id = ? AND day_of_week = ?"
	sqlUpdate                  = "UPDATE plugin_day_of_week SET last_updated = ?, start_time = ?, end_time = ?, evals = ?, mean = ?, max = ?, min = ?, sum = ?, count = ?, std_dev = ? WHERE id = ?"
)

type dao struct {
}

func getDao() *dao {
	onceDao.Do(func() {
		singletonDao.createTableIfNotExists()
	})
	return singletonDao
}

func (dao *dao) createTableIfNotExists() {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS plugin_day_of_week (
			id INTEGER PRIMARY KEY,
			point_id INTEGER,
			last_updated TIMESTAMP,
			start_time TIMESTAMP,
			end_time TIMESTAMP, 
			evals INTEGER,
			day_of_week INTEGER,
			mean FLOAT,
			max FLOAT,
			min FLOAT,
			sum FLOAT,
			count INTEGER,
			std_dev FLOAT,
			FOREIGN KEY (point_id) REFERENCES point(id),
			UNIQUE(point_id, day_of_week)
		);
	`)
	if err != nil {
		panic(fmt.Sprintf("failed to create table: %v", err))
	}
}

func (dao *dao) insert(dow *DayOfWeek) (uint32, error) {
	result, err := db.Exec(sqlInsert, dow.PointId, time.Now(), dow.Start, dow.End, dow.Evaluations, dow.DayOfWeek,
		dow.Mean, dow.Max, dow.Min, dow.Sum, dow.Count, dow.StdDev)
	if err != nil {
		return 0, fmt.Errorf("failed to insert: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to selectById ID of created point: %v", err)
	}
	return uint32(id), nil
}

func (dao *dao) selectByPointIdAndWeekday(pointId uint32, weekday Weekday) (*DayOfWeek, error) {
	dow := &DayOfWeek{}
	err := db.QueryRow(sqlSelectByPointAndWeekday, pointId, weekday).Scan(
		&dow.Id,
		&dow.PointId,
		&dow.DayOfWeek,
		&dow.LastUpdated,
		&dow.Start,
		&dow.End,
		&dow.Evaluations,
		&dow.Mean,
		&dow.Max,
		&dow.Min,
		&dow.Sum,
		&dow.Count,
		&dow.StdDev,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // no rows found, return nil without error
		}
		return nil, fmt.Errorf("failed to select: %v", err)
	}
	return dow, nil
}

func (dao *dao) selectAllByPointId(pointId uint32) ([]*DayOfWeek, error) {
	rows, err := db.Query(sqlSelectAllByPoint, pointId)
	if err != nil {
		return nil, fmt.Errorf("failed to select: %v", err)
	}
	defer rows.Close()
	results := make([]*DayOfWeek, 0)
	if err != sql.ErrNoRows {
		return results, nil
	}

	if err != nil {
		return nil, err
	}
	for rows.Next() {
		dow := &DayOfWeek{}
		err = rows.Scan(&dow.PointId, &dow.LastUpdated, &dow.Start, &dow.End, &dow.Evaluations, &dow.DayOfWeek,
			&dow.Mean, &dow.Max, &dow.Min, &dow.Sum, &dow.Count, &dow.StdDev)
		if err != nil {
			return nil, fmt.Errorf("failed to scan point row: %v", err)
		}
		results = append(results, dow)
	}
	return results, nil
}

func (dao *dao) update(dow *DayOfWeek) error {
	_, err := db.Exec(sqlUpdate,
		time.Now(),
		dow.Start,
		dow.End,
		dow.Evaluations,
		dow.Mean,
		dow.Max,
		dow.Min,
		dow.Sum,
		dow.Count,
		dow.StdDev,
		dow.Id,
	)
	if err != nil {
		return fmt.Errorf("failed to update: %v", err)
	}
	return nil
}
