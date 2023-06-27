package main

import (
	"context"
	"errors"
	"github.com/ekimeel/sabal-pb/pb"
	"github.com/ekimeel/sabal-plugin/pkg/plugin"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"sync"
	"time"
)

var (
	singletonService *service
	onceService      sync.Once
)

type service struct {
	dao *dao
}

func getService() *service {

	onceService.Do(func() {
		singletonService = &service{}
		singletonService.dao = getDao()
	})

	return singletonService
}

func (s *service) doCalc(point *pb.Point, data []*pb.Metric, weekday Weekday) {

	if len(data) == 0 {
		return
	}

	existing, err := s.dao.selectAllByPointIdAndWeekday(point.Id, weekday)
	if err != nil {
		log.Error(err)
		return
	}
	if existing == nil {
		existing = &DayOfWeek{PointId: point.Id, DayOfWeek: weekday}
		existing.Id, err = s.dao.insert(existing)
		if err != nil {
			log.Error(err)
			return
		}
		if existing.Id == 0 {
			log.Error("failed to obtain id")
			return
		}
	}

	var sum, sumsqrd, stddev float64
	var count int
	for i := range data {
		d := data[i]

		sum += d.Value
		if !existing.Min.Valid || existing.Count == 0 || d.Value < existing.Min.Float64 {
			err = existing.Min.Scan(d.Value)
			if err != nil {
				log.Warnf("failed ot scan min: %s", err)
			}
		}
		if !existing.Max.Valid || existing.Count == 0 || d.Value > existing.Max.Float64 {
			err = existing.Max.Scan(d.Value)
			if err != nil {
				log.Warnf("failed ot scan max: %s", err)
			}
		}
		if existing.Start.Unix() <= 0 || data[i].Timestamp.GetSeconds() < existing.Start.Unix() {
			existing.Start = data[i].Timestamp.AsTime()
		}

		if data[i].Timestamp.GetSeconds() > existing.End.Unix() {
			existing.End = data[i].Timestamp.AsTime()
		}

		sumsqrd += d.Value * d.Value
		count += 1
	}

	average := sum / float64(count)
	stddev = math.Sqrt(sumsqrd/float64(count) - average*average)

	if existing.Count > 0 {
		curWeight := float64(len(data)) / float64(existing.Count-len(data))
		totalWeight := 1.0 - curWeight

		if existing.Mean.Valid {
			existing.Mean.Scan(weightedAverage(existing.Mean.Float64, totalWeight, average, curWeight))
		} else {
			existing.Mean.Scan(average)
		}

		if existing.StdDev.Valid {
			existing.StdDev.Scan(weightedAverage(existing.StdDev.Float64, totalWeight, stddev, curWeight))
		} else {
			existing.StdDev.Scan(stddev)
		}

		if existing.Sum.Valid {
			existing.Sum.Scan(existing.Sum.Float64 + sum)
		} else {
			existing.Sum.Scan(sum)
		}

	} else {
		existing.Sum.Scan(sum)
		existing.Mean.Scan(average)
		existing.StdDev.Scan(stddev)
	}

	existing.Count += count
	existing.Evaluations += 1

	err = s.dao.update(existing)
	if err != nil {
		log.Errorf("failed to update day_of_week: %s", err)
	}

}

func (s *service) run(offset *plugin.Offset) error {

	req := &pb.ListRequest{Limit: 1000, Offset: 0}
	res, err := pointServiceClient.GetAll(context.Background(), req)

	if err != nil {
		log.Warn("failed to get points")
		return errors.New("no points")
	}

	for i := range res.Points {
		point := res.Points[i]

		r := &pb.MetricRequest{
			PointId: point.Id,
			From:    timestamppb.New(offset.Value),
			To:      timestamppb.New(time.Unix(1<<63-1, 0)),
		}

		data, err := metricServiceClient.Select(context.Background(), r)
		if err != nil {
			log.Errorf("failed to read data for point id:%d, from:%s, to:%s err: %s", point.Id, r.From, r.To, err)
			continue
		}

		mon := make([]*pb.Metric, 0)
		tue := make([]*pb.Metric, 0)
		wed := make([]*pb.Metric, 0)
		thr := make([]*pb.Metric, 0)
		fri := make([]*pb.Metric, 0)
		sat := make([]*pb.Metric, 0)
		sun := make([]*pb.Metric, 0)

		for j := range data.Metrics {
			d := data.Metrics[j]
			switch d.Timestamp.AsTime().Weekday() {
			case time.Monday:
				mon = append(mon, d)
			case time.Tuesday:
				tue = append(tue, d)
			case time.Wednesday:
				wed = append(wed, d)
			case time.Thursday:
				thr = append(thr, d)
			case time.Friday:
				fri = append(fri, d)
			case time.Saturday:
				sat = append(sat, d)
			case time.Sunday:
				sun = append(sun, d)
			}
		}

		s.doCalc(point, mon, Monday)
		s.doCalc(point, tue, Tuesday)
		s.doCalc(point, wed, Wednesday)
		s.doCalc(point, thr, Thursday)
		s.doCalc(point, fri, Friday)
		s.doCalc(point, sat, Saturday)
		s.doCalc(point, sun, Sunday)

	}

	return nil
}

func weightedAverage(value1, weight1, value2, weight2 float64) float64 {
	return (value1*weight1 + value2*weight2) / (weight1 + weight2)
}
