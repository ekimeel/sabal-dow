package dow

import (
	"context"
	"github.com/ekimeel/sabal-pb/pb"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"
)

var (
	singletonService *Service
	onceService      sync.Once
)

type Service struct {
	dao                *dao
	PointServiceClient pb.PointServiceClient
}

func GetService() *Service {

	onceService.Do(func() {
		singletonService = &Service{}
		singletonService.dao = getDao()
	})

	return singletonService
}

func (s *Service) doCalc(pointId uint32, data []*pb.Metric, weekday time.Weekday) {

	if len(data) == 0 {
		return
	}

	existing, err := s.dao.selectByPointIdAndWeekday(pointId, weekday)
	if err != nil {
		log.Error(err)
		return
	}
	if existing == nil {
		existing = &DayOfWeek{PointId: pointId, DayOfWeek: weekday}
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

func (s *Service) Run(ctx context.Context, metrics []*pb.Metric) error {

	unitOfWork := GroupMetricsByPointId(metrics)
	var wg sync.WaitGroup
	var err error

	for pointId, items := range unitOfWork {
		wg.Add(1)
		go func(pointId uint32, items []*pb.Metric) {
			defer wg.Done()

			days := make(map[time.Weekday][]*pb.Metric)
			for _, metric := range items {
				day := metric.Timestamp.AsTime().Weekday()
				days[day] = append(days[day], metric)
			}

			for day, metrics := range days {
				s.doCalc(pointId, metrics, day)
			}
		}(pointId, items)
	}

	wg.Wait()
	return err
}

func weightedAverage(value1, weight1, value2, weight2 float64) float64 {
	return (value1*weight1 + value2*weight2) / (weight1 + weight2)
}
