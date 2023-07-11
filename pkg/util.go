package main

import "github.com/ekimeel/sabal-pb/pb"

// ConvertToPointerSlice takes a slice of pb.Metric values and returns a slice of pointers to those values.
// It creates a new slice and adds the address of each pb.Metric in the original slice to the new slice.
// Note that modifications to the pb.Metric values through the returned pointers will affect the original values.
// This function is useful when you want to avoid copying pb.Metric values, for example when passing them to a function.
func ConvertToPointerSlice(metrics []pb.Metric) []*pb.Metric {
	metricsPtr := make([]*pb.Metric, len(metrics))
	for i := range metrics {
		metricsPtr[i] = &metrics[i]
	}
	return metricsPtr
}

// UniquePointIds takes a slice of pb.Metric pointers and returns a slice of unique PointId values.
// It uses a map to efficiently eliminate duplicates, then converts the map keys to a slice.
// The returned slice contains the unique PointId values in no particular order.
func UniquePointIds(metrics []*pb.Metric) []uint32 {
	idMap := make(map[uint32]bool)
	for _, metric := range metrics {
		idMap[metric.PointId] = true
	}

	ids := make([]uint32, 0, len(idMap))
	for id := range idMap {
		ids = append(ids, id)
	}

	return ids
}

// FilterMetricsByPointId takes a slice of pb.Metric pointers and a PointId,
// and returns a new slice containing only the metrics with the provided PointId.
// The returned slice may be empty if no metrics match the provided PointId.
func FilterMetricsByPointId(metrics []*pb.Metric, pointId uint32) []*pb.Metric {
	var filteredMetrics []*pb.Metric
	for _, metric := range metrics {
		if metric.PointId == pointId {
			filteredMetrics = append(filteredMetrics, metric)
		}
	}
	return filteredMetrics
}

// GroupMetricsByPointId takes a slice of pb.Metric pointers and returns a map where the keys are PointIds
// and the values are slices of metrics with the corresponding PointId.
// The returned map may be empty if the input slice is empty.
func GroupMetricsByPointId(metrics []*pb.Metric) map[uint32][]*pb.Metric {
	metricsMap := make(map[uint32][]*pb.Metric)
	for _, metric := range metrics {
		metricsMap[metric.PointId] = append(metricsMap[metric.PointId], metric)
	}
	return metricsMap
}
