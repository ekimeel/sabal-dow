package main

import "time"

const (
	pluginName     = "timeQuality"
	pluginVersion  = "v1.0"
	configFilePath = "./ext/time-quality.json"

	envMetricServiceClient = "pb.MetricServiceClient"
	envPointServiceClient  = "pb.PointServiceClient"
	envSqlDb               = "sql.DB"
	envRunChannel          = "chan.Run"
	/**/
	timerProperty        = "tq.tickerDuration"
	timerPropertyDefault = 10 * time.Second
)
