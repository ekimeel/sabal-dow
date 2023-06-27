package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ekimeel/sabal-pb/pb"
	"github.com/ekimeel/sabal-plugin/pkg/plugin"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

var (
	status              plugin.Status
	db                  *sql.DB
	metricServiceClient pb.MetricServiceClient
	pointServiceClient  pb.PointServiceClient
	runChannel          chan string
	ticker              *time.Ticker
	logger              *log.Logger
	tickerDuration      time.Duration
)

func Install(env *plugin.Environment) error {

	if status == plugin.Running {
		return fmt.Errorf("plugin %s has running status, cannot be install", pluginName)
	}

	if val, ok := env.Get("logger"); ok {
		logger, _ = val.(*log.Logger)
	} else {
		logger = log.New()
	}

	logger.Infof("installing plugin: %s@%s", pluginName, pluginVersion)

	logger.Infof("configuring environment")
	if err := setupDatabase(env); err != nil {
		return err
	}

	if err := setupPointServiceClient(env); err != nil {
		return err
	}

	if err := setupMetricServiceClient(env); err != nil {
		return err
	}

	if err := setupRunChannel(env); err != nil {
		return err
	}

	var config plugin.Config
	configFile, err := os.ReadFile(configFilePath)

	if err != nil {
		return err
	}

	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		return err
	}

	if err := setupTimer(config); err != nil {
		return err
	}

	status = plugin.None

	start()

	return nil
}

func Run(offset *plugin.Offset) error {
	log.Infof("running timeQualityPlugin: %s", Name())
	status = plugin.Running
	err := getService().run(offset)
	if err != nil {
		status = plugin.Error
		return errors.New("failed to run timeQualityPlugin")
	}
	status = plugin.Ready
	return nil
}

func Name() string {
	return fmt.Sprintf("%s@%s", pluginName, pluginVersion)
}

func setupTimer(config plugin.Config) error {
	var err error
	if exists := config.Properties[timerProperty]; exists == nil {
		log.Warnf("no tickerDuration set, defaulting to [%s]", timerPropertyDefault)
		tickerDuration = timerPropertyDefault
	} else {
		p := config.Properties[timerProperty]
		log.Infof("found property: [%s]", timerProperty)
		tickerDuration, err = time.ParseDuration(fmt.Sprintf("%s", p))
		if err != nil {
			log.Errorf("failed to ready property [%s]", timerProperty)
			log.Warnf("no tickerDuration set, defaulting to [%s]", timerPropertyDefault)
			tickerDuration = timerPropertyDefault
			return err
		}
	}
	return err
}

func setupDatabase(env *plugin.Environment) error {
	if val, ok := env.Get(envSqlDb); ok {
		db = val.(*sql.DB)
		log.Infof("sucessfully found %s", envSqlDb)
	} else {
		return fmt.Errorf("plugin %s requireds a valid %s value", pluginName, envSqlDb)
	}
	return nil
}

func setupRunChannel(env *plugin.Environment) error {
	if val, ok := env.Get(envRunChannel); ok {
		runChannel = val.(chan string)
		log.Infof("sucessfully found %s", envRunChannel)
	} else {
		return fmt.Errorf("plugin %s requireds a valid %s value", pluginName, envRunChannel)
	}
	return nil
}

func setupMetricServiceClient(env *plugin.Environment) error {
	if val, ok := env.Get(envMetricServiceClient); ok {
		metricServiceClient = val.(pb.MetricServiceClient)
		log.Infof("sucessfully found %s", envMetricServiceClient)
	} else {
		return fmt.Errorf("plugin %s requireds a valid %s value", pluginName, envMetricServiceClient)
	}
	return nil
}

func setupPointServiceClient(env *plugin.Environment) error {
	if val, ok := env.Get(envPointServiceClient); ok {
		pointServiceClient = val.(pb.PointServiceClient)
		log.Infof("sucessfully found %s", envPointServiceClient)
	} else {
		return fmt.Errorf("plugin %s requireds a valid %s value", pluginName, envPointServiceClient)
	}
	return nil
}

func start() {
	log.Infof("starting ticker")
	ticker = time.NewTicker(tickerDuration)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Infof("requesting update: %s", Name())
				runChannel <- Name()
			}
		}
	}()
}
