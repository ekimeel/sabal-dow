package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ekimeel/sabal-pb/pb"
	"github.com/ekimeel/sabal-plugin/pkg/plugin"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
)

var (
	db                 *sql.DB
	pointServiceClient pb.PointServiceClient
	logger             *log.Logger
)

func Install(env *plugin.Environment) error {

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

	var config plugin.Config
	configFile, err := os.ReadFile(configFilePath)

	if err != nil {
		return err
	}

	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		return err
	}

	return nil
}

func Process(metrics []pb.Metric) error {
	ref := ConvertToPointerSlice(metrics)

	log.Infof("running timeQualityPlugin: %s", Name())
	err := getService().run(context.Background(), ref)
	if err != nil {
		return errors.New("failed to run timeQualityPlugin")
	}
	return nil
}

func Name() string {
	return fmt.Sprintf("%s@%s", pluginName, pluginVersion)
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

func setupPointServiceClient(env *plugin.Environment) error {
	if val, ok := env.Get(envPointServiceClient); ok {
		pointServiceClient = val.(pb.PointServiceClient)
		log.Infof("sucessfully found %s", envPointServiceClient)
	} else {
		return fmt.Errorf("plugin %s requireds a valid %s value", pluginName, envPointServiceClient)
	}
	return nil
}
