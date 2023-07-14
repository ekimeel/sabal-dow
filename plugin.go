package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ekimeel/sabal-pb/pb"
	"github.com/ekimeel/sabal-plugin/pkg/plugin"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	dow "sabal-dow/pkg"
	"time"
)

const (
	PluginName            = "dow"
	PluginVersion         = "v1.0"
	configFilePath        = "./ext/dow.json"
	EnvPointServiceClient = "pb.PointServiceClient"
	EnvSqlDb              = "sql.DB"
)

var (
	logger *log.Logger
)

// Install sets up the plugin environment and configures the database and PointServiceClient.
func Install(env *plugin.Environment) error {
	// Set up logger
	if val, ok := env.Get("logger"); ok {
		logger, _ = val.(*log.Logger)
	} else {
		logger = log.New()
	}

	logger.Infof("installing plugin: %s@%s", PluginName, PluginVersion)

	// Set up database
	if err := setupDatabase(env); err != nil {
		return err
	}

	// Set up PointServiceClient
	if err := setupPointServiceClient(env); err != nil {
		return err
	}

	// Load plugin configuration
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

// Process handles the plugin event. It logs the processing time and any errors that occur.
func Process(ctx context.Context, event plugin.Event) {
	if len(event.Metrics) == 0 {
		log.Infof("no metrics to process")
		return
	}

	ref := dow.ConvertToPointerSlice(event.Metrics)

	log.Infof("running: %s", Name())
	start := time.Now()

	err := dow.GetService().Run(ctx, ref)
	if err != nil {
		log.Errorf("failed to run timeQualityPlugin: %v", err)
		return
	}

	log.Infof("%s, processed [%d] in [%s]", Name(), len(event.Metrics), time.Since(start))
}

// Name returns the plugin name and version.
func Name() string {
	return fmt.Sprintf("%s@%s", PluginName, PluginVersion)
}

// setupDatabase configures the database for the plugin.
func setupDatabase(env *plugin.Environment) error {
	if val, ok := env.Get(EnvSqlDb); ok {
		dow.DB = val.(*sql.DB)
		log.Infof("successfully found %s", EnvSqlDb)
	} else {
		return fmt.Errorf("plugin %s requires a valid %s value", PluginName, EnvSqlDb)
	}
	return nil
}

// setupPointServiceClient configures the PointServiceClient for the plugin.
func setupPointServiceClient(env *plugin.Environment) error {
	if val, ok := env.Get(EnvPointServiceClient); ok {
		dow.GetService().PointServiceClient = val.(pb.PointServiceClient)
		log.Infof("successfully found %s", EnvPointServiceClient)
	} else {
		return fmt.Errorf("plugin %s requires a valid %s value", PluginName, EnvPointServiceClient)
	}
	return nil
}
