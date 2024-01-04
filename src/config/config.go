package config

import (
	"errors"
	"github.com/inspectadb/inspectadb/src/errs"
	"github.com/spf13/viper"
	"strconv"
	"strings"
)

type DBConfig struct {
	Driver   string
	User     string
	Password string
	Host     string
	Port     int
	Database string
	Schema   string
	SSLMode  string
}

type Config struct {
	DB                DBConfig
	AlternateSchema   string
	HistoryTable      string
	ChangeTablePrefix string
	ChangeTableSuffix string
	Exclude           []string
	Telemetry         bool
	LicenseKey        string
}

type App struct {
	Config Config
}

func parseDSN(dsn string) (DBConfig, error) {
	driver := dsn[:strings.Index(dsn, "://")]
	dsn = strings.TrimPrefix(dsn, driver+"://")
	userAndPassword := strings.Split(dsn[:strings.Index(dsn, "@")], ":")
	user := userAndPassword[0]
	password := userAndPassword[1]
	dsn = dsn[strings.Index(dsn, "@")+1:]
	hostAndPort := strings.Split(dsn[:strings.Index(dsn, "/")], ":")
	host := hostAndPort[0]
	port, err := strconv.Atoi(hostAndPort[1])

	if err != nil {
		return DBConfig{}, errors.Join(errs.InvalidPort, err)
	}

	dsn = dsn[strings.Index(dsn, "/")+1:]
	db := ""
	schema := ""

	// No options passed
	// TODO: Parse options if exists
	if strings.Index(dsn, "?") == -1 {
		dbAndSchema := strings.Split(dsn, ":")

		if len(dbAndSchema) > 1 {
			db = dbAndSchema[0]
			schema = dbAndSchema[1]
		} else {
			schema = dbAndSchema[0]
			db = dbAndSchema[0]
		}
	}

	return DBConfig{
		Driver:   driver,
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
		Schema:   schema,
		Database: db,
	}, nil
}

func Load(path string) (App, error) {
	viper.SetTypeByDefaultValue(true)
	viper.SetConfigType("env")
	viper.SetDefault("alternate_schema", "")
	viper.SetDefault("history_table", "inspecta_history")
	viper.SetDefault("change_table_prefix", "")
	viper.SetDefault("change_table_suffix", "audit")
	viper.SetDefault("exclude", []string{})
	viper.SetDefault("telemetry", true)
	viper.SetDefault("license_key", "")
	viper.AutomaticEnv()

	if path != "" {
		viper.SetConfigFile(path)

		if err := viper.ReadInConfig(); err != nil {
			return App{}, errors.Join(errs.FailedToLoad, err)
		}
	}

	if viper.GetString("dsn") == "" {
		return App{}, errs.InvalidDSN
	}

	dbConfig, err := parseDSN(viper.GetString("dsn"))

	if err != nil {
		return App{}, err
	}

	exclude := viper.GetStringSlice("exclude")

	if len(exclude) == 1 && strings.Contains(exclude[0], ",") {
		exclude = strings.Split(exclude[0], ",")
	}

	return App{
		Config{
			DB:                dbConfig,
			AlternateSchema:   viper.GetString("alternate_schema"),
			HistoryTable:      viper.GetString("history_table"),
			ChangeTablePrefix: viper.GetString("change_table_prefix"),
			ChangeTableSuffix: viper.GetString("change_table_suffix"),
			Exclude:           exclude,
			Telemetry:         viper.GetBool("telemetry"),
			LicenseKey:        viper.GetString("license_key"),
		},
	}, nil
}
