package config

import (
	"errors"
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
	DB               DBConfig
	HistoryTable     string
	AuditTablePrefix string
	AuditTableSuffix string
	CaseStrategy     string
	NamingStrategy   string
	Exclude          []string
	Telemetry        bool
}

type App struct {
	Config Config
}

func parseDSN(dsn string) (DBConfig, error) {

	if !strings.HasPrefix(dsn, "mysql://") && !strings.HasPrefix(dsn, "pgsql://") {
		return DBConfig{}, errors.New("failed to load config: unknown driver used in 'dsn'")
	}

	driver := dsn[:strings.Index(dsn, "://")]
	dsn = strings.TrimLeft(dsn, driver+"://")

	userAndPassword := strings.Split(dsn[:strings.Index(dsn, "@")], ":")
	user := userAndPassword[0]
	password := userAndPassword[1]
	dsn = dsn[strings.Index(dsn, "@")+1:]

	hostAndPort := strings.Split(dsn[:strings.Index(dsn, "/")], ":")
	host := hostAndPort[0]
	port, err := strconv.Atoi(hostAndPort[1])

	if err != nil {
		return DBConfig{}, errors.New("failed to load config: failed to parse port")
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
	viper.SetEnvPrefix("INSPECTA")
	viper.SetConfigType("env")
	viper.SetDefault("history_table", "inspecta_history")
	viper.SetDefault("audit_table_prefix", "")
	viper.SetDefault("audit_table_suffix", "audit")
	viper.SetDefault("case_strategy", "lower")
	viper.SetDefault("naming_strategy", "")
	viper.SetDefault("exclude", []string{})
	viper.SetDefault("telemetry", true)
	viper.AutomaticEnv()

	if path != "" {
		viper.SetConfigFile(path)

		if err := viper.ReadInConfig(); err != nil {
			return App{}, err
		}
	}

	if viper.GetString("dsn") == "" {
		return App{}, errors.New("failed to load config: 'dsn' must be set")
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
			DB:               dbConfig,
			HistoryTable:     viper.GetString("history_table"),
			AuditTablePrefix: viper.GetString("audit_table_prefix"),
			AuditTableSuffix: viper.GetString("audit_table_suffix"),
			CaseStrategy:     viper.GetString("case_strategy"),
			NamingStrategy:   viper.GetString("naming_strategy"),
			Exclude:          exclude,
			Telemetry:        viper.GetBool("telemetry"),
		},
	}, nil
}
