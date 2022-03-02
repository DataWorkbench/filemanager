package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/common/utils/logutil"
	"github.com/DataWorkbench/loader"
	"github.com/DataWorkbench/resourcemanager/pkg/fileio"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

// FilePath The config file path used by Load config
var FilePath string

const (
	envPrefix = "RESOURCE_MANAGER"
)

const (
	StorageBackgroundHDFS = "hdfs"
	StorageBackgroundS3   = "s3"
)

// Config is the configuration settings for spacemanager
type Config struct {
	LogConfig *logutil.Config `json:"log" yaml:"log" env:"LOG,default=" validate:"required"`

	GRPCServer    *grpcwrap.ServerConfig `json:"grpc_server"    yaml:"grpc_server"    env:"GRPC_SERVER"         validate:"required"`
	GRPCLog       *grpcwrap.LogConfig    `json:"grpc_log"       yaml:"grpc_log"       env:"GRPC_LOG"            validate:"required"`
	MetricsServer *metrics.Config        `json:"metrics_server" yaml:"metrics_server" env:"METRICS_SERVER"      validate:"required"`
	Tracer        *gtrace.Config         `json:"tracer"         yaml:"tracer"         env:"TRACER"              validate:"required"`

	// storage_background
	StorageBackground string           `json:"storage_background" yaml:"storage_background" env:"STORAGE_BACKGROUND,default=hdfs" validate:"required"`
	HadoopConfDir     string           `json:"hadoop_conf_dir" yaml:"hadoop_conf_dir" env:"HADOOP_CONF_DIR" validate:"-"`
	S3Config          *fileio.S3Config `json:"s3_config" yaml:"s3_config" env:"S3_CONFIG" validate:"-"`
}

func loadFromFile(cfg *Config) (err error) {
	if FilePath == "" {
		return
	}

	fmt.Printf("%s load config from file <%s>\n", time.Now().Format(time.RFC3339Nano), FilePath)

	var b []byte
	b, err = ioutil.ReadFile(FilePath)
	if err != nil && os.IsNotExist(err) {
		return
	}

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		fmt.Println("parse config file error:", err)
	}
	return
}

// Load load all configuration from specified file
// Must be set `FilePath` before called
func Load() (cfg *Config, err error) {
	cfg = &Config{}

	_ = loadFromFile(cfg)

	l := loader.New(
		loader.WithPrefix(envPrefix),
		loader.WithTagName("env"),
		loader.WithOverride(true),
	)
	if err = l.Load(cfg); err != nil {
		return
	}

	// output the config content
	fmt.Printf("%s pid=%d the latest configuration: \n", time.Now().Format(time.RFC3339Nano), os.Getpid())
	fmt.Println("")
	b, _ := yaml.Marshal(cfg)
	fmt.Println(string(b))

	validate := validator.New()
	if err = validate.Struct(cfg); err != nil {
		return
	}

	// check storage background
	switch cfg.StorageBackground {
	case StorageBackgroundHDFS:
		if cfg.HadoopConfDir == "" {
			err = errors.New("hadoop_conf_dir must specified when storage_background is hdfs")
		}
	case StorageBackgroundS3:
		err = validate.Struct(cfg.S3Config)
	default:
		err = errors.New("unsupported storage background")
	}
	if err != nil {
		return
	}
	return
}
