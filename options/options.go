package options

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/resourcemanager/config"
	"github.com/DataWorkbench/resourcemanager/pkg/fileio"
)

var EmptyRPCReply = &pbmodel.EmptyStruct{}

var (
	Config *config.Config
	FiloIO fileio.FileIO
)

func Init(ctx context.Context, cfg *config.Config) (err error) {
	Config = cfg
	_ = Config

	// Set grpc logger.
	grpcwrap.SetLogger(glog.FromContext(ctx), cfg.GRPCLog)

	switch cfg.Storage.Background {
	case config.StorageBackgroundHDFS:
		FiloIO, err = fileio.NewHadoopClientFromConfFile(ctx, cfg.Storage.HadoopConfDir, "root")
	case config.StorageBackgroundS3:
		FiloIO, err = fileio.NewS3Client(ctx, cfg.Storage.S3)
	}
	if err != nil {
		return
	}
	return
}

func Close() (err error) {
	if FiloIO != nil {
		_ = FiloIO.Close()
	}
	return
}
