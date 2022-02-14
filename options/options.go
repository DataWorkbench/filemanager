package options

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/resourcemanager/config"
	"github.com/DataWorkbench/resourcemanager/pkg/hdfswrap"
)

var EmptyRPCReply = &pbmodel.EmptyStruct{}

var Config *config.Config

var HDFSClient *hdfswrap.HdfsClient

func Init(ctx context.Context, cfg *config.Config) (err error) {
	Config = cfg

	// Set grpc logger.
	grpcwrap.SetLogger(glog.FromContext(ctx), cfg.GRPCLog)

	// create hdfs client.
	HDFSClient, err = hdfswrap.NewHadoopClientFromConfFile(ctx, cfg.HadoopConfDir, "root")
	if err != nil {
		return
	}

	return
}

func Close() (err error) {
	_ = HDFSClient.Close()
	return
}
