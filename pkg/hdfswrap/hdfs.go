package hdfswrap

import (
	"context"
	"os"

	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/glog"
	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
)

type HdfsClient struct {
	client *hdfs.Client
}

func NewHadoopFromNameNodes(ctx context.Context, hdfsServer string, username string) (*HdfsClient, error) {
	//nameNodesAddr := strings.Split(hdfsServer, ",")
	//options := hdfs.ClientOptions{
	//	Addresses:           nameNodesAddr,
	//	User:                username,
	//	UseDatanodeHostname: false,
	//}
	//client, err := hdfs.NewClient(options)
	//if err != nil {
	//	return nil, err
	//}
	client, err := hdfs.New(hdfsServer)
	if err != nil {
		return nil, err
	}
	return &HdfsClient{client: client}, nil
}

func NewHadoopClientFromEnv(ctx context.Context, username string) (*HdfsClient, error) {
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil || conf == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	return newHadoopClientFromConf(ctx, conf, username)
}

func NewHadoopClientFromConfMap(ctx context.Context, confMap map[string]string, username string) (*HdfsClient, error) {
	conf := hadoopconf.HadoopConf{}
	for k, v := range confMap {
		conf[k] = v
	}
	return newHadoopClientFromConf(ctx, conf, username)
}

func NewHadoopClientFromConfFile(ctx context.Context, confPath string, username string) (*HdfsClient, error) {
	conf, err := hadoopconf.Load(confPath)
	if err != nil || conf == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	return newHadoopClientFromConf(ctx, conf, username)
}

func newHadoopClientFromConf(ctx context.Context, conf hadoopconf.HadoopConf, username string) (*HdfsClient, error) {
	lg := glog.FromContext(ctx)

	lg.Info().Msg("hdfs creating new client").Fire()

	for k, v := range conf {
		lg.Info().Msg("hadoop config: ").String(k, v).Fire()
	}
	options := hdfs.ClientOptionsFromConf(conf)
	lg.Info().Any("hdfs client options", options).Fire()

	if options.Addresses == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	if options.KerberosClient != nil {
		//TODO future load kerberos file
		options.User = username
	} else {
		options.User = username
	}
	client, err := hdfs.NewClient(options)
	if err != nil || client == nil {
		lg.Error().Error("hdfs create client failed", err).Fire()
		return nil, err
	}
	hadoopClient := HdfsClient{client: client}
	return &hadoopClient, nil
}

func (hc *HdfsClient) Close() error {
	return hc.client.Close()
}

func (hc *HdfsClient) CreateFileWriter(ctx context.Context, path string) (*hdfs.FileWriter, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: create new file for write").String("path", path).Fire()
	writer, err := hc.client.Create(path)
	if err != nil {
		lg.Error().Msg("hdfs: create new file failed").Error("error", err).Fire()
		return nil, err
	}
	return writer, nil
}

func (hc *HdfsClient) OpenFileReader(ctx context.Context, path string) (*hdfs.FileReader, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: open file for read").String("path", path).Fire()
	reader, err := hc.client.Open(path)
	if err != nil {
		lg.Error().Msg("hdfs: open file failed").Error("error", err).Fire()
		return nil, err
	}
	return reader, nil
}

func (hc *HdfsClient) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: stat file").String("path", path).Fire()
	fileInfo, err := hc.client.Stat(path)
	if err != nil {
		lg.Error().Msg("hdfs: stat file failed").Error("error", err).Fire()
		return nil, err
	}
	return fileInfo, nil
}

func (hc *HdfsClient) MkdirAll(ctx context.Context, dirPath string, perm os.FileMode) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: create directory recursively").
		String("path", dirPath).
		Uint32("perm", uint32(perm)).Fire()
	err := hc.client.MkdirAll(dirPath, perm)
	if err != nil {
		lg.Debug().Msg("hdfs: create create directory recursively failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (hc *HdfsClient) Remove(ctx context.Context, path string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: remove file").String("path", path).Fire()
	err := hc.client.Remove(path)
	if err != nil {
		lg.Debug().Msg("hdfs: remove file failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (hc *HdfsClient) Rename(ctx context.Context, oldName string, newName string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: rename file").String("old", oldName).String("new", newName).Fire()
	err := hc.client.Rename(oldName, newName)
	if err != nil {
		lg.Debug().Msg("hdfs: rename file failed").Error("error", err).Fire()
		return err
	}
	return nil
}
