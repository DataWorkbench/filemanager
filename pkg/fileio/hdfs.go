package fileio

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"

	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/glog"
	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
)

var _ FileIO = (*HDFS)(nil)

type HDFS struct {
	client *hdfs.Client
}

func NewHadoopClientFromEnv(ctx context.Context, username string) (*HDFS, error) {
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil || conf == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	return newHadoopClientFromConf(ctx, conf, username)
}

func NewHadoopClientFromConfMap(ctx context.Context, confMap map[string]string, username string) (*HDFS, error) {
	conf := hadoopconf.HadoopConf{}
	for k, v := range confMap {
		conf[k] = v
	}
	return newHadoopClientFromConf(ctx, conf, username)
}

func NewHadoopClientFromConfFile(ctx context.Context, confPath string, username string) (*HDFS, error) {
	conf, err := hadoopconf.Load(confPath)
	if err != nil || conf == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	return newHadoopClientFromConf(ctx, conf, username)
}

func newHadoopClientFromConf(ctx context.Context, conf hadoopconf.HadoopConf, username string) (*HDFS, error) {
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
	hadoopClient := HDFS{client: client}
	return &hadoopClient, nil
}

func (hd *HDFS) Close() error {
	if hd.client != nil {
		return hd.client.Close()
	}
	return nil
}

func (hd *HDFS) MkdirAll(ctx context.Context, dirname string, perm os.FileMode) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: create directory recursively").
		String("dirname", dirname).
		Uint32("perm", uint32(perm)).Fire()
	err := hd.client.MkdirAll(dirname, perm)
	if err != nil {
		lg.Error().Msg("hdfs: create create directory recursively failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (hd *HDFS) IsExists(ctx context.Context, name string) (bool, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: check file is exists").String("name", name).Fire()
	_, err := hd.client.Stat(name)
	if err != nil {
		lg.Warn().Msg("hdfs: stat file failed").Error("error", err).Fire()
		if _, ok := err.(*os.PathError); ok {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func (hd *HDFS) CreateAndWrite(ctx context.Context, name string, reader io.ReadCloser) (string, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: create new file for write").String("name", name).Fire()
	writer, err := hd.client.Create(name)
	if err != nil {
		lg.Error().Msg("hdfs: create new file failed").Error("error", err).Fire()
		return "", err
	}

	h := md5.New()
	buf := make([]byte, 4096)
	var position int

LOOP:
	for {
		position, err = reader.Read(buf)
		if err == io.EOF && position == 0 {
			err = nil
			break LOOP
		}
		if err != nil {
			break LOOP
		}

		data := buf[:position]
		// Update md5 sum.
		h.Write(data)
		// write data to hdfs.
		_, err = writer.Write(data)
		if err != nil {
			break LOOP
		}
	}
	if err != nil {
		return "", nil
	}
	if err = writer.Flush(); err != nil {
		return "", nil
	}
	if err = writer.Close(); err != nil {
		return "", nil
	}

	// Calculate the md5 as hex.
	b := h.Sum(nil)
	md5Hex := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(md5Hex, b)
	eTag := string(md5Hex)
	return eTag, nil
}

func (hd *HDFS) OpenForRead(ctx context.Context, name string) (io.ReadCloser, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: open file for read").String("name", name).Fire()
	reader, err := hd.client.Open(name)
	if err != nil {
		lg.Error().Msg("hdfs: open file failed").Error("error", err).Fire()
		return nil, err
	}
	return reader, nil
}

func (hd *HDFS) Remove(ctx context.Context, name string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: remove file").String("name", name).Fire()
	err := hd.client.Remove(name)
	if err != nil {
		lg.Error().Msg("hdfs: remove file failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (hd *HDFS) RemoveAll(ctx context.Context, name string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: remove dir and all children").String("name", name).Fire()
	err := hd.client.RemoveAll(name)
	if err != nil {
		lg.Error().Msg("hdfs: remove dir and all children failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (hd *HDFS) Rename(ctx context.Context, oldName string, newName string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("hdfs: rename file").String("old", oldName).String("new", newName).Fire()
	err := hd.client.Rename(oldName, newName)
	if err != nil {
		lg.Error().Msg("hdfs: rename file failed").Error("error", err).Fire()
		return err
	}
	return nil
}
