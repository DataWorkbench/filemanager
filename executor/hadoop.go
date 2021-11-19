package executor

import (
	"github.com/DataWorkbench/common/qerror"
	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"os"
)

type HadoopClient struct {
	client *hdfs.Client
}

func NewHadoopClientFromEnv(username string) (*HadoopClient, error) {
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil || conf == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	return newHadoopClientFromConf(conf, username)
}

func NewHadoopClientFromConfFile(confPath string, username string) (*HadoopClient, error) {
	conf, err := hadoopconf.Load(confPath)
	if err != nil || conf == nil {
		return nil, qerror.HadoopClientCreateFailed
	}
	return newHadoopClientFromConf(conf, username)
}

func NewHadoopClientFromConfMap(confMap map[string]string, username string) (*HadoopClient, error) {
	conf := hadoopconf.HadoopConf{}
	for k, v := range confMap {
		conf[k] = v
	}
	return newHadoopClientFromConf(conf, username)
}

func newHadoopClientFromConf(conf hadoopconf.HadoopConf, username string) (*HadoopClient, error) {
	options := hdfs.ClientOptionsFromConf(conf)
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
	hadoopClient := HadoopClient{client}
	return &hadoopClient, err
}

func (hc *HadoopClient) createFileWriter(path string) (*hdfs.FileWriter, error) {
	return hc.client.Create(path)
}

func (hc *HadoopClient) openFileReader(filepath string) (*hdfs.FileReader, error) {
	return hc.client.Open(filepath)
}

func (hc *HadoopClient) mkdirP(dirPath string, perm os.FileMode) error {
	return hc.client.MkdirAll(dirPath, perm)
}

func (hc *HadoopClient) remove(path string) error {
	return hc.client.Remove(path)
}

func (hc *HadoopClient) close() error {
	return hc.client.Close()
}
