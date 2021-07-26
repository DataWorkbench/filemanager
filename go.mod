module github.com/DataWorkbench/filemanager

go 1.15

require (
	github.com/DataWorkbench/common v0.0.0-20210623105008-a86377741286
	github.com/DataWorkbench/glog v0.0.0-20201114060240-9471edb2b8cf
	github.com/DataWorkbench/gproto v0.0.0-20210623105316-eaab7b81b170
	github.com/DataWorkbench/loader v0.0.0-20201119073611-6f210eb11a8c
	github.com/colinmarc/hdfs v1.1.3
	github.com/go-playground/validator/v10 v10.4.1
	github.com/spf13/cobra v1.1.1
	github.com/stretchr/testify v1.6.1
	google.golang.org/grpc v1.34.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	gorm.io/gorm v1.20.7
)

replace (
	github.com/DataWorkbench/common v0.0.0-20210623105008-a86377741286 => /Users/gxlevi/go/src/github.com/DataWorkbench/common
	github.com/DataWorkbench/gproto v0.0.0-20210623105316-eaab7b81b170 => /Users/gxlevi/go/src/github.com/DataWorkbench/gproto
)
