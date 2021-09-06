package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/respb"

	"github.com/DataWorkbench/glog"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
)

const (
	spaceId  = "wks-0123456789012345"
	spaceId2 = "wks-0123456789012346"
)

var (
	client         respb.ResourceClient
	ctx            context.Context
	initDone       bool
	generator      *idgenerator.IDGenerator
	createDirResId string
)

func init() {
	if initDone == true {
		return
	}

	address := "127.0.0.1:9111"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)
	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address: address,
	})
	if err != nil {
		panic(err)
	}
	client = respb.NewResourceClient(conn)
	logger := glog.NewDefault()
	generator = idgenerator.New(constants.FileMangerIDPrefix)
	reqId, _ := generator.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)
	ctx = glog.WithContext(ctx, ln)
}

func Test_CreateDir(t *testing.T) {
	var req = request.CreateDirectory{
		SpaceId: spaceId,
		DirName: "demo",
	}
	dir, err := client.CreateDir(ctx, &req)
	require.Nil(t, err, "%+v", err)
	createDirResId = dir.Id
}

func Test_Update(t *testing.T) {
	var req = request.UpdateResource{
		ResourceId:   createDirResId,
		ResourceName: "abc",
		ResourceType: 0,
	}
	_, err := client.UpdateResource(ctx, &req)
	require.Nil(t, err, "%+v", err)
}
