package tests

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/gproto/pkg/resource"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
)

const (
	spaceId  = "wks-0123456789012345"
	spaceId2 = "wks-0123456789012346"

	dirId1 = "res-04bbca8755d62127"
	//TODO 2 在1下面
	dirId2 = "res-04bbca8755d62128"
	//TODO 3 在1下面
	dirId3 = "res-04bbca8755d62129"
	//TODO 4 在3下面
	dirId4 = "res-04bbca8755d62130"

	deleteId0  = "res-04bbca8755d62131"
	deleteId00 = "res-04bbca8755d62132"
	deleteId1  = "res-04bbca8755d62133"
	deleteId2  = "res-04bbca8755d62134"
	deleteId3  = "res-04bbca8755d62135"
	deleteId4  = "res-04bbca8755d62136"
	deleteId5  = "res-04bbca8755d62137"
	deleteId6  = "res-04bbca8755d62138"
	deleteId7  = "res-04bbca8755d62139"
	deleteId8  = "res-04bbca8755d62140"

	deleteId9  = "res-04bbca8755d62141"
	deleteId10 = "res-04bbca8755d62142"
	deleteId11 = "res-04bbca8755d62143"

	jarId = "res-04bbca8755d62151"
	udfId = "res-04bbca8755d62152"
)

var (
	client    resource.ResourceManagerClient
	ctx       context.Context
	initDone  bool
	generator *idgenerator.IDGenerator
	deleteIds = []string{dirId1, deleteId00, deleteId0}
	spaceIds  = []string{spaceId2}
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
	client = resource.NewResourceManagerClient(conn)
	logger := glog.NewDefault()
	generator = idgenerator.New(constants.FileMangerIDPrefix)
	reqId, _ := generator.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)
	ctx = glog.WithContext(ctx, ln)
}

func Test_CreateDir(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing create dir")
	fmt.Println("===================================================================")
	createDirRequest := []*resource.CreateDirRequest{
		{ResourceId: dirId1, SpaceId: spaceId, DirName: "doc"},
		{ResourceId: dirId2, SpaceId: spaceId, DirName: "demo2", ParentId: dirId1},
		{ResourceId: dirId3, SpaceId: spaceId, DirName: "demo3", ParentId: dirId1},
		{ResourceId: dirId4, SpaceId: spaceId, DirName: "demo4", ParentId: dirId3},
	}
	for _, v := range createDirRequest {
		_, err := client.CreateDir(ctx, v)
		require.Nil(t, err, "%+v", err)
	}
}

func Test_UploadFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing upload")
	fmt.Println("===================================================================")
	uploadRequest := []*resource.UploadFileRequest{
		//TODO 在根目录下
		{ResourceId: deleteId00, SpaceId: spaceId, ResourceType: 1, ResourceName: "0-window1.jar"},
		{ResourceId: deleteId0, SpaceId: spaceId, ResourceType: 1, ResourceName: "0-window2.jar"},
		//TODO 在1目录下
		{ResourceId: deleteId1, SpaceId: spaceId, ResourceType: 1, ResourceName: "1-window1.jar", ParentId: dirId1},
		{ResourceId: deleteId2, SpaceId: spaceId, ResourceType: 1, ResourceName: "1-window2.jar", ParentId: dirId1},
		//TODO 在2目录下
		{ResourceId: deleteId3, SpaceId: spaceId, ResourceType: 1, ResourceName: "2-window1.jar", ParentId: dirId2},
		{ResourceId: deleteId4, SpaceId: spaceId, ResourceType: 1, ResourceName: "2-window2.jar", ParentId: dirId2},
		//TODO 在3目录下
		{ResourceId: deleteId5, SpaceId: spaceId, ResourceType: 1, ResourceName: "3-window1.jar", ParentId: dirId3},
		{ResourceId: deleteId6, SpaceId: spaceId, ResourceType: 1, ResourceName: "3-window2.jar", ParentId: dirId3},
		//TODO 在4目录下
		{ResourceId: deleteId7, SpaceId: spaceId, ResourceType: 1, ResourceName: "4-window1.jar", ParentId: dirId4},
		{ResourceId: deleteId8, SpaceId: spaceId, ResourceType: 1, ResourceName: "4-window2.jar", ParentId: dirId4},

		{ResourceId: deleteId9, SpaceId: spaceId2, ResourceType: 1, ResourceName: "0-window1.jar"},
		{ResourceId: deleteId10, SpaceId: spaceId2, ResourceType: 1, ResourceName: "0-window2.jar"},
		{ResourceId: deleteId11, SpaceId: spaceId2, ResourceType: 1, ResourceName: "0-window3.jar"},
		//TODO 在根目录下

		{ResourceId: jarId, SpaceId: spaceId, ResourceType: 1, ResourceName: "jar-window1.jar"},
		{ResourceId: udfId, SpaceId: spaceId, ResourceType: 1, ResourceName: "udf-window2.jar"},
	}

	for index, v := range uploadRequest {
		var file *os.File
		var err error
		if index == len(uploadRequest)-1 {
			file, err = os.Open("../resources/udf.jar")
		} else if index == len(uploadRequest)-2 {
			file, err = os.Open("../resources/frauddetection-0.1.jar")
		} else {
			file, err = os.Open("../resources/WindowJoin.jar")
		}
		defer func() {
			_ = file.Close()
		}()
		require.Nil(t, err, "%+v", err)
		stat, err := file.Stat()
		require.Nil(t, err, "%+v", err)
		v.Size = stat.Size()

		reader := io.Reader(file)
		buf := make([]byte, 4096)
		var n int
		stream, err := client.UploadFile(ctx)
		require.Nil(t, err, "%+v", err)
		err = stream.Send(v)
		require.Nil(t, err, "%+v", err)
		for {
			n, err = reader.Read(buf)
			if err == io.EOF {
				break
			}
			require.Nil(t, err, "%+v", err)
			v.Data = buf[:n]
			require.Nil(t, err, "%+v", err)
			err = stream.Send(v)
			require.Nil(t, err, "%+v", err)
		}
		_, err = stream.CloseAndRecv()
		require.Nil(t, err, "%+v", err)
	}
}

//func Test_DownloadFile(t *testing.T) {
//	fmt.Println("===================================================================")
//	fmt.Println("testing download")
//	fmt.Println("===================================================================")
//	var (
//		stream resource.ResourceManager_DownloadFileClient
//		recv   *resource.DownloadResponse
//	)
//
//	f, err := os.Create("../resources/test_download.jar")
//	require.Nil(t, err, "%+v", err)
//	stream, err = client.DownloadFile(ctx, &resource.DownloadRequest{ResourceId: jarId})
//	require.Nil(t, err, "%+v", err)
//	_, err = stream.Recv()
//	require.Nil(t, err,"%+v",err)
//	for {
//		recv, err = stream.Recv()
//		if err == io.EOF {
//			break
//		}
//		require.Nil(t, err, "%+v", err)
//		_, err = f.Write(recv.Data)
//		require.Nil(t, err, "%+v", err)
//	}
//	_ = f.Close()
//
//}

func Test_DescribeFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing describe")
	fmt.Println("===================================================================")
	_, err := client.DescribeFile(ctx, &resource.DescribeRequest{ResourceId: jarId})
	require.Nil(t, err, "%+v", err)
	_, err = client.DescribeFile(ctx, &resource.DescribeRequest{ResourceId: deleteId1})
	require.Nil(t, err, "%+v", err)
}

func Test_ListFiles(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing list files by dir")
	fmt.Println("===================================================================")
	listReqs := []*resource.ListRequest{
		{ResourceId: "-1", SpaceId: spaceId, Limit: 10, Offset: 0, ResourceType: 1},
		{ResourceId: dirId1, SpaceId: spaceId, Limit: 10, Offset: 0, ResourceType: 1},
	}
	for _, v := range listReqs {
		_, err := client.ListFiles(ctx, v)
		require.Nil(t, err, "%+v", err)
	}
}

func Test_DeleteFiles(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing deleting file")
	fmt.Println("===================================================================")
	testDeleteRequests := &resource.DeleteRequest{
		Ids: deleteIds,
	}
	_, err := client.Delete(ctx, testDeleteRequests)
	require.Nil(t, err, "%+v", err)
}

func Test_UpdateFile(t *testing.T) {
	time.Sleep(time.Second * 1)
	fmt.Println("===================================================================")
	fmt.Println("testing updating file")
	fmt.Println("===================================================================")
	var testUpdateRequests []*resource.UpdateFileRequest
	testUpdateRequests = append(testUpdateRequests, &resource.UpdateFileRequest{
		ResourceId:   jarId,
		ResourceName: "test_jar.jar",
		ResourceType: 1,
	})
	testUpdateRequests = append(testUpdateRequests, &resource.UpdateFileRequest{
		ResourceId:   udfId,
		ResourceName: "test_udf.jar",
		ResourceType: 2,
	})
	for _, v := range testUpdateRequests {
		_, err := client.UpdateFile(ctx, v)
		require.Nil(t, err, "%+v", err)
	}
}

func Test_DeleteFilesBySpaceIds(t *testing.T) {
	time.Sleep(time.Second * 1)
	fmt.Println("===================================================================")
	fmt.Println("testing deleting all")
	fmt.Println("===================================================================")
	deleteAllRequest := &resource.DeleteSpaceRequest{SpaceIds: spaceIds}
	_, err := client.DeleteSpace(ctx, deleteAllRequest)
	require.Nil(t, err, "%+v", err)
}
