package tests

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
)

const (
	spaceId   = "wks-0123456789012345"
	spaceId2  = "wks-0123456789012346"
	spaceId3  = "wks-0123456789012347"
	spaceId4  = "wks-0123456789012348"
	dirId     = "res-04bbca8755d62130"
	jarId     = "res-04bbca8755d62131"
	udfId     = "res-04bbca8755d62132"
	deleteId1 = "res-04bbca8755d62133"
	deleteId2 = "res-04bbca8755d62134"
	deleteId3 = "res-04bbca8755d62135"
	deleteId4 = "res-04bbca8755d62136"
	deleteId5 = "res-04bbca8755d62137"
)

var (
	client    fmpb.FileManagerClient
	ctx       context.Context
	initDone  bool
	generator *idgenerator.IDGenerator
	deleteIds = []string{deleteId4, deleteId5}
	deleteDir = ""
	spaceIds  = []string{spaceId2, spaceId3}
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
	client = fmpb.NewFileManagerClient(conn)
	logger := glog.NewDefault()
	generator = idgenerator.New(constants.FileMangerIDPrefix)
	reqId, _ := generator.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)
	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
}

func Test_UploadFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing upload")
	fmt.Println("===================================================================")
	uploadRequest := []*fmpb.UploadFileRequest{
		{SpaceId: spaceId2, FileType: 1, FileName: "/jar/demo/window01.jar"},
		{SpaceId: spaceId2, FileType: 1, FileName: "jar/demo/window02.jar"},
		{SpaceId: spaceId2, FileType: 1, FileName: "/jar/demo/x/window03.jar"},
		{SpaceId: spaceId2, FileType: 1, FileName: "window01.jar"},
		{SpaceId: spaceId2, FileType: 1, FileName: "/window02.jar"},
		{SpaceId: spaceId3, FileType: 1, FileName: "/jar/demo/window01.jar"},
		{SpaceId: spaceId3, FileType: 1, FileName: "jar/demo/window02.jar"},
		{SpaceId: spaceId3, FileType: 1, FileName: "/jar/demo/x/window03.jar"},
		{SpaceId: spaceId3, FileType: 1, FileName: "window01.jar"},
		{SpaceId: spaceId3, FileType: 1, FileName: "/xxx/abc/ex/window02.jar"},
		{FileId: deleteId1, SpaceId: spaceId4, FileType: 1, FileName: "/jar/delete01.jar"},
		{FileId: deleteId2, SpaceId: spaceId4, FileType: 1, FileName: "/jar/demo/delete02.jar"},
		{FileId: deleteId3, SpaceId: spaceId4, FileType: 1, FileName: "/jar/demo/x/delete03.jar"},
		{FileId: deleteId4, SpaceId: spaceId3, FileType: 1, FileName: "delete01.jar"},
		{FileId: deleteId5, SpaceId: spaceId3, FileType: 1, FileName: "xxx/abc/ex/delete02.jar"},
		{FileId: jarId, SpaceId: spaceId, FileType: 2, FileName: "/jar.jar"},
		{FileId: udfId, SpaceId: spaceId, FileType: 1, FileName: "udf.jar"},
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
		reader := io.Reader(file)
		buf := make([]byte, 4096)
		var n int
		stream, err := client.UploadFile(ctx)
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

func Test_DownloadFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing download")
	fmt.Println("===================================================================")
	var (
		stream fmpb.FileManager_DownloadFileClient
		recv   *fmpb.DownloadResponse
	)

	f, err := os.Create(fmt.Sprintf("../resources/test_download.jar"))
	require.Nil(t, err, "%+v", err)
	stream, err = client.DownloadFile(ctx, &fmpb.DownloadRequest{FileId: jarId})
	require.Nil(t, err, "%+v", err)
	for {
		recv, err = stream.Recv()
		if err == io.EOF {
			break
		}
		require.Nil(t, err, "%+v", err)
		_, err = f.Write(recv.Data)
		require.Nil(t, err, "%+v", err)
	}
	_ = f.Close()

}

func Test_DescribeFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing describe")
	fmt.Println("===================================================================")
	_, err := client.DescribeFile(ctx, &fmpb.DescribeRequest{FileId: jarId})
	require.Nil(t, err, "%+v", err)
}

func Test_CreateDir(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing create dir")
	fmt.Println("===================================================================")
	_, err := client.CreateDir(ctx, &fmpb.CreateDirRequest{
		FileId:  dirId,
		SpaceId: spaceId4,
		DirName: "/jar/demo",
	})
	require.Nil(t, err, "%+v", err)
}

func Test_ListFilesByDir(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing list files by dir")
	fmt.Println("===================================================================")
	_, err := client.ListFilesByDir(ctx, &fmpb.ListByDirRequest{
		SpaceId:  spaceId4,
		Limit:    10,
		Offset:   0,
		DirName:  "/jar/demo/",
		FileType: 1,
	})
	require.Nil(t, err, "%+v", err)
}

func Test_DeleteDir(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing delete dir")
	fmt.Println("===================================================================")
	_, err := client.DeleteDir(ctx, &fmpb.DeleteDirRequest{FileId: dirId})
	require.Nil(t, err, "%+v", err)
}

func Test_ListFiles(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing get file list")
	fmt.Println("===================================================================")
	getFileRequests := []*fmpb.ListRequest{
		{SpaceId: spaceId2, Limit: 3, Offset: 2, FileType: 1},
	}
	for _, v := range getFileRequests {
		_, err := client.ListFiles(ctx, v)
		require.Nil(t, err, "%+v", err)
	}
}

func Test_DeleteFilesByIds(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing deleting file")
	fmt.Println("===================================================================")
	testDeleteRequests := &fmpb.DeleteFilesRequest{
		Ids: deleteIds,
	}
	_, err := client.DeleteFiles(ctx, testDeleteRequests)
	require.Nil(t, err, "%+v", err)
}

func Test_DeleteFilesBySpaceIds(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing deleting all")
	fmt.Println("===================================================================")
	deleteAllRequest := &fmpb.DeleteAllFilesRequest{SpaceIds: spaceIds}
	_, err := client.DeleteAllFiles(ctx, deleteAllRequest)
	require.Nil(t, err, "%+v", err)
}

func Test_UpdateFile(t *testing.T) {
	time.Sleep(time.Second * 2)
	fmt.Println("===================================================================")
	fmt.Println("testing updating file")
	fmt.Println("===================================================================")
	var testUpdateRequests []*fmpb.UpdateFileRequest
	testUpdateRequests = append(testUpdateRequests, &fmpb.UpdateFileRequest{
		FileId:   jarId,
		FileName: "test/jar/demo/test_jar.jar",
		FileType: 1,
	})
	testUpdateRequests = append(testUpdateRequests, &fmpb.UpdateFileRequest{
		FileId:   udfId,
		FileName: "/test/udf/demo/test_udf.jar",
		FileType: 2,
	})
	for _, v := range testUpdateRequests {
		_, err := client.UpdateFile(ctx, v)
		require.Nil(t, err, "%+v", err)
	}
}
