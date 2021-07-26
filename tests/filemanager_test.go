package tests

import (
	"context"
	"fmt"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
)

var (
	client    fmpb.FileManagerClient
	ctx       context.Context
	initDone  bool
	generator *idgenerator.IDGenerator
)

const (
	spaceID     = "wks-0123456789012345"
	tpl         = `<html>
<head>
<title>上传文件</title>
</head>
<body>
<form enctype="multipart/form-data" action="/upload2" method="post">
<input type="file" name="uploadfile">
<input type="text" placeholder="输入文件名" name = "filename">
<input type="text" placeholder="输入目录" name = "dir">
<input type="text" placeholder="输入文件类型" name = "type">
<input type="hidden" name="token" value="{...{.}...}">
<input type="submit" value="upload">
</form>
</body>
</html>`
)

func mainInit(t *testing.T) {
	if initDone == true {
		return
	}

	address := "127.0.0.1:56001"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)
	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      address,
	})
	require.Nil(t, err, "%+v", err)
	client = fmpb.NewFileManagerClient(conn)
	logger := glog.NewDefault()
	generator = idgenerator.New(constants.FileMangerIDPrefix)
	reqId, _ := generator.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)

	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
}

func TestUpload(t *testing.T) {
	var uploadRequest *fmpb.UploadRequest
	mainInit(t)
	index := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(tpl))
	}
	upload := func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(32 << 20)
		require.Nil(t, err, "%+v", err)
		file, _, err := r.FormFile("uploadfile")
		filename := r.FormValue("filename")
		filetype := r.FormValue("type")
		dir := r.FormValue("dir")
		defer file.Close()
		require.Nil(t, err, "%+v", err)
		reader := io.Reader(file)
		buf := make([]byte, 4096)
		var n int
		var init bool
		id, err := generator.Take()
		stream, err := client.UploadStream(ctx)
		require.Nil(t, err, "%+v", err)

		for {
			n, err = reader.Read(buf)
			if err != io.EOF {
				require.Nil(t, err, "%+v", err)
			}
			if n == 0 {
				break
			}

			var i int
			i, err = strconv.Atoi(filetype)
			require.Nil(t, err, "%+v", err)
			if !init {
				uploadRequest = &fmpb.UploadRequest{
					ID:          id,
					SpaceID:     spaceID,
					Data:        buf[:n],
					FileName:    filename,
					FileDir:     dir,
					FileType:    int32(i),
				}
			} else {
				uploadRequest = &fmpb.UploadRequest{
					Data: buf[:n],
				}
			}
			err = stream.Send(uploadRequest)
			require.Nil(t, err, "%+v", err)
		}
		recv, err := stream.CloseAndRecv()
		require.Nil(t, err, "%+v", err)
		fmt.Println(recv.String())
		return
	}
	http.HandleFunc("/", index)
	http.HandleFunc("/upload2", upload)
	err := http.ListenAndServe(":8888", nil)
	if err == io.EOF {
		return
	}
}

func TestDownload(t *testing.T) {
	mainInit(t)
	f, err := os.Create("../resources/w.jar")
	require.Nil(t, err, "%+v", err)
	defer f.Close()
	var testDownloadRequest = []fmpb.DownloadRequest{
		{ID: "file-049d26a7de129000"},
	}
	for _, v := range testDownloadRequest {
		stream, err := client.DownloadStream(ctx, &v)
		require.Nil(t, err, "%+v", err)
		for {
			recv, err := stream.Recv()
			if io.EOF == err {
				if fromError, ok := status.FromError(err); ok {
					fmt.Println(fromError.Code(), fromError.Message())
				} else {
					require.Nil(t, err, "%+v", err)
				}
				break
			}
			if recv == nil {
				break
			}
			_, err = f.Write(recv.Data)
			require.Nil(t, err, "%+v", err)
		}
	}
}

func TestListDir(t *testing.T) {
	var testListRequest = []fmpb.GetDirListRequest{
		{SpaceID: spaceID},
	}
	mainInit(t)
	for _, v1 := range testListRequest {
		info, err := client.GetDirList(ctx, &v1)
		require.Nil(t, err, "%+v", err)
		fmt.Println("===============================")
		fmt.Println(info)
		fmt.Println("===============================")
	}
}

func TestDeleteFile(t *testing.T) {
	mainInit(t)
	var testDeleteRequest = []fmpb.DeleteRequest{
		{ID: "file-04a5c266f8dfc000"},
	}
	for _, v := range testDeleteRequest {
		_, err := client.DeleteFileById(ctx, &v)
		require.Nil(t, err, "%+v", err)
	}
}

func TestDeleteDir(t *testing.T) {
	mainInit(t)
	var testDeleteRequest = []fmpb.DeleteRequest{
		{ID: "file-04a5c2333a1fc000"},
	}
	for _, v := range testDeleteRequest {
		_, err := client.DeleteDirById(ctx, &v)
		require.Nil(t, err, "%+v", err)
	}
}

func TestGetFileById(t *testing.T) {
	mainInit(t)
	var testGetFileRequest = []fmpb.IdRequest{
		{ID: "file-04a5c266f8dfc000"},
	}
	for _, v := range testGetFileRequest {
		vs, err := client.GetFileById(ctx, &v)
		require.Nil(t, err, "%+v", err)
		fmt.Println(vs.Name, vs.URL, vs.FileType,vs.Path)
	}
}