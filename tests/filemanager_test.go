package tests

import (
	"context"
	"fmt"
	"io"
	"os"
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
	ids       []string
)

const (
	spaceID = "wks-0123456789012345"
	tpl     = `<html>
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

func init() {
	if initDone == true {
		return
	}

	address := "127.0.0.1:56001"
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

//func TestUpload(t *testing.T) {
//	var uploadRequest *fmpb.UploadRequest
//	mainInit(t)
//	index := func(w http.ResponseWriter, r *http.Request) {
//		w.Write([]byte(tpl))
//	}
//	upload := func(w http.ResponseWriter, r *http.Request) {
//		err := r.ParseMultipartForm(32 << 20)
//		require.Nil(t, err, "%+v", err)
//		file, _, err := r.FormFile("uploadfile")
//		filename := r.FormValue("filename")
//		filetype := r.FormValue("type")
//		dir := r.FormValue("dir")
//		defer file.Close()
//		require.Nil(t, err, "%+v", err)
//		reader := io.Reader(file)
//		buf := make([]byte, 4096)
//		var n int
//		var init bool
//		id, err := generator.Take()
//		stream, err := client.UploadStream(ctx)
//		require.Nil(t, err, "%+v", err)
//
//		for {
//			n, err = reader.Read(buf)
//			if err != io.EOF {
//				require.Nil(t, err, "%+v", err)
//			}
//			if n == 0 {
//				break
//			}
//
//			var i int
//			i, err = strconv.Atoi(filetype)
//			require.Nil(t, err, "%+v", err)
//			if !init {
//				uploadRequest = &fmpb.UploadRequest{
//					ID:          id,
//					SpaceID:     spaceID,
//					Data:        buf[:n],
//					FileName:    filename,
//					FileDir:     dir,
//					FileType:    int32(i),
//				}
//			} else {
//				uploadRequest = &fmpb.UploadRequest{
//					Data: buf[:n],
//				}
//			}
//			err = stream.Send(uploadRequest)
//			require.Nil(t, err, "%+v", err)
//		}
//		recv, err := stream.CloseAndRecv()
//		require.Nil(t, err, "%+v", err)
//		fmt.Println(recv.String())
//		return
//	}
//	http.HandleFunc("/", index)
//	http.HandleFunc("/upload2", upload)
//	err := http.ListenAndServe(":8888", nil)
//	if err == io.EOF {
//		return
//	}
//}

func TestUploadFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing upload")
	fmt.Println("===================================================================")
	uploadRequest := []*fmpb.UploadFileRequest{
		{SpaceID: spaceID, FileType: 1, FileName: "window01.jar", FilePath: "/jar/demo"},
		{SpaceID: spaceID, FileType: 1, FileName: "window02.jar", FilePath: "/jar/demo"},
		{SpaceID: spaceID, FileType: 1, FileName: "window03.jar", FilePath: "/jar/demo/x"},
		{SpaceID: spaceID, FileType: 1, FileName: "window01.jar", FilePath: ""},
		{SpaceID: spaceID, FileType: 1, FileName: "window02.jar", FilePath: "/xxx/abc/ex"},
		{SpaceID: spaceID, FileType: 1, FileName: "window03.jar", FilePath: "/"},
		{SpaceID: spaceID, FileType: 2, FileName: "udf01.jar", FilePath: "/udf/demo"},
	}

	for index, v := range uploadRequest {
		var file *os.File
		var err error
		if index != len(uploadRequest)-1 {
			file, err = os.Open("../resources/WindowJoin.jar")
		} else {
			file, err = os.Open("../resources/udf.jar")
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
		rsp, err := stream.CloseAndRecv()
		require.Nil(t, err, "%+v", err)
		ids = append(ids, rsp.ID)
	}
}

func TestDownloadFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing download")
	fmt.Println("===================================================================")

	var downloadRequest []fmpb.DownloadRequest
	for _, id := range ids {
		downloadRequest = append(downloadRequest, fmpb.DownloadRequest{ID: id})
	}
	var (
		stream fmpb.FileManager_DownloadFileClient
		recv   *fmpb.DownloadResponse
	)
	for index, v := range downloadRequest {
		f, err := os.Create(fmt.Sprintf("../download/w%d.jar", index))
		require.Nil(t, err, "%+v", err)
		stream, err = client.DownloadFile(ctx, &v)
		require.Nil(t, err, "%+v", err)
		for {
			recv, err = stream.Recv()
			if err == io.EOF || recv == nil {
				err = nil
				break
			}
			require.Nil(t, err, "%+v", err)
			_, err = f.Write(recv.Data)
			require.Nil(t, err, "%+v", err)
		}
		_ = f.Close()
	}
}

func TestListFiles(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing get file list")
	fmt.Println("===================================================================")
	getFileRequests := []*fmpb.FilesFilterRequest{
		{ID: ids[0], SpaceID: spaceID},
		{SpaceID: spaceID},
		{SpaceID: spaceID, Name: "ud"},
		{SpaceID: spaceID, Name: "wind"},
		{SpaceID: spaceID, Name: "wind", Path: "/jar/demo/"},
		{SpaceID: spaceID, Type: 2},
	}
	for _, v := range getFileRequests {
		_, err := client.ListFiles(ctx, v)
		require.Nil(t, err, "%+v", err)
	}

}

func TestUpdateFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing updating file")
	fmt.Println("===================================================================")
	var testUpdateRequests []*fmpb.UpdateFileRequest
	for index, id := range ids {
		testUpdateRequests = append(testUpdateRequests, &fmpb.UpdateFileRequest{
			ID:   id,
			Name: fmt.Sprintf("window%d.jar", index),
			Type: 2,
			Path: fmt.Sprintf("/jar/demo%d/abc/", index),
		})
	}
	for _, v := range testUpdateRequests {
		_, err := client.UpdateFile(ctx, v)
		require.Nil(t, err, "%+v", err)

	}
}

//func TestDeleteFile(t *testing.T) {
//	fmt.Println("===================================================================")
//	fmt.Println("testing deleting file")
//	fmt.Println("===================================================================")
//	var testDeleteRequests []*fmpb.DeleteFileRequest
//	for _, id := range ids {
//		testDeleteRequests = append(testDeleteRequests, &fmpb.DeleteFileRequest{
//			ID:      id,
//			SpaceID: spaceID,
//		})
//	}
//	for _, r := range testDeleteRequests {
//		_, err := client.DeleteFile(ctx, r)
//		require.Nil(t, err, "%+v", err)
//	}
//}
//
//func TestDeleteAll(t *testing.T) {
//	fmt.Println("===================================================================")
//	fmt.Println("testing deleting all")
//	fmt.Println("===================================================================")
//	deleteAllRequest := &fmpb.DeleteFileRequest{SpaceID: spaceID}
//	_, err := client.DeleteFile(ctx, deleteAllRequest)
//	require.Nil(t, err, "%+v", err)
//}
