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

func TestUpload(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing upload")
	fmt.Println("===================================================================")
	uploadRequest := []*fmpb.UploadRequest{
		{SpaceID: spaceID, FileType: 1, FileName: "window01.jar", FileDir: "/jar/demo"},
		{SpaceID: spaceID, FileType: 1, FileName: "window02.jar", FileDir: "/jar/demo"},
		{SpaceID: spaceID, FileType: 1, FileName: "window03.jar", FileDir: "/jar/demo/x"},
		{SpaceID: spaceID, FileType: 1, FileName: "window01.jar", FileDir: ""},
		{SpaceID: spaceID, FileType: 1, FileName: "window02.jar", FileDir: "/xxx/abc/ex"},
		{SpaceID: spaceID, FileType: 1, FileName: "window03.jar", FileDir: "/"},
		{SpaceID: spaceID, FileType: 2, FileName: "udf01.jar", FileDir: "/udf/demo"},
	}

	for index, v := range uploadRequest {
		var file *os.File
		var err error
		if index != len(uploadRequest)-1 {
			file, err = os.Open("../resources/WindowJoin.jar")
		} else {
			file, err = os.Open("../resources/udf.jar")
		}
		defer file.Close()
		require.Nil(t, err, "%+v", err)
		reader := io.Reader(file)
		buf := make([]byte, 4096)
		var n int
		stream, err := client.UploadStream(ctx)
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

func TestDownload(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing download")
	fmt.Println("===================================================================")

	var testDownloadRequest = []fmpb.DownloadRequest{
		{ID: ids[0]},
	}
	var (
		stream fmpb.FileManager_DownloadStreamClient
		recv   *fmpb.DownloadReply
	)
	for _, v := range testDownloadRequest {
		f, err := os.Create("../resources/w.jar")
		require.Nil(t, err, "%+v", err)
		stream, err = client.DownloadStream(ctx, &v)
		require.Nil(t, err, "%+v", err)
		for {
			recv, err = stream.Recv()
			if err == io.EOF {
				break
			}
			require.Nil(t, err, "%+v", err)
			if recv == nil {
				break
			}
			_, err = f.Write(recv.Data)
			require.Nil(t, err, "%+v", err)
		}
		_ = f.Close()
	}
}

func TestGetFileList(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing get file list")
	fmt.Println("===================================================================")
	var testListRequest = fmpb.GetDirListRequest{
		SpaceID: spaceID,
	}

	_, err := client.GetDirList(ctx, &testListRequest)
	require.Nil(t, err, "%+v", err)

}

func TestGetFileById(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing get file by id")
	fmt.Println("===================================================================")

	var testGetFileRequest []fmpb.IdRequest
	for _, v := range ids {
		testGetFileRequest = append(testGetFileRequest, fmpb.IdRequest{ID: v})
	}
	for _, v := range testGetFileRequest {
		_, err := client.GetFileById(ctx, &v)
		require.Nil(t, err, "%+v", err)
	}
}

//func TestGetSubDirFile(t *testing.T) {
//	fmt.Println("===================================================================")
//	fmt.Println("testing get sub dir file by id")
//	fmt.Println("===================================================================")
//
//	testGetSubFileDirRequest := fmpb.GetSubDirListRequest{ID: "file-04a73171ff1fc002"}
//	_, err := client.GetSubDirFile(ctx, &testGetSubFileDirRequest)
//	require.Nil(t, err, "%+v", err)
//}

func TestUpdateFile(t *testing.T) {
	fmt.Println("===================================================================")
	fmt.Println("testing updating file")
	fmt.Println("===================================================================")
	var testUpdateFileRequest []*fmpb.UpdateFileRequest
	for index, id := range ids {
		testUpdateFileRequest = append(testUpdateFileRequest, &fmpb.UpdateFileRequest{
			ID:   id,
			Name: fmt.Sprintf("window%d.jar", index),
			Type: 2,
			Path: fmt.Sprintf("/jar/demo%d/abc/", index),
		})
	}
	for _, v := range testUpdateFileRequest {
		_, err := client.UpdateFile(ctx, v)
		require.Nil(t, err, "%+v", err)

	}
}

func TestDeleteDir(t *testing.T) {
	var testDeleteRequest = []fmpb.DeleteRequest{
		{ID: "file-04a77744581fc000"},
	}
	for _, v := range testDeleteRequest {
		_, err := client.DeleteDirById(ctx, &v)
		require.Nil(t, err, "%+v", err)
	}
}
