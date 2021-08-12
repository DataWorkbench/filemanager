package server

import (
	"context"
	"github.com/DataWorkbench/filemanager/executor"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
	"github.com/DataWorkbench/gproto/pkg/model"
)

type FileManagerServer struct {
	fmpb.UnimplementedFileManagerServer
	executor   *executor.FileManagerExecutor
	emptyReply *model.EmptyStruct
}

func NewFileManagerServer(executor *executor.FileManagerExecutor) *FileManagerServer {
	return &FileManagerServer{
		executor:   executor,
		emptyReply: &model.EmptyStruct{},
	}
}

// UploadFile @title  文件上传
// @description   上传文件到hadoop文件系统
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     SpaceID        string         "工作空间id"
// @param     FileName       string         "文件名"
// @param     FilePath       string         "文件路径"
// @param     FileType       int32          "文件类型 1 jar包文件 2 udf文件"
// @param     SpaceID        string         "工作空间"
// @param     SpaceID        string         "工作空间"
// @return    ID             string         "文件id"
// @return    SpaceID        string         "工作空间id"
// @return    FileName       string         "文件名"
// @return    FilePath       string         "文件路径"
// @return    FileType       int32          "文件类型 1 jar包文件 2 udf文件"
// @return    HdfsAddress    string         "hadoop地址"
// @return    URL            string         "文件的hadoop地址"
func (fs *FileManagerServer) UploadFile(fu fmpb.FileManager_UploadFileServer) error {
	return fs.executor.UploadFile(fu)
}

// DownloadFile @title  文件下载
// @description   下载hadoop文件到本地
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     ID             string         "文件id"
func (fs *FileManagerServer) DownloadFile(req *fmpb.DownloadRequest, res fmpb.FileManager_DownloadFileServer) error {
	return fs.executor.DownloadFile(req.ID, res)
}

// ListFiles @title  文件下载
// @description   下载hadoop文件到本地
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     ID             string         "根据文件id查询"
// @param     SpaceID        string         "根据spaceId查询列表"
// @param     Name           string         "根据文件名模糊查询"
// @param     Path           string         "匹配路径下的全部"
// @param     Type           int32          "文件类型查询列表"
// @return    Total          int32          "文件总数"
// @return    ID             string         "文件id"
// @return    SpaceID        string         "工作空间id"
// @return    FileName       string         "文件名"
// @return    FilePath       string         "文件路径"
// @return    FileType       int32          "文件类型 1 jar包文件 2 udf文件"
// @return    HdfsAddress    string         "hadoop地址"
// @return    URL            string         "文件的hadoop地址"
func (fs *FileManagerServer) ListFiles(ctx context.Context, req *fmpb.FilesFilterRequest) (*fmpb.FileListResponse, error) {
	return fs.executor.ListFiles(ctx, req.ID, req.SpaceID, req.Name, req.Path, req.Type)
}

// UpdateFile @title  更新文件
// @description   更新文件信息
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     ID             string         "文件id"
// @param     Name           string         "文件名"
// @param     Path           string         "文件路径"
// @param     Type           int32          "文件类型"
func (fs *FileManagerServer) UpdateFile(ctx context.Context, req *fmpb.UpdateFileRequest) (*model.EmptyStruct, error) {
	return fs.executor.UpdateFile(ctx, req.ID, req.Name, req.Path, req.Type)
}

// DeleteFile @title  更新文件
// @description   更新文件信息
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     ID             string         "文件id"
// @param     SpaceID        string         "工作空间id"
func (fs *FileManagerServer) DeleteFile(ctx context.Context, req *fmpb.DeleteFileRequest) (*model.EmptyStruct, error) {
	return fs.executor.DeleteFile(ctx, req.IDS, req.SpaceID)
}
