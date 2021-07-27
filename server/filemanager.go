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

//UploadStream 流式上传文件
func (fs *FileManagerServer) UploadStream(fu fmpb.FileManager_UploadStreamServer) error {
	return fs.executor.UploadStream(fu)
}

//DownloadStream 流式下载文件
func (fs *FileManagerServer) DownloadStream(req *fmpb.DownloadRequest, res fmpb.FileManager_DownloadStreamServer) error {
	return fs.executor.DownloadStream(req.ID, res)
}

//GetDirList 查看文件夹下的子文件夹及文件
func (fs *FileManagerServer) GetDirList(ctx context.Context, req *fmpb.GetDirListRequest) (*fmpb.GetDirListReply, error) {
	return fs.executor.GetDirList(ctx, req.SpaceID)
}

//DeleteFileById 根据id删除文件
func (fs *FileManagerServer) DeleteFileById(ctx context.Context, req *fmpb.DeleteRequest) (*model.EmptyStruct, error) {
	return fs.emptyReply, fs.executor.DeleteFile(ctx, req.ID)
}

//DeleteDirById 根据id删除文件夹
func (fs *FileManagerServer) DeleteDirById(ctx context.Context, req *fmpb.DeleteRequest) (*model.EmptyStruct, error) {
	return fs.emptyReply, fs.executor.DeleteDir(ctx, req.ID)
}

//GetFileById 根据id获取文件
func (fs *FileManagerServer) GetFileById(ctx context.Context, req *fmpb.IdRequest) (*fmpb.FileInfoReply, error) {
	return fs.executor.GetFileById(ctx, req.ID)
}

//GetSubDirFile 根据文件夹id查找其下级文件夹和文件
func (fs *FileManagerServer) GetSubDirFile(ctx context.Context, req *fmpb.GetSubDirListRequest) (*fmpb.GetSubDirListReply, error) {
	return fs.executor.GetSubDirFile(ctx, req.ID)
}

//UpdateFile 更新文件的Name Type Path
func (fs *FileManagerServer) UpdateFile(ctx context.Context, req *fmpb.UpdateFileRequest) (*model.EmptyStruct, error) {
	return fs.executor.UpdateFile(ctx, req.ID, req.Name, req.Type,req.Path)
}
