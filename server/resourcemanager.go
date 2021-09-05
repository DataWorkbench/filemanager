package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/resource"
	"github.com/DataWorkbench/resourcemanager/executor"
)

type ResourceManagerServer struct {
	resource.UnimplementedResourceManagerServer
	executor   *executor.ResourceManagerExecutor
	emptyReply *model.EmptyStruct
}

func NewResourceManagerServer(executor *executor.ResourceManagerExecutor) *ResourceManagerServer {
	return &ResourceManagerServer{
		executor:   executor,
		emptyReply: &model.EmptyStruct{},
	}
}

// CreateDir @title  创建文件夹
// @description   创建文件夹
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     SpaceID       string         "工作空间id"
// @param     DirName       string         "文件夹全路径名"
func (fs *ResourceManagerServer) CreateDir(ctx context.Context, req *resource.CreateDirRequest) (*model.EmptyStruct, error) {
	return fs.executor.CreateDir(ctx, req.SpaceId, req.DirName, req.ParentId, req.ResourceId)
}

// UploadFile @title  文件上传
// @description   上传文件到hadoop文件系统
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     SpaceID        string         "工作空间id"
// @param     FileName       string         "文件全路径名"
// @param     FileType       int32          "文件类型 1 jar包文件 2 udf文件"
func (fs *ResourceManagerServer) UploadFile(fu resource.ResourceManager_UploadFileServer) error {
	return fs.executor.UploadFile(fu)
}

// DownloadFile @title  文件下载
// @description   下载hadoop文件到本地
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     FileId             string         "文件id"
func (fs *ResourceManagerServer) DownloadFile(req *resource.DownloadRequest, res resource.ResourceManager_DownloadFileServer) error {
	return fs.executor.DownloadFile(req.ResourceId, res)
}

// ListFiles @title  文件下载
// @description   下载hadoop文件到本地
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     SpaceId        string        "根据spaceId查询列表"
// @param     FileType       int32         "根据类型查询"
// @param     DirName        string        "根据目录名查询"
// @param     Limit          int32         "分页限制"
// @param     Offset         int32         "页码"
// @return    Total          int32         "文件总数"
// @return    HasMode        int32         "是否有更多"
// @return    ID             string        "文件id"
// @return    SpaceID        string        "工作空间id"
// @return    FileName       string        "文件名"
// @return    FilePath       string        "文件路径"
// @return    FileType       int32         "文件类型 1 jar包文件 2 udf文件"
// @return    Url            string        "文件的hadoop路径"
// @return    IsDir          bool          "true 文件夹 、false 文件"
func (fs *ResourceManagerServer) ListFiles(ctx context.Context, req *resource.ListRequest) (*resource.ListResponse, error) {
	infos, count, err := fs.executor.ListFiles(ctx, req.ResourceId, req.SpaceId, req.ResourceType, req.Limit, req.Offset)
	if err != nil {
		return nil, err
	}
	reply := &resource.ListResponse{
		Infos:   infos,
		HasMore: len(infos) >= int(req.Limit),
		Total:   count,
	}
	return reply, nil
}

// UpdateFile @title  更新文件
// @description   更新文件信息
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     Id            string        "文件id"
// @param     FileName      string        "文件名"
// @param     FilePath      string        "文件路径"
// @param     FileType      int32         "文件类型"
func (fs *ResourceManagerServer) UpdateFile(ctx context.Context, req *resource.UpdateFileRequest) (*model.EmptyStruct, error) {
	return fs.executor.UpdateFile(ctx, req.ResourceId, req.ResourceName, req.ResourceType)
}

// Delete @title  根据id批量删除文件
// @description   删除文件
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     Ids           string         "文件id"
func (fs *ResourceManagerServer) Delete(ctx context.Context, req *resource.DeleteRequest) (*model.EmptyStruct, error) {
	return fs.executor.DeleteFiles(ctx, req.Ids)
}

// DeleteSpace @title  根据space_id批量删除文件
// @description   删除文件
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     SpaceIds           string    "工作空间id"
func (fs *ResourceManagerServer) DeleteSpace(ctx context.Context, req *resource.DeleteSpaceRequest) (*model.EmptyStruct, error) {
	return fs.executor.DeleteSpace(ctx, req.SpaceIds)
}

// DescribeFile @title
// @description   根据id查询文件信息
// @auth      gx             时间（2021/7/28   10:57 ）
// @param     id           string    "文件id"
func (fs *ResourceManagerServer) DescribeFile(ctx context.Context, req *resource.DescribeRequest) (*resource.ResourceResponse, error) {
	return fs.executor.DescribeFile(ctx, req.ResourceId)
}
