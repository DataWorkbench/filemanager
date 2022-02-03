package server

import (
	"context"

	"github.com/DataWorkbench/gproto/xgo/service/pbsvcresource"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"

	"github.com/DataWorkbench/resourcemanager/executor"
)

type ResourceManagerServer struct {
	pbsvcresource.UnimplementedResourceManageServer
	executor   *executor.ResourceManagerExecutor
	emptyReply *pbmodel.EmptyStruct
}

func NewResourceManagerServer(executor *executor.ResourceManagerExecutor) *ResourceManagerServer {
	return &ResourceManagerServer{
		executor:   executor,
		emptyReply: &pbmodel.EmptyStruct{},
	}
}

func (rm *ResourceManagerServer) UploadFile(re pbsvcresource.ResourceManage_UploadFileServer) error {
	return rm.executor.UploadFile(re)
}

func (rm *ResourceManagerServer) ReUploadFile(re pbsvcresource.ResourceManage_ReUploadFileServer) error {
	return rm.executor.ReUploadFile(re)
}

func (rm *ResourceManagerServer) DownloadFile(req *pbrequest.DownloadFile,
	resp pbsvcresource.ResourceManage_DownloadFileServer) error {
	return rm.executor.DownloadFile(req.ResourceId, resp)
}

func (rm *ResourceManagerServer) DescribeFile(ctx context.Context, req *pbrequest.DescribeFile) (*pbmodel.Resource, error) {
	return rm.executor.DescribeFile(ctx, req.ResourceId)
}

func (rm *ResourceManagerServer) ListResources(ctx context.Context, req *pbrequest.ListResources) (*pbresponse.ListResources, error) {

	infos, count, err := rm.executor.ListResources(ctx, req)
	if err != nil {
		return nil, err
	}
	reply := &pbresponse.ListResources{
		Infos:   infos,
		HasMore: len(infos) >= int(req.Limit),
		Total:   count,
	}
	return reply, nil
}

func (rm *ResourceManagerServer) UpdateResource(ctx context.Context, req *pbrequest.UpdateResource) (*pbmodel.EmptyStruct, error) {
	return rm.executor.UpdateResource(ctx, req.ResourceId, req.SpaceId, req.ResourceName, req.Description, req.ResourceType)
}

func (rm *ResourceManagerServer) DeleteResources(ctx context.Context, req *pbrequest.DeleteResources) (*pbmodel.EmptyStruct, error) {
	err := rm.executor.DeleteResources(ctx, req.ResourceIds, req.SpaceId)
	return &pbmodel.EmptyStruct{}, err
}

func (rm *ResourceManagerServer) DeleteSpaces(ctx context.Context, req *pbrequest.DeleteWorkspaces) (*pbmodel.EmptyStruct, error) {
	return rm.executor.DeleteSpaces(ctx, req.SpaceIds)
}
