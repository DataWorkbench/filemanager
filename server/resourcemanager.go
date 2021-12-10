package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/respb"
	"github.com/DataWorkbench/gproto/pkg/response"

	"github.com/DataWorkbench/resourcemanager/executor"
)

type ResourceManagerServer struct {
	respb.UnimplementedResourceServer
	executor   *executor.ResourceManagerExecutor
	emptyReply *model.EmptyStruct
}

func NewResourceManagerServer(executor *executor.ResourceManagerExecutor) *ResourceManagerServer {
	return &ResourceManagerServer{
		executor:   executor,
		emptyReply: &model.EmptyStruct{},
	}
}

func (rm *ResourceManagerServer) UploadFile(re respb.Resource_UploadFileServer) error {
	return rm.executor.UploadFile(re)
}

func (rm *ResourceManagerServer) ReUploadFile(re respb.Resource_ReUploadFileServer) error {
	return rm.executor.ReUploadFile(re)
}

func (rm *ResourceManagerServer) DownloadFile(req *request.DownloadFile, resp respb.Resource_DownloadFileServer) error {
	return rm.executor.DownloadFile(req.ResourceId, resp)
}

func (rm *ResourceManagerServer) DescribeFile(ctx context.Context,req *request.DescribeFile) (*model.Resource, error) {
	return rm.executor.DescribeFile(ctx,req.ResourceId)
}

func (rm *ResourceManagerServer) ListResources(ctx context.Context, req *request.ListResources) (*response.ListResources, error) {

	infos, count, err := rm.executor.ListResources(ctx, req)
	if err != nil {
		return nil, err
	}
	reply := &response.ListResources{
		Infos:   infos,
		HasMore: len(infos) >= int(req.Limit),
		Total:   count,
	}
	return reply, nil
}

func (rm *ResourceManagerServer) UpdateResource(ctx context.Context, req *request.UpdateResource) (*model.EmptyStruct, error) {
	return rm.executor.UpdateResource(ctx, req.ResourceId, req.SpaceId, req.ResourceName,req.Description,req.ResourceType)
}

func (rm *ResourceManagerServer) DeleteResources(ctx context.Context, req *request.DeleteResources) (*model.EmptyStruct, error) {
	err := rm.executor.DeleteResources(ctx, req.ResourceIds, req.SpaceId)
	return &model.EmptyStruct{}, err
}

func (rm *ResourceManagerServer) DeleteSpaces(ctx context.Context, req *request.DeleteWorkspaces) (*model.EmptyStruct, error) {
	return rm.executor.DeleteSpaces(ctx, req.SpaceIds)
}
