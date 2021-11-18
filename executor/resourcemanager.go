package executor

import (
	"context"
	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/request"
	"gorm.io/gorm/clause"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/DataWorkbench/gproto/pkg/respb"
	"github.com/DataWorkbench/gproto/pkg/response"

	"github.com/DataWorkbench/gproto/pkg/model"

	"github.com/colinmarc/hdfs"
	"gorm.io/gorm"
)

type ResourceManagerExecutor struct {
	db          *gorm.DB
	idGenerator *idgenerator.IDGenerator
	logger      *glog.Logger
	hdfsServer  string
}

const (
	fileSplit = "/"
)

func NewResourceManagerExecutor(db *gorm.DB, l *glog.Logger, hdfsServer string) *ResourceManagerExecutor {
	return &ResourceManagerExecutor{
		db:          db,
		idGenerator: idgenerator.New(constants.FileMangerIDPrefix),
		logger:      l,
		hdfsServer:  hdfsServer,
	}
}

func (ex *ResourceManagerExecutor) UploadFile(re respb.Resource_UploadFileServer) (err error) {
	var (
		client      *hdfs.Client
		writer      *hdfs.FileWriter
		recv        *respb.UploadFileRequest
		res         model.Resource
		receiveSize int64
		batch       int
	)
	if recv, err = re.Recv(); err != nil {
		return
	}
	if res.Id, err = ex.idGenerator.Take(); err != nil {
		return
	}
	tx := ex.db.Begin().WithContext(re.Context())
	if err = tx.Error; err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit().Error
		} else {
			tx.Rollback()
		}
	}()

	res.Name = recv.ResourceName
	res.SpaceId = recv.SpaceId
	res.Type = recv.ResourceType
	res.Size = recv.Size
	res.Description = recv.Description

	hdfsFileDir := fileSplit + res.SpaceId + fileSplit
	hdfsPath := getHdfsPath(res.SpaceId, res.Id)
	if client, err = hdfs.New(ex.hdfsServer); err != nil {
		return
	}
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	if writer, err = client.Create(hdfsPath); err != nil {
		if _, ok := err.(*os.PathError); ok {
			if err = client.MkdirAll(hdfsFileDir, 0777); err != nil {
				return
			}
			if writer, err = client.Create(hdfsPath); err != nil {
				return
			}
		} else {
			return
		}
	}
	defer func() {
		if err == nil {
			_ = writer.Close()
		} else {
			_ = client.Remove(hdfsPath)
		}
	}()

	for {
		recv, err = re.Recv()
		if err == io.EOF {
			if receiveSize != res.Size {
				ex.logger.Warn().Msg("file message lose").String("file id", res.Id).Fire()
				return qerror.Internal
			}
			if err = tx.Table(resourceTableName).Create(&res).Error; err != nil {
				return
			}
			return re.SendAndClose(&response.UploadFile{Id: res.Id})
		}
		if err != nil {
			return
		}
		if batch, err = writer.Write(recv.Data); err != nil {
			return
		}
		// count total size,provided the file size right.
		receiveSize += int64(batch)
	}
}

func (ex *ResourceManagerExecutor) ReUploadFile(re respb.Resource_ReUploadFileServer) (err error) {
	var (
		client      *hdfs.Client
		writer      *hdfs.FileWriter
		recv        *respb.ReUploadFileRequest
		res         model.Resource
		receiveSize int64
		batch       int
	)
	if recv, err = re.Recv(); err != nil {
		return
	}
	res.Id = recv.ResourceId
	res.SpaceId = recv.SpaceId

	tx := ex.db.Begin().WithContext(re.Context())
	if err = tx.Error; err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit().Error
		} else {
			tx.Rollback()
		}
	}()

	//TODO receive file message
	if recv, err = re.Recv(); err != nil {
		return
	}
	res.Size = recv.Size

	hdfsFileDir := fileSplit + res.SpaceId + fileSplit
	hdfsPath := getHdfsPath(res.SpaceId, res.Id)
	if client, err = hdfs.New(ex.hdfsServer); err != nil {
		return
	}
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	if err = client.Remove(hdfsPath); err != nil {
		return
	}
	if writer, err = client.Create(hdfsPath); err != nil {
		if _, ok := err.(*os.PathError); ok {
			if err = client.MkdirAll(hdfsFileDir, 0777); err != nil {
				return
			}
			if writer, err = client.Create(hdfsPath); err != nil {
				return
			}
		} else {
			return
		}
	}
	defer func() {
		if err == nil {
			_ = writer.Close()
		} else {
			_ = client.Remove(hdfsPath)
		}
	}()

	for {
		recv, err = re.Recv()

		if err == io.EOF {
			if receiveSize != res.Size {
				ex.logger.Warn().Msg("file message lose").String("file id", res.Id).Fire()
				return qerror.Internal
			}
			if err = tx.Table(resourceTableName).Updates(&res).Error; err != nil {
				return
			}
			return re.SendAndClose(&model.EmptyStruct{})
		}

		if err != nil {
			return
		}

		if batch, err = writer.Write(recv.Data); err != nil {
			return
		}

		// count total size,provided the file size right.
		receiveSize += int64(batch)
	}
}

func (ex *ResourceManagerExecutor) DownloadFile(resourceId string, resp respb.Resource_DownloadFileServer) (err error) {
	var (
		info   model.Resource
		client *hdfs.Client
		reader *hdfs.FileReader
	)
	db := ex.db.WithContext(resp.Context())
	if db.Table(resourceTableName).Where("id = ?", resourceId).First(&info).RowsAffected == 0 {
		return qerror.ResourceNotExists
	}
	if client, err = hdfs.New(ex.hdfsServer); err != nil {
		return
	}
	defer func() {
		err = client.Close()
	}()
	hdfsPath := getHdfsPath(info.SpaceId, resourceId)
	if reader, err = client.Open(hdfsPath); err != nil {
		return
	}
	defer func() {
		err = reader.Close()
	}()
	if err = resp.Send(&response.DownloadFile{Size: info.Size, Name: info.Name}); err != nil {
		return
	}
	buf := make([]byte, 4096)
	n := 0
	for {
		n, err = reader.Read(buf)
		if err == io.EOF && n == 0 {
			return nil
		}
		if err != nil {
			return
		}
		if err = resp.Send(&response.DownloadFile{Data: buf[:n]}); err != nil {
			return
		}
	}
}

func (ex *ResourceManagerExecutor) ListResources(ctx context.Context, req *request.ListResources) (rsp []*model.Resource, count int64, err error) {
	db := ex.db.WithContext(ctx)
	order := req.SortBy
	if order == "" {
		order = "updated"
	}
	if req.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}
	exp := []clause.Expression{
		clause.Eq{
			Column: "space_id",
			Value:  req.SpaceId,
		},
	}
	if req.ResourceName != "" && len(req.ResourceName) > 0 {
		if len(req.ResourceName) == 0 ||
			len(req.ResourceName) > 256 || !strings.HasSuffix(req.ResourceName, ".jar") {
			err = qerror.InvalidParams.Format("resource_name")
			return
		}
		var reg *regexp.Regexp
		reg, err = regexp.Compile(`[\\^?*|"<>:/\s]`)
		if err != nil {
			return
		} else if len(reg.FindString(req.ResourceName)) > 0 {
			err = qerror.InvalidParams.Format("resource_name")
			return
		}
		exp = append(exp, clause.Eq{
			Column: "name",
			Value:  req.ResourceName,
		})
	} else if len(req.Search) > 0 {
		exp = append(exp, clause.Like{
			Column: "name",
			Value:  "%"+req.Search+"%",
		})
	}
	if req.ResourceType > 0 {
		exp = append(exp, clause.Eq{
			Column: "type",
			Value:  req.ResourceType,
		})
	}
	if err = db.Table(resourceTableName).Select("*").Clauses(clause.Where{Exprs: exp}).
		Limit(int(req.Limit)).Offset(int(req.Offset)).Order(order).Scan(&rsp).Error; err != nil {
		return
	}
	if err = db.Table(resourceTableName).Select("count(id)").Clauses(clause.Where{Exprs: exp}).
		Count(&count).Error; err != nil {
		return
	}
	return
}

func (ex *ResourceManagerExecutor) UpdateResource(ctx context.Context, resourceId, spaceId, resourceName, description string, resourceType model.Resource_Type) (*model.EmptyStruct, error) {
	var err error
	db := ex.db.WithContext(ctx)
	info := model.Resource{
		Id:      resourceId,
		SpaceId: spaceId,
	}
	if resourceName != "" {
		info.Name = resourceName
	}
	if description != "" {
		info.Description = description
	}
	if resourceType > 0 {
		info.Type = resourceType
	}
	if err = db.Table(resourceTableName).Updates(&info).Error; err != nil {
		return nil, err
	}
	return &model.EmptyStruct{}, nil
}

func (ex *ResourceManagerExecutor) DeleteResources(ctx context.Context, ids []string, spaceId string) (err error) {
	client, err := hdfs.New(ex.hdfsServer)
	if err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()

	if len(ids) == 0 {
		return qerror.InvalidParams.Format("ids")
	}
	for _, id := range ids {
		if err = ex.db.WithContext(ctx).Where("id = ? AND space_id = ?", id, spaceId).Delete(&model.Resource{}).Error; err != nil {
			return
		}
		if err = client.Remove(getHdfsPath(spaceId, id)); err != nil {
			if _, ok := err.(*os.PathError); !ok {
				return
			}
		}
	}
	return
}

func (ex *ResourceManagerExecutor) DeleteSpaces(ctx context.Context, spaceIds []string) (*model.EmptyStruct, error) {
	db := ex.db.WithContext(ctx)
	client, err := hdfs.New(ex.hdfsServer)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = client.Close()
	}()

	for _, spaceId := range spaceIds {
		if err = client.Remove(fileSplit + spaceId); err != nil {
			if _, ok := err.(*os.PathError); !ok {
				return nil, err
			}
		}
		if err = db.Where("space_id = ?", spaceId).Delete(&model.Resource{}).Error; err != nil {
			return nil, err
		}
	}
	return &model.EmptyStruct{}, nil
}

func (ex *ResourceManagerExecutor) DescribeFile(ctx context.Context, id string) (rsp *model.Resource, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(resourceTableName).Where("id = ?", id).First(&rsp).Error
	return
}

func getHdfsPath(spaceId, resourceId string) string {
	return fileSplit + spaceId + fileSplit + resourceId + ".jar"
}
