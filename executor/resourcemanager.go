package executor

import (
	"context"
	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/respb"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/colinmarc/hdfs/v2"
	"gorm.io/gorm/clause"
	"io"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/DataWorkbench/gproto/pkg/model"
	"gorm.io/gorm"
)

type ResourceManagerExecutor struct {
	db              *gorm.DB
	idGenerator     *idgenerator.IDGenerator
	logger          *glog.Logger
	hadoopConfigDir string
}

const (
	fileSplit = "/"
)

func NewResourceManagerExecutor(db *gorm.DB, l *glog.Logger, hadoopConfigDir string) *ResourceManagerExecutor {
	return &ResourceManagerExecutor{
		db:              db,
		idGenerator:     idgenerator.New(constants.FileMangerIDPrefix),
		logger:          l,
		hadoopConfigDir: hadoopConfigDir,
	}
}

func (ex *ResourceManagerExecutor) UploadFile(re respb.Resource_UploadFileServer) (err error) {
	var (
		client      *HadoopClient
		writer      *hdfs.FileWriter
		recv        *respb.UploadFileRequest
		res         model.Resource
		receiveSize int64
		batch       int
	)
	if recv, err = re.Recv(); err != nil {
		return
	}
	if res.ResourceId, err = ex.idGenerator.Take(); err != nil {
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
	res.ResourceSize = recv.ResourceSize
	res.Description = recv.Description
	res.Status = model.Resource_Enabled
	res.CreateBy = recv.CreateBy

	var x string
	//TODO check if resource exists
	if r := ex.db.Table(resourceTableName).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("space_id = ? AND type = ? AND name = ? AND status != ?", res.SpaceId, res.Type, res.Name, model.Resource_Deleted).
		Take(&x).RowsAffected; r > 0 {
		err = qerror.ResourceAlreadyExists
		return
	}
	hdfsFileDir := fileSplit + res.SpaceId + fileSplit
	hdfsPath := getHdfsPath(res.SpaceId, res.ResourceId)
	//client, err = NewHadoopFromNameNodes(ex.hdfsServer, "root")
	if client, err = NewHadoopClientFromConfFile(ex.hadoopConfigDir, "root"); err != nil {
		return
	}
	defer func() {
		if client != nil {
			_ = client.close()
		}
	}()

	if writer, err = client.createFileWriter(hdfsPath); err != nil {
		if _, ok := err.(*os.PathError); ok {
			if err = client.mkdirP(hdfsFileDir, 0777); err != nil {
				ex.logger.Warn().Msg("mkdir directory failed").String("directory is", hdfsFileDir).Fire()
				return
			}
			if writer, err = client.createFileWriter(hdfsPath); err != nil {
				ex.logger.Warn().Msg("create file failed").String("path is", hdfsPath).Fire()
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
			_ = client.remove(hdfsPath)
		}
	}()

	for {
		recv, err = re.Recv()
		if err == io.EOF {
			if receiveSize != res.ResourceSize {
				ex.logger.Warn().Msg("file message lose").String("file id", res.ResourceId).Fire()
				return qerror.Internal
			}
			if err = tx.Table(resourceTableName).Create(&res).Error; err != nil {
				return
			}
			return re.SendAndClose(&response.UploadFile{Id: res.ResourceId})
		}
		if err != nil {
			ex.logger.Error().Msg(err.Error()).Fire()
			return
		}
		if batch, err = writer.Write(recv.Data); err != nil {
			ex.logger.Warn().Msg("write data failed").Fire()
			return
		}
		// count total size,provided the file size right.
		receiveSize += int64(batch)
	}
}

func (ex *ResourceManagerExecutor) ReUploadFile(re respb.Resource_ReUploadFileServer) (err error) {
	var (
		client      *HadoopClient
		writer      *hdfs.FileWriter
		recv        *respb.ReUploadFileRequest
		res         model.Resource
		receiveSize int64
		batch       int
	)
	if recv, err = re.Recv(); err != nil {
		return
	}
	res.ResourceId = recv.ResourceId
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
	res.ResourceSize = recv.Size

	hdfsFileDir := fileSplit + res.SpaceId + fileSplit
	tmpPath := getHdfsPath(res.SpaceId, strconv.FormatInt(time.Now().Unix(), 10))
	//client, err = NewHadoopFromNameNodes(ex.hdfsServer, "root")
	client, err = NewHadoopClientFromConfFile(ex.hadoopConfigDir, "root")
	defer func() {
		if client != nil {
			_ = client.close()
		}
	}()

	if writer, err = client.createFileWriter(tmpPath); err != nil {
		if _, ok := err.(*os.PathError); ok {
			if err = client.mkdirP(hdfsFileDir, 0777); err != nil {
				return
			}
			if writer, err = client.createFileWriter(tmpPath); err != nil {
				return
			}
		} else {
			return
		}
	}

	defer func() {
		if err == nil {
			_ = writer.Close()
			hdfsPath := getHdfsPath(res.SpaceId, res.ResourceId)
			err = client.rename(tmpPath, hdfsPath)
		} else {
			_ = client.remove(tmpPath)
		}
	}()

	for {
		recv, err = re.Recv()

		if err == io.EOF {
			if receiveSize != res.ResourceSize {
				ex.logger.Warn().Msg("file message lose").String("file id", res.ResourceId).Fire()
				return qerror.Internal
			}
			res.Updated = time.Now().Unix()
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
		client *HadoopClient
		reader *hdfs.FileReader
	)
	db := ex.db.WithContext(resp.Context())
	if db.Table(resourceTableName).Where("resource_id = ? and status != ?", resourceId, model.Resource_Deleted).First(&info).RowsAffected == 0 {
		return qerror.ResourceNotExists
	}
	//client, err = NewHadoopFromNameNodes(ex.hdfsServer, "root")
	client, err = NewHadoopClientFromConfFile(ex.hadoopConfigDir, "root")
	defer func() {
		err = client.close()
	}()
	hdfsPath := getHdfsPath(info.SpaceId, resourceId)
	if reader, err = client.openFileReader(hdfsPath); err != nil {
		return
	}
	defer func() {
		err = reader.Close()
	}()
	if err = resp.Send(&response.DownloadFile{Size: info.ResourceSize, Name: info.Name}); err != nil {
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
		clause.Neq{
			Column: "status",
			Value:  model.Resource_Deleted,
		},
	}
	if req.ResourceName != "" && len(req.ResourceName) > 0 {
		if err = checkResourceName(req.ResourceName); err != nil {
			return
		}
		exp = append(exp, clause.Eq{
			Column: "name",
			Value:  req.ResourceName,
		})
	} else if len(req.Search) > 0 {
		exp = append(exp, clause.Like{
			Column: "name",
			Value:  "%" + req.Search + "%",
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
	if err = db.Table(resourceTableName).Select("count(resource_id)").Clauses(clause.Where{Exprs: exp}).
		Count(&count).Error; err != nil {
		return
	}
	return
}

func (ex *ResourceManagerExecutor) UpdateResource(ctx context.Context, resourceId, spaceId, resourceName, description string, resourceType model.Resource_Type) (*model.EmptyStruct, error) {
	var err error
	db := ex.db.WithContext(ctx)
	info := model.Resource{
		ResourceId: resourceId,
		SpaceId:    spaceId,
	}
	if description != "" {
		info.Description = description
	}
	if resourceType > 0 {
		info.Type = resourceType
	}
	if resourceName != "" {
		if err = checkResourceName(resourceName); err != nil {
			return nil, err
		}
		info.Name = resourceName
		var id int
		if rows := db.Table(resourceTableName).Select("resource_id").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("space_id = ? and type = ? and name = ? and status != ?", spaceId, resourceType, resourceName, model.Resource_Deleted).
			Take(&id).RowsAffected; rows > 0 {
			return nil, qerror.ResourceAlreadyExists
		}
	}
	if err = db.Table(resourceTableName).Updates(&info).Error; err != nil {
		return nil, err
	}
	return &model.EmptyStruct{}, nil
}

func (ex *ResourceManagerExecutor) DeleteResources(ctx context.Context, ids []string, spaceId string) (err error) {
	//client, err := NewHadoopFromNameNodes(ex.hdfsServer, "root")
	//if err != nil {
	//	return
	//}
	//defer func() {
	//	_ = client.close()
	//}()

	if len(ids) == 0 {
		return qerror.InvalidParams.Format("ids")
	}
	eqExpr := make([]clause.Expression, len(ids))
	for i := 0; i < len(ids); i++ {
		eqExpr[i] = clause.Eq{Column: "resource_id", Value: ids[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}

	currentTime := time.Now().Unix()
	err = ex.db.WithContext(ctx).Table(resourceTableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Eq{Column: "space_id", Value: spaceId},
			clause.Neq{Column: "status", Value: model.Resource_Deleted},
			expr,
		},
	}).Updates(map[string]interface{}{"status": model.Resource_Deleted, "updated": currentTime}).Error
	//if err = ex.db.WithContext(ctx).Table(resourceTableName).Where("id = ? AND space_id = ?", id, spaceId).Delete(&model.Resource{}).Error; err != nil {
	//	return
	//}
	//if err = client.remove(getHdfsPath(spaceId, id)); err != nil {
	//	if _, ok := err.(*os.PathError); !ok {
	//		return
	//	}
	//}
	return
}

func (ex *ResourceManagerExecutor) DeleteSpaces(ctx context.Context, spaceIds []string) (*model.EmptyStruct, error) {
	//db := ex.db.WithContext(ctx)
	//client, err := NewHadoopFromNameNodes(ex.hdfsServer, "root")
	//if err != nil {
	//	return nil, err
	//}
	//defer func() {
	//	_ = client.close()
	//}()

	//for _, spaceId := range spaceIds {
	//	if err = client.remove(fileSplit + spaceId); err != nil {
	//		if _, ok := err.(*os.PathError); !ok {
	//			return nil, err
	//		}
	//	}
	//	if err = db.Table(resourceTableName).Where("space_id = ?", spaceId).Delete(&model.Resource{}).Error; err != nil {
	//		return nil, err
	//	}
	//}
	eqExpr := make([]clause.Expression, len(spaceIds))
	for i := 0; i < len(spaceIds); i++ {
		eqExpr[i] = clause.Eq{Column: "space_id", Value: spaceIds[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}
	currentTime := time.Now().Unix()
	err := ex.db.WithContext(ctx).Table(resourceTableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: model.Resource_Deleted},
			expr,
		},
	}).Updates(map[string]interface{}{"status": model.Resource_Deleted, "updated": currentTime}).Error
	return &model.EmptyStruct{}, err
}

func (ex *ResourceManagerExecutor) DescribeFile(ctx context.Context, id string) (rsp *model.Resource, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(resourceTableName).Where("resource_id = ?", id).First(&rsp).Error
	return
}

func getHdfsPath(spaceId, resourceId string) string {
	return fileSplit + spaceId + fileSplit + resourceId + ".jar"
}

func checkResourceName(name string) (err error) {
	if len(name) == 0 ||
		len(name) > 256 {
		err = qerror.InvalidParams.Format("resource_name")
		return
	}
	var reg *regexp.Regexp
	reg, err = regexp.Compile(`[\\^?*|"<>:/\s]`)
	if err != nil {
		return
	} else if len(reg.FindString(name)) > 0 {
		err = qerror.InvalidParams.Format("resource_name")
		return
	}
	return
}
