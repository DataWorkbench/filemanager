package executor

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/DataWorkbench/glog"
	"gorm.io/gorm/clause"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"

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

func (ex *ResourceManagerExecutor) CreateDir(ctx context.Context, spaceId, dirName, parentId string) (rsp *response.CreateDir, err error) {
	tx := ex.db.Begin().WithContext(ctx)
	if err = tx.Error; err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit().Error
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	var x string
	if parentId != "" &&
		tx.Table(resourceTableName).Select("id").Where("id = ? and is_directory = 1", parentId).Take(&x).RowsAffected == 0 {
		err = qerror.ResourceNotExists
		ex.logger.Warn().Msg("user create directory parent directory not exit").String("parent id", parentId).Fire()
		return
	}
	if tx.Table(resourceTableName).Select("id").Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("pid = ? and space_id = ? and name = ?", parentId, spaceId, dirName).Take(&x).RowsAffected > 0 {
		err = qerror.ResourceAlreadyExists
		return
	}

	resourceId, err := ex.idGenerator.Take()
	if err != nil {
		return nil, err
	}
	info := model.Resource{
		Id:          resourceId,
		Pid:         parentId,
		SpaceId:     spaceId,
		Name:        dirName,
		IsDirectory: true,
	}
	err = tx.Table(resourceTableName).Create(&info).Error
	rsp = &response.CreateDir{Id: resourceId}
	return
}

func (ex *ResourceManagerExecutor) UploadFile(re respb.Resource_UploadFileServer) (err error) {
	var (
		client      *hdfs.Client
		writer      *hdfs.FileWriter
		recv        *respb.UploadFileRequest
		res         model.Resource
		receiveSize int64
		batch       int
		check       string
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

	var x string
	if recv.ParentId != "" && tx.Table(resourceTableName).Select("id").Where("id = ? and is_directory = 1", recv.ParentId).Take(&x).RowsAffected == 0 {
		err = qerror.ResourceNotExists
		return
	}
	if result := tx.Table(resourceTableName).Select("id").Where("pid = ? and space_id = ? and name = ?", recv.ParentId, recv.SpaceId, recv.ResourceName).Take(&x); result.RowsAffected > 0 {
		err = qerror.ResourceAlreadyExists
		return
	}

	res.SpaceId = recv.SpaceId
	res.Pid = recv.ParentId
	res.Type = recv.ResourceType
	res.Name = recv.ResourceName
	res.Size = recv.Size
	res.IsDirectory = false
	hdfsFileDir := fileSplit + recv.SpaceId + fileSplit
	hdfsPath := getHdfsPath(recv.SpaceId, res.Id)
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

	hash := md5.New()
	for {
		recv, err = re.Recv()

		if err == io.EOF {
			if receiveSize != res.Size {
				ex.logger.Warn().Msg("file message lose").String("file id", res.Id).Fire()
				return qerror.Internal
			}
			if !strings.EqualFold(fmt.Sprintf("%X", hash.Sum(nil)), check) {
				ex.logger.Warn().Msg("file message not match").String("file id", res.Id).Fire()
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

		hash.Write(recv.Data)
		check = recv.Md5Message
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

func (ex *ResourceManagerExecutor) ListResources(ctx context.Context, resourceId, spaceId string, resourceType int32, limit, offset int32) (rsp []*model.Resource, count int64, err error) {
	db := ex.db.WithContext(ctx)
	resourceTypes := []int32{0}

	if resourceType > 0 {
		resourceTypes = append(resourceTypes, resourceType)
	}
	if err = db.Table(resourceTableName).Select("*").Where("pid = ? AND space_id = ? AND type IN ?", resourceId, spaceId, resourceTypes).
		Limit(int(limit)).Offset(int(offset)).Order("updated ASC").Scan(&rsp).Error; err != nil {
		return
	}
	if err = db.Table(resourceTableName).Where("pid = ? AND space_id = ? AND type IN ?", resourceId, spaceId, resourceTypes).Count(&count).Error; err != nil {
		return
	}
	return
}

func (ex *ResourceManagerExecutor) UpdateResource(ctx context.Context, resourceId, spaceId, resourceName string, resourceType model.Resource_Type) (*model.EmptyStruct, error) {
	var err error
	db := ex.db.WithContext(ctx)
	info := model.Resource{
		Id:      resourceId,
		SpaceId: spaceId,
		Name:    resourceName,
		Type:    resourceType,
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

	tx := ex.db.Begin().WithContext(ctx)
	defer func() {
		if err == nil {
			tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	if len(ids) == 0 {
		return qerror.InvalidParams.Format("ids")
	}
	err = deleteById(ids, spaceId, tx, client)
	return
}

func deleteById(ids []string, spaceId string, tx *gorm.DB, client *hdfs.Client) (err error) {
	for _, id := range ids {
		var res model.Resource
		if result := tx.Table(resourceTableName).Where(&model.Resource{Id: id}).First(&res); result.RowsAffected == 0 {
			return qerror.ResourceNotExists
		}
		if res.IsDirectory {
			var sourceIds []string
			if result := tx.Table(resourceTableName).Select("id").Where("pid = ?", id).Find(&sourceIds); result.RowsAffected > 0 {
				if err = deleteById(sourceIds, spaceId, tx, client); err != nil {
					return
				}
			} else if err = result.Error; err != nil {
				return
			}
		} else {
			if err = client.Remove(getHdfsPath(spaceId, id)); err != nil {
				if _, ok := err.(*os.PathError); !ok {
					return
				}
			}
		}
		if err = tx.Table(resourceTableName).Delete(model.Resource{Id: id}).Error; err != nil {
			return
		}
	}
	return
}

//func (ex *ResourceManagerExecutor) DeleteResources(ctx context.Context, ids []string, spaceId string) (rsp *model.EmptyStruct, err error) {
//	tx := ex.db.Begin().WithContext(ctx)
//	defer func() {
//		if err == nil {
//			tx.Commit()
//		}
//		if err != nil {
//			tx.Rollback()
//		}
//	}()
//
//	if len(ids) == 0 {
//		return nil, qerror.InvalidParams.Format("ids")
//	}
//	client, err := hdfs.New(ex.hdfsServer)
//	if err != nil {
//		return
//	}
//	defer func() {
//		_ = client.Close()
//	}()
//
//	deleteInfoMap := make(map[string][]string)
//	if err = deleteByIds(ids, tx, deleteInfoMap, "-1"); err != nil {
//		return
//	}
//	for key, value := range deleteInfoMap {
//		for _, id := range value {
//			if err = tx.Table(resourceTableName).Delete(model.Resource{Id: id}).Error; err != nil {
//				return
//			}
//			if err = client.Remove(getHdfsPath(spaceId, id)); err != nil {
//				if _, ok := err.(*os.PathError); !ok {
//					return
//				}
//			}
//		}
//		if key != "-1" {
//			if err = tx.Table(resourceTableName).Delete(model.Resource{Id: key, SpaceId: spaceId}).Error; err != nil {
//				return
//			}
//		}
//	}
//	return &model.EmptyStruct{}, nil
//}
//
//func deleteByIds(ids []string, db *gorm.DB, deleteInfoMap map[string][]string, pid string) (err error) {
//	var sourceIds []string
//	for _, id := range ids {
//		if id == "" {
//			continue
//		}
//		var res model.Resource
//		if result := db.Table(resourceTableName).Where(&model.Resource{Id: id}).First(&res); result.RowsAffected == 0 {
//			return qerror.ResourceNotExists
//		}
//		if res.IsDirectory {
//			deleteInfoMap[id] = []string{}
//			if result := db.Table(resourceTableName).Select("id").Where("pid = ?", id).Find(&sourceIds); result.RowsAffected > 0 {
//				if err = deleteByIds(sourceIds, db, deleteInfoMap, id); err != nil {
//					return
//				}
//			} else if err = result.Error; err != nil {
//				return
//			}
//		} else {
//			deleteInfoMap[pid] = append(deleteInfoMap[pid], id)
//		}
//	}
//	return
//}

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

func (ex *ResourceManagerExecutor) CheckResourceExist(ctx context.Context, spaceId string, parentId string, name string) (err error) {
	db := ex.db.WithContext(ctx)
	var x string
	if db.Table(resourceTableName).Select("id").Where("pid = ? AND space_id = ? AND name = ?", parentId, spaceId, name).Take(&x).RowsAffected != 0 {
		err = qerror.ResourceAlreadyExists
	}
	return
}

//func (ex *ResourceManagerExecutor) DescribeFile(ctx context.Context, resourceId string) (*resource.ResourceResponse, error) {
//	var (
//		info Resource
//	)
//	db := ex.db.WithContext(ctx)
//	if db.Where(Resource{ResourceId: resourceId}).First(&info).RowsAffected == 0 {
//		return nil, qerror.ResourceNotExists
//	}
//	var rsp = &resource.ResourceResponse{
//		ResourceId:   resourceId,
//		SpaceId:      info.SpaceId,
//		ResourceName: info.Name,
//		ResourceType: info.Type,
//		Size:         info.Size,
//		Url:          "hdfs://" + ex.hdfsServer + getHdfsPath(info.SpaceId, resourceId),
//	}
//	return rsp, nil
//}

//func checkAndGetFile(path *string) (fileName string, err error) {
//	if !strings.HasSuffix(*path, ".jar") {
//		err = qerror.InvalidParams.Format("fileName")
//		return
//	}
//	if !strings.HasPrefix(*path, fileSplit) {
//		*path = fileSplit + *path
//	}
//	filePath := *path
//	fileName = filePath[strings.LastIndex(filePath, fileSplit)+1:]
//	if *path = filePath[:strings.LastIndex(filePath, fileSplit)+1]; strings.EqualFold(*path, fileSplit) {
//		return
//	}
//	dirReg := `^\/(\w+\/?)+$`
//	if ok, _ := regexp.Match(dirReg, []byte(*path)); !ok {
//		err = qerror.InvalidParams.Format("filePath")
//		return
//	}
//	return
//}

//func checkDirName(dir string) (dirName string, err error) {
//	if dir == fileSplit {
//		dirName = fileSplit
//		return
//	}
//	if !strings.HasSuffix(dir, fileSplit) {
//		dir = dir + fileSplit
//	}
//	if !strings.HasPrefix(dir, fileSplit) {
//		dir = fileSplit + dir
//	}
//	dirReg := `^\/(\w+\/?)+$`
//	if ok, _ := regexp.Match(dirReg, []byte(dir)); !ok {
//		err = qerror.InvalidParams.Format("filePath")
//		return
//	}
//	return
//}

func getHdfsPath(spaceId, resourceId string) string {
	return fileSplit + spaceId + fileSplit + resourceId + ".jar"
}
