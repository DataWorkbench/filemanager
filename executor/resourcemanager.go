package executor

import (
	"context"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/resource"

	"github.com/colinmarc/hdfs"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

func NewFileManagerExecutor(db *gorm.DB, l *glog.Logger, hdfsServer string) *ResourceManagerExecutor {
	return &ResourceManagerExecutor{
		db:          db,
		idGenerator: idgenerator.New(constants.FileMangerIDPrefix),
		logger:      l,
		hdfsServer:  hdfsServer,
	}
}

func (ex *ResourceManagerExecutor) CreateDir(ctx context.Context, spaceId, dirName, parentId, sourceId string) (*model.EmptyStruct, error) {
	var err error
	if sourceId == "" {
		if sourceId, err = ex.idGenerator.Take(); err != nil {
			return nil, err
		}
	}
	if parentId == "" {
		parentId = "-1"
	}

	db := ex.db.WithContext(ctx)
	fileManger := Resource{
		ResourceId: sourceId,
		SpaceId:    spaceId,
		ParentId:   parentId,
		Name:       dirName,
		IsDir:      true,
	}
	tx := db.Begin()
	if result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(Resource{SpaceId: spaceId, ParentId: parentId, Name: dirName}).First(&Resource{}); result.RowsAffected > 0 {
		tx.Rollback()
		return nil, qerror.ResourceAlreadyExists
	}

	if err = tx.Create(&fileManger).Error; err != nil {
		tx.Rollback()
		return nil, err
	}
	tx.Commit()
	return &model.EmptyStruct{}, nil
}

func (ex *ResourceManagerExecutor) UploadFile(fu resource.ResourceManager_UploadFileServer) (err error) {
	var (
		client      *hdfs.Client
		writer      *hdfs.FileWriter
		recv        *resource.UploadFileRequest
		resource    Resource
		receiveSize int64
		batch       int
		receiveByte []byte
		//checkmd5       string
	)
	if recv, err = fu.Recv(); err != nil {
		return err
	}

	if recv != nil {
		//TODO 测试可以传id
		if recv.ResourceId != "" {
			resource.ResourceId = recv.ResourceId
		} else {
			if resource.ResourceId, err = ex.idGenerator.Take(); err != nil {
				return
			}
		}
		db := ex.db.WithContext(fu.Context())
		if recv.ParentId == "" {
			recv.ParentId = "-1"
		} else {
			if db.Where("pid = ?", recv.ParentId).Find(&Resource{}).RowsAffected == 0 {
				return qerror.ResourceNotExists
			}
		}

		resource.SpaceId = recv.SpaceId
		resource.ParentId = recv.ParentId
		resource.Type = recv.ResourceType
		resource.Name = recv.ResourceName
		resource.Size = recv.Size
		resource.IsDir = false
		if !resource.CheckParams() {
			err = qerror.InvalidParamsValue
			return
		}

		//TODO 加锁
		if result := db.Where(Resource{
			SpaceId:  resource.SpaceId,
			Name:     resource.Name,
			ParentId: resource.ParentId}).First(&Resource{}); result.RowsAffected > 0 {
			return qerror.ResourceAlreadyExists
		}

		if client, err = hdfs.New(ex.hdfsServer); err != nil {
			return
		}
		defer func() {
			if client != nil {
				_ = client.Close()
			}
		}()

		//checkmd5 = recv.Md5Message
		hdfsFileDir := fileSplit + recv.SpaceId + fileSplit
		hdfsPath := getHdfsPath(recv.SpaceId, resource.ResourceId)
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
			if err2 := writer.Close(); err2 != nil {
				_ = client.Remove(hdfsPath)
			}
		}()

		for {
			recv, err = fu.Recv()
			if err == io.EOF {
				//if fmt.Sprintf("%x", md5.Sum(receiveByte)) != resource.Check {
				//	_ = client.Remove(hdfsPath)
				//	//TODO 是否需要定义一个丢失资源的异常
				//	return qerror.Internal.Format("传输数据有误")
				//}
				if receiveSize != resource.Size {
					_ = client.Remove(hdfsPath)
					//TODO 是否需要定义一个丢失资源的异常
					return qerror.Internal.Format("传输数据有丢失")
				}
				if result := db.Create(&resource); result.Error != nil {
					_ = client.Remove(hdfsPath)
					return result.Error
				}
				return fu.SendAndClose(&model.EmptyStruct{})
			}

			if err != nil {
				_ = client.Remove(hdfsPath)
				return
			}
			if batch, err = writer.Write(recv.Data); err != nil {
				_ = client.Remove(hdfsPath)
				return
			}
			receiveByte = append(receiveByte, recv.Data...)
			receiveSize += int64(batch)
		}
	}
	return qerror.Internal
}

func (ex *ResourceManagerExecutor) DownloadFile(resourceId string, res resource.ResourceManager_DownloadFileServer) (err error) {
	var (
		info   Resource
		client *hdfs.Client
		reader *hdfs.FileReader
	)
	db := ex.db.WithContext(res.Context())
	if err = db.Where("id = ?", resourceId).First(&info).Error; err != nil {
		return
	}
	if client, err = hdfs.New(ex.hdfsServer); err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()
	hdfsPath := getHdfsPath(info.SpaceId, resourceId)
	if reader, err = client.Open(hdfsPath); err != nil {
		return qerror.ResourceNotExists
	}
	buf := make([]byte, 4096)
	n := 0
	if err = res.Send(&resource.DownloadResponse{Size: info.Size, Name: info.Name}); err != nil {
		return
	}
	for {
		n, err = reader.Read(buf)
		if err == io.EOF && n == 0 {
			return nil
		}
		if err != nil {
			return
		}
		if err = res.Send(&resource.DownloadResponse{Data: buf[:n]}); err != nil {
			return
		}
	}
}

func (ex *ResourceManagerExecutor) ListFiles(ctx context.Context, resourceId, spaceId string, resourceType int32, limit, offset int32) (rsp []*resource.ResourceResponse, count int64, err error) {
	db := ex.db.WithContext(ctx)
	var (
		files []*Resource
	)
	//TODO 加入文件夹？
	if resourceId == "" {
		resourceId = "-1"
	}

	if resourceType == 0 {
		if err = db.Model(&Resource{}).Select("*").Where("pid = ? AND space_id = ?", resourceId, spaceId).
			Limit(int(limit)).Offset(int(offset)).Order("update_time ASC").Scan(&files).Error; err != nil {
			return
		}
		if err = db.Model(&Resource{}).Where("pid = ? AND space_id = ?", resourceId, spaceId).Count(&count).Error; err != nil {
			return
		}
	} else {
		resourceTypes := []int32{0, resourceType}
		if err = db.Model(&Resource{}).Select("*").Where("pid = ? AND space_id = ? AND type IN ?", resourceId, spaceId, resourceTypes).
			Limit(int(limit)).Offset(int(offset)).Order("update_time ASC").Scan(&files).Error; err != nil {
			return
		}
		if err = db.Model(&Resource{}).Where("pid = ? AND space_id = ? AND type IN ?", resourceId, spaceId, resourceTypes).Count(&count).Error; err != nil {
			return
		}
	}

	for _, file := range files {
		rsp = append(rsp, &resource.ResourceResponse{
			ResourceId:   file.ResourceId,
			SpaceId:      file.SpaceId,
			ResourceName: file.Name,
			ResourceType: file.Type,
			IsDir:        file.IsDir,
		})
	}
	return
}

func (ex *ResourceManagerExecutor) UpdateFile(ctx context.Context, resourceId, resourceName string, resourceType int32) (*model.EmptyStruct, error) {
	var (
		err  error
		info Resource
	)

	db := ex.db.WithContext(ctx)
	tDb := db
	info.ResourceId = resourceId
	if result := db.First(&info); result.RowsAffected == 0 {
		return nil, qerror.ResourceNotExists
	}
	if resourceName != "" {
		info.Name = resourceName
		tDb = tDb.Where(Resource{Name: resourceName})
	}
	if resourceType > 0 {
		info.Type = resourceType
	}

	if result := tDb.Debug().Where(Resource{SpaceId: info.SpaceId}).Find(&Resource{}); result.RowsAffected > 0 {
		return nil, qerror.ResourceAlreadyExists
	}
	if err = db.Save(&info).Error; err != nil {
		return nil, err
	}
	return &model.EmptyStruct{}, nil
}

func (ex *ResourceManagerExecutor) DeleteFiles(ctx context.Context, ids []string) (rsp *model.EmptyStruct, err error) {
	db := ex.db.WithContext(ctx)
	if len(ids) == 0 {
		return nil, qerror.InvalidParams.Format("ids")
	}
	client, err := hdfs.New(ex.hdfsServer)
	if err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()

	deleteInfoMap := make(map[string][]string)
	var spaceId string
	//TODO -1表示传入的文件id; key 为要删除的文件夹 value 为文件夹下的文件
	if spaceId, err = deleteByIds(ids, db, deleteInfoMap, "-1"); err != nil {
		return
	}
	tx := db.Begin()
	for key, value := range deleteInfoMap {
		for _, id := range value {
			if err = tx.Delete(Resource{ResourceId: id}).Error; err != nil {
				tx.Rollback()
				return
			}
			if err = client.Remove(getHdfsPath(spaceId, id)); err != nil {
				if _, ok := err.(*os.PathError); !ok {
					tx.Rollback()
					return
				}
			}
		}
		if key != "-1" {
			if err = db.Delete(Resource{ResourceId: key}).Error; err != nil {
				tx.Rollback()
				return
			}
		}
	}
	tx.Commit()
	return &model.EmptyStruct{}, nil
}

func deleteByIds(ids []string, db *gorm.DB, deleteInfoMap map[string][]string, pid string) (spaceId string, err error) {
	var sourceIds []string
	for _, id := range ids {
		var resource Resource
		if result := db.Where(&Resource{ResourceId: id}).First(&resource); result.RowsAffected == 0 {
			return "", qerror.ResourceNotExists
		} else {
			spaceId = resource.SpaceId
		}
		if resource.IsDir {
			deleteInfoMap[id] = []string{}
			if result := db.Model(&Resource{}).Select("id").Where("pid = ?", id).Find(&sourceIds); result.RowsAffected > 0 {
				if _, err = deleteByIds(sourceIds, db, deleteInfoMap, id); err != nil {
					return
				}
			} else if err = result.Error; err != nil {
				return
			}
		} else {
			deleteInfoMap[pid] = append(deleteInfoMap[pid], id)
		}
	}
	return
}

func (ex *ResourceManagerExecutor) DeleteSpace(ctx context.Context, spaceIds []string) (*model.EmptyStruct, error) {
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
		if err = db.Where("space_id = ?", spaceId).Delete(&Resource{}).Error; err != nil {
			return nil, err
		}
	}
	return &model.EmptyStruct{}, nil
}

func (ex *ResourceManagerExecutor) DescribeFile(ctx context.Context, resourceId string) (*resource.ResourceResponse, error) {
	var (
		info Resource
	)
	db := ex.db.WithContext(ctx)
	if db.Where(Resource{ResourceId: resourceId}).First(&info).RowsAffected == 0 {
		return nil, qerror.ResourceNotExists
	}
	var rsp = &resource.ResourceResponse{
		ResourceId:   resourceId,
		SpaceId:      info.SpaceId,
		ResourceName: info.Name,
		ResourceType: info.Type,
		Size:         info.Size,
		Url:          "hdfs://" + ex.hdfsServer + getHdfsPath(info.SpaceId, resourceId),
	}
	return rsp, nil
}

func checkAndGetFile(path *string) (fileName string, err error) {
	if !strings.HasSuffix(*path, ".jar") {
		err = qerror.InvalidParams.Format("fileName")
		return
	}
	if !strings.HasPrefix(*path, fileSplit) {
		*path = fileSplit + *path
	}
	filePath := *path
	fileName = filePath[strings.LastIndex(filePath, fileSplit)+1:]
	if *path = filePath[:strings.LastIndex(filePath, fileSplit)+1]; strings.EqualFold(*path, fileSplit) {
		return
	}
	dirReg := `^\/(\w+\/?)+$`
	if ok, _ := regexp.Match(dirReg, []byte(*path)); !ok {
		err = qerror.InvalidParams.Format("filePath")
		return
	}
	return
}

func checkDirName(dir string) (dirName string, err error) {
	if dir == fileSplit {
		dirName = fileSplit
		return
	}
	if !strings.HasSuffix(dir, fileSplit) {
		dir = dir + fileSplit
	}
	if !strings.HasPrefix(dir, fileSplit) {
		dir = fileSplit + dir
	}
	dirReg := `^\/(\w+\/?)+$`
	if ok, _ := regexp.Match(dirReg, []byte(dir)); !ok {
		err = qerror.InvalidParams.Format("filePath")
		return
	}
	return
}

func getHdfsPath(spaceId, resourceId string) string {
	return fileSplit + spaceId + fileSplit + resourceId + ".jar"
}
