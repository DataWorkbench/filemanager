package executor

import (
	"context"
	"errors"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/model"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
	"github.com/colinmarc/hdfs"
	"gorm.io/gorm"
)

type FileManagerExecutor struct {
	db          *gorm.DB
	idGenerator *idgenerator.IDGenerator
	logger      *glog.Logger
	hdfsServer  string
}

const (
	TimeFormat = "2006-01-02 15:04:05"
	fileSplit  = "/"
)

type FileInfo struct {
	SpaceID  *string
	FileName *string
	FilePath *string
	HdfsPath *string
	FileType *int32
}

func NewFileManagerExecutor(db *gorm.DB, l *glog.Logger, hdfsServer string) *FileManagerExecutor {
	return &FileManagerExecutor{
		db:          db,
		idGenerator: idgenerator.New(constants.FileMangerIDPrefix),
		logger:      l,
		hdfsServer:  hdfsServer,
	}
}

func (ex *FileManagerExecutor) UploadFile(fu fmpb.FileManager_UploadFileServer) error {
	var info FileManager
	fileId, err := ex.idGenerator.Take()
	if err != nil {
		return err
	}

	if fileId, err = ex.uploadStreamHandler(fu, &info, fileId); err != nil {
		return err
	}
	if err = ex.bindingData(fu.Context(), fileId, ex.hdfsServer, &info); err != nil {
		client, _ := hdfs.New(ex.hdfsServer)
		defer func() {
			_ = client.Close()
		}()
		_ = client.Remove(info.HdfsPath)
		return err
	}
	return fu.SendAndClose(&model.EmptyStruct{})
}

func (ex *FileManagerExecutor) DownloadFile(id string, res fmpb.FileManager_DownloadFileServer) (err error) {
	var (
		fileInfo FileManager
		client   *hdfs.Client
		reader   *hdfs.FileReader
	)
	db := ex.db.WithContext(res.Context())
	if err = db.Where("id = ?", id).First(&fileInfo).Error; err != nil {
		return
	}
	if client, err = hdfs.New(fileInfo.Address); err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()
	if reader, err = client.Open(fileInfo.HdfsPath); err != nil {
		return qerror.ResourceNotExists
	}
	buf := make([]byte, 4096)
	n := 0
	for {
		n, err = reader.Read(buf)
		if err == io.EOF || n == 0 {
			return nil
		}
		if err != nil {
			return
		}
		if err = res.Send(&fmpb.DownloadResponse{Data: buf[:n]}); err != nil {
			return
		}
	}
}

func (ex *FileManagerExecutor) ListFiles(ctx context.Context, spaceId string, limit, offset int32) (rsp []*fmpb.FileInfoResponse, count int64, err error) {
	db := ex.db.WithContext(ctx)
	var (
		isDeleted int32
		files     []*FileManager
	)
	if err = db.Model(&FileManager{}).Select("*").Where("space_id = ?", spaceId).
		Limit(int(limit)).Offset(int(offset)).Order("update_time ASC").
		Scan(&files).Error; err != nil {
		return
	}

	if err = db.Model(&FileManager{}).Where(FileManager{
		SpaceID: spaceId, DeleteTimestamp: &isDeleted,
	}).Count(&count).Error; err != nil {
		return
	}
	for _, file := range files {
		rsp = append(rsp, &fmpb.FileInfoResponse{
			Id:       file.ID,
			SpaceId:  file.SpaceID,
			FileName: file.Name,
			FilePath: file.Path,
			FileType: file.Type,
		})
	}
	return
}

func (ex *FileManagerExecutor) UpdateFile(ctx context.Context, id, name, path string, fileType int32) (*model.EmptyStruct, error) {
	var (
		err       error
		fileInfo  FileManager
		isDeleted int32
	)
	if err = checkFileDir(&path); err != nil {
		return nil, err
	}
	db := ex.db.WithContext(ctx)
	tDb := db
	fileInfo.ID = id
	if result := db.First(&fileInfo); result.RowsAffected == 0 {
		return nil, qerror.ResourceNotExists
	}
	if name != "" {
		fileInfo.Name = name
		tDb = tDb.Where(FileManager{Name: name})
	}
	if path != "" {
		fileInfo.Path = path
		tDb = tDb.Where(FileManager{Path: path})
	}
	if fileType > 0 {
		fileInfo.Type = fileType
	}
	if result := tDb.Debug().Where(FileManager{SpaceID: fileInfo.SpaceID, Address: fileInfo.Address, DeleteTimestamp: &isDeleted}).Find(&FileManager{}); result.RowsAffected > 0 {
		return nil, qerror.ResourceAlreadyExists
	}
	if err = db.Save(&fileInfo).Error; err != nil {
		return nil, err
	}
	return &model.EmptyStruct{}, nil
}

func (ex *FileManagerExecutor) DeleteFiles(ctx context.Context, ids []string) (*model.EmptyStruct, error) {
	var (
		isDeleted   int32
		removePaths []string
		address     string
	)
	db := ex.db.WithContext(ctx)
	if len(ids) == 0 {
		return nil, qerror.InvalidParams.Format("ids")
	}

	tx := db.Begin()
	for _, id := range ids {
		var file FileManager
		if result := db.Where(&FileManager{ID: id, DeleteTimestamp: &isDeleted}).First(&file); result.RowsAffected == 0 {
			return nil, qerror.ResourceNotExists
		}
		removePaths = append(removePaths, file.HdfsPath)
		address = file.Address
	}

	curTime := time.Now()
	deleteTimestamp := int32(curTime.Unix())
	updateTime := curTime.Format(TimeFormat)
	if err := tx.Model(&FileManager{}).Where("delete_timestamp = 0 AND id IN (?)", ids).
		UpdateColumns(&FileManager{DeleteTimestamp: &deleteTimestamp, UpdateTime: updateTime}).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	client, err := hdfs.New(address)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer func() {
		_ = client.Close()
	}()
	for _, removePath := range removePaths {
		if err = client.Remove(removePath); err != nil && errors.Is(&os.PathError{
			Op:   "remove",
			Path: removePath,
			Err:  errors.New("file does not exits"),
		}, err) {
			tx.Rollback()
			return nil, err
		}
	}
	tx.Commit()
	return &model.EmptyStruct{}, nil
}

func (ex *FileManagerExecutor) DeleteAllFiles(ctx context.Context, spaceIds []string) (*model.EmptyStruct, error) {
	var (
		ids []string
	)
	db := ex.db.WithContext(ctx)
	if db.Model(&FileManager{}).Where("delete_timestamp = 0 AND space_id IN (?)", spaceIds).Select("id").Find(&ids).RowsAffected == 0 {
		return nil, qerror.ResourceNotExists
	}

	tx := db.Begin()
	curTime := time.Now()
	deleteTimestamp := int32(curTime.Unix())
	updateTime := curTime.Format(TimeFormat)
	if err := tx.Model(&FileManager{}).Where("space_id IN (?) and delete_timestamp = 0", spaceIds).
		UpdateColumns(&FileManager{DeleteTimestamp: &deleteTimestamp, UpdateTime: updateTime}).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	client, err := hdfs.New(ex.hdfsServer)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer func() {
		_ = client.Close()
	}()
	for _, removePath := range spaceIds {
		removePath = fileSplit + removePath
		if err = client.Remove(removePath); err != nil && errors.Is(&os.PathError{
			Op:   "remove",
			Path: removePath,
			Err:  errors.New("file does not exits"),
		}, err) {
			tx.Rollback()
			return nil, err
		}
	}
	tx.Commit()
	return &model.EmptyStruct{}, nil
}

func (ex *FileManagerExecutor) DescribeFile(ctx context.Context, id string) (*fmpb.FileInfoResponse, error) {
	var (
		file      FileManager
		isDeleted int32
	)
	db := ex.db.WithContext(ctx)
	if db.Where(FileManager{ID: id, DeleteTimestamp: &isDeleted}).First(&file).RowsAffected == 0 {
		return nil, qerror.ResourceNotExists
	}
	var rsp = &fmpb.FileInfoResponse{
		Id:       id,
		SpaceId:  file.SpaceID,
		FileName: file.Name,
		FilePath: file.Path,
		FileType: file.Type,
		Url:      "hdfs://" + file.Address + file.HdfsPath,
	}
	return rsp, nil
}

func (ex *FileManagerExecutor) uploadStreamHandler(fu fmpb.FileManager_UploadFileServer, fileInfo *FileManager, fileId string) (newFileId string, err error) {
	var (
		client *hdfs.Client
		writer *hdfs.FileWriter
		recv   *fmpb.UploadFileRequest
	)
	if recv, err = fu.Recv(); err != nil {
		return
	}

	if recv != nil {
		//TODO 测试可以传id
		if recv.Id != "" {
			fileId = recv.Id
		}
		newFileId = fileId
		fileInfo.SpaceID = recv.SpaceId
		fileInfo.Type = recv.FileType
		fileInfo.Name = recv.FileName
		hdfsFileDir := fileSplit + recv.SpaceId + fileSplit
		fileInfo.HdfsPath = hdfsFileDir + fileId + "_" + fileInfo.Name
		if err = checkFileDir(&recv.FilePath); err != nil {
			return
		}
		fileInfo.Path = recv.FilePath
		var isDeleted int32
		db := ex.db.WithContext(fu.Context())
		//TODO 数据库检查是否重复创建
		if result := db.Where(FileManager{
			SpaceID:         fileInfo.SpaceID,
			Name:            fileInfo.Name,
			Path:            fileInfo.Path,
			Address:         ex.hdfsServer,
			DeleteTimestamp: &isDeleted,
		}).Find(&FileManager{}); result.RowsAffected > 0 {
			err = qerror.ResourceAlreadyExists
			return
		}
		//TODO hdfs路径

		if client, err = hdfs.New(ex.hdfsServer); err != nil {
			return
		}
		//TODO 释放client
		defer func() {
			if client != nil {
				_ = client.Close()
			}
		}()
		if writer, err = client.Create(fileInfo.HdfsPath); err != nil {
			if _, ok := err.(*os.PathError); ok {
				if err = client.MkdirAll(hdfsFileDir, 0777); err != nil {
					return
				}
				if writer, err = client.Create(fileInfo.HdfsPath); err != nil {
					return
				}
			} else {
				return
			}
		}
		//TODO 释放writer
		defer func() {
			if err2 := writer.Close(); err2 != nil {
				_ = client.Remove(fileInfo.HdfsPath)
			}
		}()
		if _, err = writer.Write(recv.Data); err != nil {
			_ = client.Remove(fileInfo.HdfsPath)
			return
		}
		for {
			recv, err = fu.Recv()
			if err == io.EOF {
				err = nil
				return
			}

			if err != nil {
				_ = client.Remove(fileInfo.HdfsPath)
				return
			}
			if _, err = writer.Write(recv.Data); err != nil {
				_ = client.Remove(fileInfo.HdfsPath)
				return
			}
		}
	}
	err = qerror.Internal
	return
}

func (ex *FileManagerExecutor) bindingData(ctx context.Context, id, fileAddress string, info *FileManager) (err error) {
	db := ex.db.WithContext(ctx)
	var isDeleted int32 = 0
	updateTime := time.Now().Format(TimeFormat)
	fileManger := FileManager{
		ID:              id,
		SpaceID:         info.SpaceID,
		Name:            info.Name,
		HdfsPath:        info.HdfsPath,
		Path:            info.Path,
		Address:         fileAddress,
		CreateTime:      updateTime,
		UpdateTime:      updateTime,
		DeleteTimestamp: &isDeleted,
	}
	if info.Type > 0 {
		fileManger.Type = info.Type
	}
	if err = db.Create(&fileManger).Error; err != nil {
		return
	}
	return
}

func checkFileDir(path *string) (err error) {
	if *path == "" {
		*path = fileSplit
		return
	}
	if *path == fileSplit {
		return
	}
	*path = strings.ReplaceAll(*path, "\\", fileSplit)
	var match bool
	dirReg := `^\/(\w+\/?)+$`
	if !strings.HasSuffix(*path, fileSplit) {
		*path = *path + fileSplit
	}
	if !strings.HasPrefix(*path, fileSplit) {
		*path = fileSplit + *path
	}
	if match, err = regexp.Match(dirReg, []byte(*path)); err != nil {
		return
	} else if !match {
		err = qerror.InvalidParams.Format("filePath")
	}
	return
}
