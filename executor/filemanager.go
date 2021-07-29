package executor

import (
	"context"
	"fmt"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (ex *FileManagerExecutor) Upload(fu fmpb.FileManager_UploadServer) error {
	var info = FileInfo{}
	fileId, err := ex.idGenerator.Take()
	if err != nil {
		return err
	}
	err = ex.uploadStreamHandler(fu, &info, fileId)
	if err != nil {
		return err
	}
	if err = ex.bindingData(fu.Context(), fileId, *info.SpaceID, *info.FileName, *info.FilePath, ex.hdfsServer, *info.FileType); err != nil {
		return bindingErrorHandler(ex.hdfsServer, *info.FilePath, err)
	}

	return fu.SendAndClose(&fmpb.FileInfoResponse{
		ID:          fileId,
		SpaceID:     *info.SpaceID,
		FileName:    *info.FileName,
		FilePath:    *info.FilePath,
		FileType:    *info.FileType,
		HdfsAddress: ex.hdfsServer,
		URL:         getHdfsUrl(ex.hdfsServer, *info.SpaceID, fileId, *info.FileName),
	})
}

func (ex *FileManagerExecutor) Download(id string, res fmpb.FileManager_DownloadServer) (err error) {
	var (
		fileInfo FileManager
		client   *hdfs.Client
		reader   *hdfs.FileReader
		filePath string
	)
	db := ex.db.WithContext(res.Context())
	if err = db.Where("id = ?", id).First(&fileInfo).Error; err != nil {
		return
	}
	filePath = fileSplit + fileInfo.SpaceID + fileSplit + fileInfo.ID + "_" + fileInfo.HdfsName
	if client, err = hdfs.New(fileInfo.Address); err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()
	if reader, err = client.Open(filePath); err != nil {
		return
	}
	buf := make([]byte, 4096)
	n := 0
	for {
		n, err = reader.Read(buf)
		if err == io.EOF || n == 0 {
			return io.EOF
		}
		if err != nil {
			return
		}
		if err = res.Send(&fmpb.DownloadResponse{Data: buf[:n]}); err != nil {
			return
		}
	}
}

func (ex *FileManagerExecutor) GetFile(ctx context.Context, id, spaceId, name, path string, fileType int32) (*fmpb.FileListResponse, error) {
	db := ex.db.WithContext(ctx)
	var (
		isDeleted int32 = 0
		file      FileManager
		files     []*FileManager
		data      []*fmpb.FileInfoResponse
		total     int32
	)
	if id != "" {
		result := db.Where(FileManager{ID: id, DeleteTimestamp: &isDeleted}).First(&file)
		if result.RowsAffected == 0 {
			return nil, status.Errorf(codes.NotFound, "file is not exist")
		}
		total = int32(result.RowsAffected)
		data = append(data, &fmpb.FileInfoResponse{
			ID:          file.ID,
			SpaceID:     file.SpaceID,
			FileName:    file.Name,
			FilePath:    file.Path,
			FileType:    file.Type,
			HdfsAddress: file.Address,
			URL:         getHdfsUrl(file.Address, file.SpaceID, file.ID, file.Name),
		})
	} else {
		//TODO 根据文件类型查询
		if fileType > 0 {
			if fileType > 2 {
				return nil, status.Errorf(codes.InvalidArgument, "file type does not match 1 2")
			}
			db = db.Where(FileManager{Type: fileType})
		}
		//TODO 根据文件名称匹配
		if name != "" {
			db = db.Where(fmt.Sprintf("name LIKE '%s'", "%"+name+"%"))
		}
		//TODO 根据文件路径匹配
		if path != "" {
			if err := checkFileDir(&path); err != nil {
				return nil, err
			}
			db = db.Where(fmt.Sprintf("path LIKE '%s'", path+"%"))
		}

		result := db.Where(FileManager{SpaceID: spaceId, DeleteTimestamp: &isDeleted}).Find(&files)
		if result.RowsAffected == 0 {
			return nil, status.Errorf(codes.NotFound, "file is not exist")
		}
		total = int32(result.RowsAffected)
		for _, f := range files {
			data = append(data, &fmpb.FileInfoResponse{
				ID:          f.ID,
				SpaceID:     f.SpaceID,
				FileName:    f.Name,
				FilePath:    f.Path,
				FileType:    f.Type,
				HdfsAddress: f.Address,
				URL:         getHdfsUrl(f.Address, f.SpaceID, f.ID, f.Name),
			})
		}
	}
	return &fmpb.FileListResponse{
		Total: total,
		Data:  data,
	}, nil
}

func (ex *FileManagerExecutor) Update(ctx context.Context, id, name, path string, fileType int32) (*model.EmptyStruct, error) {
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
	if result := db.Find(&fileInfo); result.RowsAffected == 0 {
		return nil, status.Errorf(codes.NotFound, "file not exist")
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
		return nil, status.Errorf(codes.AlreadyExists, fmt.Sprintf("%s already exist", name))
	}
	if err = db.Save(&fileInfo).Error; err != nil {
		return nil, err
	}
	return &model.EmptyStruct{}, nil
}

func (ex *FileManagerExecutor) Delete(ctx context.Context, id, spaceId string) (*model.EmptyStruct, error) {
	var (
		isDeleted  int32
		file       FileManager
		removePath string
	)
	db := ex.db.WithContext(ctx)
	curTime := time.Now()
	deleteTimestamp := int32(curTime.Unix())
	updateTime := curTime.Format(TimeFormat)
	tx := db.Begin()
	if id != "" {
		if result := db.Where(&FileManager{ID: id, DeleteTimestamp: &isDeleted}).First(&file); result.RowsAffected == 0 {
			return nil, status.Errorf(codes.NotFound, "file not exist")
		}
		if result := tx.Where(&FileManager{ID: id, DeleteTimestamp: &isDeleted}).
			Updates(&FileManager{DeleteTimestamp: &deleteTimestamp, UpdateTime: updateTime}); result.Error != nil {
			tx.Rollback()
			return nil, result.Error
		}
		removePath = fileSplit + file.SpaceID + fileSplit + file.ID + "_" + file.HdfsName
	} else {
		if result := db.Where(&FileManager{SpaceID: spaceId, DeleteTimestamp: &isDeleted}).Find(&file); result.RowsAffected == 0 {
			return nil, status.Errorf(codes.NotFound, "file not exist")
		}
		if result := tx.Model(&FileManager{}).Where(FileManager{SpaceID: spaceId, DeleteTimestamp: &isDeleted}).
			UpdateColumns(&FileManager{DeleteTimestamp: &deleteTimestamp, UpdateTime: updateTime}); result.Error != nil {
			tx.Rollback()
			return nil, result.Error
		}
		removePath = fileSplit + file.SpaceID + fileSplit
	}
	client, err := hdfs.New(file.Address)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer func() {
		_ = client.Close()
	}()
	if err = client.Remove(removePath); err != nil {
		tx.Rollback()
		return nil, err
	}
	tx.Commit()
	return &model.EmptyStruct{}, nil
}

func (ex *FileManagerExecutor) uploadStreamHandler(fu fmpb.FileManager_UploadServer, fileInfo *FileInfo, fileId string) (err error) {
	var (
		client *hdfs.Client
		writer *hdfs.FileWriter
		recv   *fmpb.UploadFileRequest
	)
	if recv, err = fu.Recv(); err != nil {
		return
	}

	if recv != nil {
		fileInfo.SpaceID = &recv.SpaceID
		fileInfo.FilePath = &recv.FilePath
		fileInfo.FileType = &recv.FileType
		fileInfo.FileName = &recv.FileName
		if err = checkFileDir(&recv.FilePath); err != nil {
			return
		}
		var isDeleted int32
		db := ex.db.WithContext(fu.Context())
		//TODO 数据库检查是否重复创建
		if result := db.Where(FileManager{
			SpaceID:         *fileInfo.SpaceID,
			Name:            *fileInfo.FileName,
			Path:            *fileInfo.FilePath,
			Address:         ex.hdfsServer,
			DeleteTimestamp: &isDeleted,
		}).Find(&FileManager{}); result.RowsAffected > 0 {
			return status.Errorf(codes.AlreadyExists, fmt.Sprintf("%s already exist", *fileInfo.FileName))
		}
		//TODO hdfs路径
		hdfsFileDir := fileSplit + recv.SpaceID + fileSplit
		hdfsFilePath := hdfsFileDir + fileId + "_" + recv.FileName
		if client, err = hdfs.New(ex.hdfsServer); err != nil {
			return
		}
		//TODO 释放client
		defer func() {
			if client != nil {
				_ = client.Close()
			}
		}()
		if writer, err = client.Create(hdfsFilePath); err != nil {
			if _, ok := err.(*os.PathError); ok {
				if err = client.MkdirAll(hdfsFileDir, 0777); err != nil {
					return
				}
				if writer, err = client.Create(hdfsFilePath); err != nil {
					return
				}
			} else {
				return
			}
		}
		//TODO 释放writer
		defer func() {
			if err2 := writer.Close(); err2 != nil {
				_ = client.Remove(hdfsFilePath)
			}
		}()
		if _, err = writer.Write(recv.Data); err != nil {
			_ = client.Remove(hdfsFilePath)
			return
		}
		for {
			recv, err = fu.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				_ = client.Remove(hdfsFilePath)
				return
			}
			if _, err = writer.Write(recv.Data); err != nil {
				_ = client.Remove(hdfsFilePath)
				return
			}
		}
	}
	return status.Errorf(codes.InvalidArgument, "error received message")
}

func (ex *FileManagerExecutor) bindingData(ctx context.Context, id, spaceId, fileName, filePath, fileAddress string, fileType int32) (err error) {
	db := ex.db.WithContext(ctx)
	var isDeleted int32 = 0
	updateTime := time.Now().Format(TimeFormat)
	fileManger := FileManager{
		ID:              id,
		SpaceID:         spaceId,
		Name:            fileName,
		HdfsName:        fileName,
		Path:            filePath,
		Address:         fileAddress,
		CreateTime:      updateTime,
		UpdateTime:      updateTime,
		DeleteTimestamp: &isDeleted,
	}
	if fileType > 0 {
		fileManger.Type = fileType
	}
	if err = db.Create(&fileManger).Error; err != nil {
		return
	}
	return
}

func getHdfsUrl(address, spaceId, fileId, fileName string) string {
	return fmt.Sprintf("hdfs://%s/%s/%s_%s", address, spaceId, fileId, fileName)
}

func bindingErrorHandler(address, filePath string, err error) error {
	client, _ := hdfs.New(address)
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()
	_ = client.Remove(filePath)
	return err
}

func checkFileDir(fileDir *string) (err error) {
	if *fileDir == "" {
		*fileDir = fileSplit
	}
	if *fileDir == fileSplit {
		return
	}
	*fileDir = strings.ReplaceAll(*fileDir, "\\", fileSplit)
	var match bool
	dirReg := `^\/(\w+\/?)+$`
	if !strings.HasSuffix(*fileDir, fileSplit) {
		*fileDir = *fileDir + fileSplit
	}
	if !strings.HasPrefix(*fileDir, fileSplit) {
		*fileDir = fileSplit + *fileDir
	}
	if match, err = regexp.Match(dirReg, []byte(*fileDir)); err != nil {
		return
	} else if !match {
		err = status.Error(codes.InvalidArgument, "fileDir not match error")
	}
	return
}
