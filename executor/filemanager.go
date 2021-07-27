package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
	"github.com/DataWorkbench/gproto/pkg/model"
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
	ID       *string
	SpaceID  *string
	FileName *string
	FileDir  *string
	UserId   *string
	Address  *string
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

func (ex *FileManagerExecutor) Upload(ctx context.Context, id, spaceID, fileName, fileDir string, fType int32) (err error) {
	if err = checkFileDir(&fileDir); err != nil {
		return
	}

	fileDir = fileSplit + spaceID + fileDir
	filePath := fileDir + id + "-" + fileName
	if err = ex.bindingData(ctx, id, spaceID, fileName, fileDir, ex.hdfsServer, fType); err != nil {
		err = bindingErrorHandler(ex.hdfsServer, filePath, err)
	}
	return
}

func (ex *FileManagerExecutor) UploadStream(fu fmpb.FileManager_UploadStreamServer) error {
	var info = FileInfo{}
	err := ex.uploadStreamHandler(fu, &info)
	if err != nil {
		return err
	}
	if err = ex.bindingData(fu.Context(), *info.ID, *info.SpaceID, *info.FileName, *info.FileDir, *info.Address, *info.FileType); err != nil && err != io.EOF {
		return bindingErrorHandler(*info.Address, *info.FilePath, err)
	}
	return fu.SendAndClose(&model.EmptyStruct{})
}

func (ex *FileManagerExecutor) uploadStreamHandler(fu fmpb.FileManager_UploadStreamServer, fileInfo *FileInfo) (err error) {
	var (
		client *hdfs.Client
		writer *hdfs.FileWriter
		recv   *fmpb.UploadRequest
	)
	if recv, err = fu.Recv(); err != nil {
		return
	}

	if recv != nil {
		fileInfo.ID = &recv.ID
		fileInfo.SpaceID = &recv.SpaceID
		fileInfo.FileDir = &recv.FileDir
		fileInfo.Address = &ex.hdfsServer
		fileInfo.FileType = &recv.FileType
		if err = checkFileDir(&recv.FileDir); err != nil {
			return
		}
		fileInfo.FileName = &recv.FileName
		//TODO hdfs路径
		fileDir := fileSplit + recv.SpaceID + fileSplit
		filePath := fileDir + recv.ID + "_" + recv.FileName
		fileInfo.FilePath = &filePath
		if client, err = hdfs.New(ex.hdfsServer); err != nil {
			return
		}
		//TODO 释放client
		defer func() {
			if client != nil {
				_ = client.Close()
			}
		}()
		if writer, err = client.Create(filePath); err != nil {
			if _, ok := err.(*os.PathError); ok {
				if err = client.MkdirAll(fileDir, 0777); err != nil {
					return
				}
				if writer, err = client.Create(filePath); err != nil {
					return
				}
			} else {
				return
			}
		}
		//TODO 释放writer
		defer func() {
			if err2 := writer.Close(); err2 != nil {
				_ = client.Remove(filePath)
			}
		}()
		if _, err = writer.Write(recv.Data); err != nil {
			_ = client.Remove(filePath)
			return
		}
		for {
			recv, err = fu.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				_ = client.Remove(filePath)
				return
			}
			if _, err = writer.Write(recv.Data); err != nil {
				_ = client.Remove(filePath)
				return
			}
		}
	}
	return status.Errorf(codes.InvalidArgument, "error receive message")
}

func (ex *FileManagerExecutor) bindingData(ctx context.Context, id, spaceID, fileName, fileDir, address string, fileType int32) (err error) {
	db := ex.db.WithContext(ctx)
	tx := db.Begin()
	var dirId string
	if dirId, err = createDirs(tx, ex.idGenerator, spaceID, fileDir, address); err != nil {
		tx.Rollback()
		return
	}
	if err = createFile(tx, id, dirId, fileName, fileType); err != nil {
		tx.Rollback()
		return
	}
	if err = tx.Commit().Error; err != nil {
		tx.Rollback()
		return
	}
	return nil
}

func (ex *FileManagerExecutor) DownloadStream(id string, res fmpb.FileManager_DownloadStreamServer) (err error) {
	var (
		fileInfo FileManager
		dirInfo  FileDir
		client   *hdfs.Client
		reader   *hdfs.FileReader
		filePath string
	)
	db := ex.db.WithContext(res.Context())
	if err = db.Where("id = ?", id).First(&fileInfo).Error; err != nil {
		return
	}
	if err = db.Where("id = ?", fileInfo.FileDirID).First(&dirInfo).Error; err != nil {
		return
	}
	filePath = fileSplit + dirInfo.SpaceID + fileSplit + fileInfo.ID + "_" + fileInfo.Name
	if client, err = hdfs.New(dirInfo.Address); err != nil {
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
		if err = res.Send(&fmpb.DownloadReply{Data: buf[:n]}); err != nil {
			return
		}
	}
}

func (ex *FileManagerExecutor) GetDirList(ctx context.Context, spaceId string) (*fmpb.GetDirListReply, error) {
	var (
		fileDirInfos []*FileDir
		maxLevel     int
	)
	db := ex.db.WithContext(ctx)
	if err := db.Raw("select max(level) from file_dir where delete_timestamp = ? group by level", 0).Scan(&maxLevel).Error; err != nil {
		return nil, err
	}
	var sb strings.Builder
	db = db.Preload("SubFile")
	for i := 0; i < maxLevel; i++ {
		var fb = "SubFile"
		if i == maxLevel-1 {
			sb.WriteString("SubDir")
			fb = sb.String() + "." + fb
		} else {
			sb.WriteString("SubDir.")
			fb = sb.String() + fb
		}
		db = db.Preload(fb)
	}
	result := db.Debug().Preload(sb.String()).Where("address = ? and space_id = ? and delete_timestamp = ? and level = ?", ex.hdfsServer, spaceId, 0, 1).Find(&fileDirInfos)
	if result.Error != nil && errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.Internal, result.Error.Error())
	}
	jsonRes, err := json.Marshal(&fileDirInfos)
	if err != nil {
		return nil, err
	}
	return &fmpb.GetDirListReply{JsonData: string(jsonRes)}, nil
}

func (ex *FileManagerExecutor) DeleteFile(ctx context.Context, fileId string) (err error) {
	var (
		fileInfo FileManager
		dirInfo  FileDir
		client   *hdfs.Client
		filePath string
	)

	db := ex.db.WithContext(ctx)
	if err = db.Where("id = ?", fileId).First(&fileInfo).Error; err != nil {
		return
	}

	if err = db.Where("id = ?", fileInfo.FileDirID).First(&dirInfo).Error; err != nil {
		return
	}

	if client, err = hdfs.New(dirInfo.Address); err != nil {
		return
	}

	defer func() {
		_ = client.Close()
	}()

	filePath = fileSplit + dirInfo.SpaceID + fileSplit + fileInfo.ID + "_" + fileInfo.Name
	tx := db.Begin()

	if err = tx.Model(&FileManager{}).Where("id = ?", fileId).Updates(FileManager{
		DeleteTimestamp: int32(time.Now().Unix()),
	}).Error; err != nil {
		tx.Rollback()
		return
	}

	if err = client.Remove(filePath); err != nil {
		return
	}

	tx.Commit()
	return
}

func (ex *FileManagerExecutor) DeleteDir(ctx context.Context, dirId string) (err error) {
	var (
		dirInfo  FileDir
		dirList  []*FileDir
		fileList []*FileManager
		client   *hdfs.Client
	)
	db := ex.db.WithContext(ctx)
	if err = db.Where("id = ?", dirId).First(&dirInfo).Error; err != nil {
		return
	} else if dirInfo.DeleteTimestamp != 0 {
		return status.Error(codes.AlreadyExists, "dir already exists")
	}

	tx := db.Begin()

	//TODO 删除当前文件夹下的文件
	if err = tx.Model(&FileManager{}).Where(&FileManager{FileDirID: dirId, DeleteTimestamp: 0}).UpdateColumns(FileManager{
		DeleteTimestamp: int32(time.Now().Unix()),
		UpdateTime:      time.Now().Format(TimeFormat),
	}).Error; err != nil {
		tx.Rollback()
		return
	}

	//TODO 删除当前文件夹所有自文件夹及文件
	if err = deleteChildDirAndFile(tx, dirId, dirInfo.Address, dirInfo.SpaceID); err != nil {
		tx.Rollback()
		return
	}

	//TODO 删除当前文件夹
	if err = tx.Model(&FileDir{}).Where("id = ?", dirId).Updates(FileDir{
		DeleteTimestamp: int32(time.Now().Unix()),
		UpdateTime:      time.Now().Format(TimeFormat),
	}).Error; err != nil {
		tx.Rollback()
		return
	}

	if client, err = hdfs.New(dirInfo.Address); err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()

	if err = db.Debug().Raw(fmt.Sprintf(`select * from file_dir where space_id = '%v' and address = '%v' and delete_timestamp = %d and url like '%s'`, dirInfo.SpaceID, dirInfo.Address, 0, dirInfo.Url+"%")).Find(&dirList).Error; err != nil {
		return
	}
	for _, dir := range dirList {
		if err = db.Where("dir_id = ? and delete_timestamp = ?", dir.ID, 0).Find(&fileList).Error; err != nil {
			return
		}
		for _, file := range fileList {
			//TODO 删除hdfs上的文件夹
			if err = client.Remove(fileSplit + dir.SpaceID + fileSplit + file.ID + "_" + file.Name); err != nil {
				return
			}
		}
	}
	tx.Commit()
	return
}

func (ex *FileManagerExecutor) GetFileById(ctx context.Context, id string) (res *fmpb.FileInfoReply, err error) {
	var (
		fileInfo FileManager
		dirInfo  FileDir
	)
	db := ex.db.WithContext(ctx)
	if err = db.Where("id = ? and delete_timestamp = ?", id, 0).First(&fileInfo).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "file not exist")
		}
		return
	}
	if err = db.Where("id = ?", fileInfo.FileDirID).First(&dirInfo).Error; err != nil {
		return
	}
	res = &fmpb.FileInfoReply{
		Name:     fileInfo.Name,
		FileType: fileInfo.FileType,
		URL:      "hdfs://" + ex.hdfsServer + fileSplit + dirInfo.SpaceID + fileSplit + fileInfo.ID + "_" + fileInfo.Name,
		Path:     dirInfo.Url,
	}
	return
}

func (ex *FileManagerExecutor) GetSubDirFile(ctx context.Context, id string) (*fmpb.GetSubDirListReply, error) {
	var (
		dirInfo          FileDir
		childDirs        []*FileDir
		childFiles       []*FileManager
		dirListResponse  []*fmpb.GetSubDirListReply_Dir
		fileListResponse []*fmpb.GetSubDirListReply_File
	)
	db := ex.db.WithContext(ctx)
	if result := db.Where("id = ? and delete_timestamp = 0", id).First(&dirInfo); result.RowsAffected == 0 {
		return nil, status.Errorf(codes.NotFound, "文件夹不存在")
	}
	db.Where(&FileDir{ParentID: &id}).Find(&childDirs)
	db.Where(&FileManager{FileDirID: id}).Find(&childFiles)
	for _, dir := range childDirs {
		dirListResponse = append(dirListResponse, &fmpb.GetSubDirListReply_Dir{
			ID:   dir.ID,
			Name: dir.Name,
		})
	}
	for _, file := range childFiles {
		fileListResponse = append(fileListResponse, &fmpb.GetSubDirListReply_File{
			ID:   file.ID,
			Name: file.Name,
			URL:  "hdfs://" + dirInfo.Address + fileSplit + dirInfo.SpaceID + fileSplit + file.ID + "_" + file.Name,
		})
	}
	res := fmpb.GetSubDirListReply{
		ID:         dirInfo.ID,
		Name:       dirInfo.Name,
		ChileDirs:  dirListResponse,
		ChildFiles: fileListResponse,
	}
	return &res, nil
}

func (ex *FileManagerExecutor) UpdateFile(ctx context.Context, id string, name string, t int32, path string) (*model.EmptyStruct, error) {
	var (
		fileInfo FileManager
		dirInfo  FileDir
	)
	if err := checkFileDir(&path); err != nil {
		return nil, err
	}
	db := ex.db.WithContext(ctx)
	if result := db.Where("id = ? and delete_timestamp = 0", id).First(&fileInfo); result.RowsAffected == 0 {
		return nil, status.Errorf(codes.NotFound, "文件不存在")
	}
	if result := db.Where("id = ? and delete_timestamp = 0", fileInfo.FileDirID).First(&dirInfo); result.RowsAffected == 0 {
		return nil, status.Error(codes.Internal, "文件夹不存在")
	}
	tx := db.Begin()
	if path != "" {
		dirId, err := createDirs(tx, ex.idGenerator, dirInfo.SpaceID, path, dirInfo.Address)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		fileInfo.FileDirID = dirId
	}
	if name != "" {
		fileInfo.Name = name
	}
	if t != 0 {
		fileInfo.FileType = t
	}
	if err := tx.Save(&fileInfo).Error; err != nil {
		tx.Rollback()
		return nil, err
	}
	tx.Commit()
	return &model.EmptyStruct{}, nil
}

func deleteChildDirAndFile(tx *gorm.DB, parent, address, spaceId string) (err error) {
	if parent == "" {
		return
	}
	var tInfo []*FileDir
	//TODO 找到同集群，同工作空间 的所有子文件夹
	if err = tx.Where("address = ? and space_id = ? and parent = ? and delete_timestamp = ?", address, spaceId, parent, 0).Find(&tInfo).Error; err != nil {
		return
	}
	//TODO 遍历子文件夹，进行递归删除
	for _, v := range tInfo {
		//TODO 删除子文件夹下文件
		if err = tx.Model(&FileManager{}).Where("dir_id = ? and delete_timestamp = ?", v.ID, 0).UpdateColumns(FileManager{
			DeleteTimestamp: int32(time.Now().Unix()),
			UpdateTime:      time.Now().Format(TimeFormat),
		}).Error; err != nil {
			tx.Rollback()
			return
		}
		//TODO 删除子文件夹
		if err = tx.Model(&v).Updates(FileDir{
			DeleteTimestamp: int32(time.Now().Unix()),
			UpdateTime:      time.Now().Format(TimeFormat),
		}).Error; err != nil {
			tx.Rollback()
			return
		}
		if err = deleteChildDirAndFile(tx, v.ID, address, spaceId); err != nil {
			return
		}
	}
	return
}

func bindingErrorHandler(address, filePath string, err error) (bErr error) {
	client, err2 := hdfs.New(address)
	//TODO 释放client
	defer func() {
		if client != nil {
			if err4 := client.Close(); err4 != nil {
				bErr = status.Errorf(codes.Internal, "error binding (%v) error close client (%v)", bErr, err4)
			}
		}
	}()
	if err2 != nil {
		bErr = status.Errorf(codes.Internal, "error binding data (%v); error opening hdfs client (%v)", err, err2)
		return
	}
	if err3 := client.Remove(filePath); err3 != nil {
		bErr = status.Errorf(codes.Internal, "error binding data (%v); error deleting file (%v)", err, err3)
	}
	return
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

func createDirs(tx *gorm.DB, generator *idgenerator.IDGenerator, spaceId, fileDir, address string) (dirId string, err error) {
	var (
		tree   *FileTree
		values []FileDir
	)

	if tree, err = NewFileTree(fileDir, generator); err != nil {
		return
	}
	if err = tree.TravelTree(func(tree *FileTree) (err error) {
		var parent string

		if tree.pre != nil {
			parent = tree.pre.id
		}

		var dir FileDir
		result := tx.Where("address = ? and space_id = ? and url = ? and delete_timestamp = ?", address, spaceId, tree.path, 0).Find(&dir)
		if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			err = result.Error
			return
		}
		if result.RowsAffected == 0 {
			dir = FileDir{
				ID:         tree.id,
				Name:       tree.name,
				SpaceID:    spaceId,
				Url:        tree.path,
				Level:      tree.level,
				Address:    address,
				UpdateTime: time.Now().Format(TimeFormat),
				CreateTime: time.Now().Format(TimeFormat),
			}
			if parent != "" {
				dir.ParentID = &parent
			}
			values = append(values, dir)
		} else {
			tree.id = dir.ID
		}
		if tree.next == nil {
			dirId = tree.id
		}
		return
	}); err != nil {
		return
	}

	if len(values) > 0 {
		if l := len(values) / 5; l > 0 {
			if err = tx.CreateInBatches(&values, l).Error; err != nil {
				return
			}
		} else {
			if err = tx.Create(&values).Error; err != nil {
				return
			}
		}
	}
	return
}

func createFile(tx *gorm.DB, id string, dirId string, fileName string, fileType int32) (err error) {
	var (
		info FileManager
	)
	//TODO 二次验证是否创建过文件，防止创建重复文件
	if result := tx.Where("name = ? and dir_id = ? and delete_timestamp = ?", fileName, dirId, 0).Find(&info); result.Error != nil {
		return result.Error
	} else if result.RowsAffected != 0 {
		return status.Error(codes.AlreadyExists, "file already exists")
	}
	info.ID = id
	info.Name = fileName
	info.FileType = fileType
	info.CreateTime = time.Now().Format(TimeFormat)
	info.UpdateTime = info.CreateTime
	info.FileDirID = dirId
	err = tx.Create(&info).Error
	return
}
