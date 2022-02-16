package controller

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"

	"github.com/DataWorkbench/common/lib/storeio"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcstoreio"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
	"github.com/DataWorkbench/resourcemanager/options"
	"github.com/colinmarc/hdfs/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StoreIo struct {
	pbsvcstoreio.UnimplementedStoreIOServer
}

func (x *StoreIo) getRootDir(spaceId string) string {
	return storeio.GenerateFileRootDir(spaceId)
}

func (x *StoreIo) getFilePath(spaceId string, resourceId string, version string) string {
	return storeio.GenerateFilePath(spaceId, resourceId, version)
}

func (x *StoreIo) ensureRootDirExists(ctx context.Context, spaceId string) (err error) {
	rootDir := x.getRootDir(spaceId)
	client := options.HDFSClient

	_, err = client.Stat(ctx, rootDir)
	if err == nil {
		return
	}
	_, ok := err.(*os.PathError)
	if ok {
		err = client.MkdirAll(ctx, rootDir, 0777)
	}
	if err != nil {
		return
	}
	return
}

func (x *StoreIo) WriteFileData(req pbsvcstoreio.StoreIO_WriteFileDataServer) (err error) {
	var (
		writer *hdfs.FileWriter
		recv   *pbrequest.WriteFileData
	)

	client := options.HDFSClient
	ctx := req.Context()
	lg := glog.FromContext(ctx)

	// Receive first stream data. Only metadata. No data.
	if recv, err = req.Recv(); err != nil {
		return err
	}

	// For stream API. not invoker `Validate` in interceptor.
	if err = recv.Validate(); err != nil {
		return
	}

	if len(recv.Data) != 0 {
		lg.Error().Msg("cannot sent data in first stream").Fire()
		return qerror.Internal
	}
	resourceSize := recv.Size

	if err = x.ensureRootDirExists(ctx, recv.SpaceId); err != nil {
		return err
	}

	filePath := x.getFilePath(recv.SpaceId, recv.FileId, recv.Version)
	writer, err = client.CreateFileForWrite(ctx, filePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = writer.Close()

		if err != nil {
			_ = client.Remove(ctx, filePath)
		}
	}()

	h := md5.New()

	var receiveSize int64
	var written int

	for {
		recv, err = req.Recv()
		if err == io.EOF {
			if receiveSize != resourceSize {
				lg.Warn().Msg("file message lose").String("file id", recv.FileId).Fire()
				return qerror.Internal
			}
			err = nil
			b := h.Sum(nil)
			md5Hex := make([]byte, hex.EncodedLen(len(b)))
			hex.Encode(md5Hex, b)
			eTag := string(md5Hex)
			return req.SendAndClose(&pbresponse.WriteFileData{Etag: eTag})
		}
		if err != nil {
			lg.Error().Msg("receive data failed").Error("error", err).Fire()
			return err
		}
		// Update md5 sum.
		h.Write(recv.Data)
		written, err = writer.Write(recv.Data)
		if err != nil {
			lg.Warn().Msg("write data failed").Error("error", err).Fire()
			return err
		}
		// count total size,provided the file size right.
		receiveSize += int64(written)
	}
}
func (x *StoreIo) ReadFileData(req *pbrequest.ReadFileData, reply pbsvcstoreio.StoreIO_ReadFileDataServer) (err error) {
	var (
		reader *hdfs.FileReader
	)

	client := options.HDFSClient
	ctx := reply.Context()

	filePath := x.getFilePath(req.SpaceId, req.FileId, req.Version)
	if reader, err = client.OpenFileForRead(ctx, filePath); err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()

	n := 0
	buf := make([]byte, 4096)
	for {
		n, err = reader.Read(buf)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil {
			return err
		}
		data := buf[:n]
		err = reply.Send(&pbresponse.ReadFileData{Data: data})
		if err != nil {
			return err
		}
	}

	return
}
func (x *StoreIo) DeleteFileData(ctx context.Context, req *pbrequest.DeleteFileData) (*pbmodel.EmptyStruct, error) {
	client := options.HDFSClient
	filePath := x.getFilePath(req.SpaceId, req.FileId, req.Version)
	err := client.Remove(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return options.EmptyRPCReply, nil
}
func (x *StoreIo) DeleteFileDataByFileIds(ctx context.Context, req *pbrequest.DeleteFileDataByFileIds) (*pbmodel.EmptyStruct, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteFileDataByFileIds not implemented")
}
func (x *StoreIo) DeleteFileDataBySpaceIds(ctx context.Context, req *pbrequest.DeleteFileDataBySpaceIds) (*pbmodel.EmptyStruct, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteFileDataBySpaceIds not implemented")
}
