package controller

import (
	"context"
	"io"

	"github.com/DataWorkbench/common/lib/storeio"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcstoreio"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
	"github.com/DataWorkbench/resourcemanager/options"
)

type StoreIo struct {
	pbsvcstoreio.UnimplementedStoreIOServer
}

func (x *StoreIo) generateWorkspaceDir(spaceId string) string {
	return storeio.GenerateWorkspaceDir(spaceId)
}

func (x *StoreIo) generateResourceFileDir(spaceId, fileId string) string {
	return storeio.GenerateResourceFileDir(spaceId, fileId)
}

func (x *StoreIo) generateResourceFilePath(spaceId, fileId, version string) string {
	return storeio.GenerateResourceFilePath(spaceId, fileId, version)
}

func (x *StoreIo) ensureRootDirExists(ctx context.Context, spaceId, fileId string) (err error) {
	rootDir := x.generateResourceFileDir(spaceId, fileId)
	if err = options.FiloIO.MkdirAll(ctx, rootDir, 0777); err != nil {
		return
	}
	return
}

func (x *StoreIo) receiveAndWrite(req pbsvcstoreio.StoreIO_WriteFileDataServer, writer io.WriteCloser, fileSize int64) (err error) {
	var (
		recv        *pbrequest.WriteFileData
		receiveSize int64
		written     int
	)

	ctx := req.Context()
	lg := glog.FromContext(ctx)

	for {
		recv, err = req.Recv()
		if err == io.EOF {
			if receiveSize != fileSize {
				lg.Warn().Msg("file data lose").Int64("fileSize", fileSize).Int64("receiveSize", receiveSize).Fire()
				return qerror.Internal
			}
			err = nil
			return
		}
		if err != nil {
			lg.Error().Msg("receive data from stream failed").Error("error", err).Fire()
			return
		}

		written, err = writer.Write(recv.Data)
		if err != nil {
			lg.Warn().Msg("write data to writer failed").Error("error", err).Fire()
			return
		}
		// count total size, provided the file size right.
		receiveSize += int64(written)
	}
}

func (x *StoreIo) WriteFileData(req pbsvcstoreio.StoreIO_WriteFileDataServer) (err error) {
	var (
		recv *pbrequest.WriteFileData
		eTag string
	)

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

	if err = x.ensureRootDirExists(ctx, recv.SpaceId, recv.FileId); err != nil {
		return err
	}

	fileSize := recv.Size
	filePath := x.generateResourceFilePath(recv.SpaceId, recv.FileId, recv.Version)

	reader, writer := io.Pipe()

	defer func() {
		if err != nil {
			lg.Error().Msg("write data to storage failed").Error("error", err).Fire()
			_ = options.FiloIO.Remove(ctx, filePath)
		}
		_ = writer.Close()
		_ = reader.Close()
	}()

	var writeError error
	done := make(chan struct{})

	go func() {
		lg.Debug().Msg("start to write data to storage").Int64("size", fileSize).Fire()
		writeError = x.receiveAndWrite(req, writer, fileSize)
		_ = writer.Close()
		close(done)
	}()

	if eTag, err = options.FiloIO.CreateAndWrite(ctx, filePath, reader); err != nil {
		return
	}

	<-done
	if writeError != nil {
		err = writeError
		return
	}

	lg.Debug().Msg("write data to storage end").String("eTag", eTag).Fire()
	_ = req.SendAndClose(&pbresponse.WriteFileData{Etag: eTag})
	return
}
func (x *StoreIo) ReadFileData(req *pbrequest.ReadFileData, reply pbsvcstoreio.StoreIO_ReadFileDataServer) (err error) {
	var (
		reader io.ReadCloser
	)

	ctx := reply.Context()
	filePath := x.generateResourceFilePath(req.SpaceId, req.FileId, req.Version)
	if reader, err = options.FiloIO.OpenForRead(ctx, filePath); err != nil {
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
			err = nil
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
	filePath := x.generateResourceFilePath(req.SpaceId, req.FileId, req.Version)
	err := options.FiloIO.Remove(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return options.EmptyRPCReply, nil
}
func (x *StoreIo) DeleteFileDataByFileIds(ctx context.Context, req *pbrequest.DeleteFileDataByFileIds) (*pbmodel.EmptyStruct, error) {
	for _, fileId := range req.FileIds {
		filePath := x.generateResourceFileDir(req.SpaceId, fileId)
		err := options.FiloIO.RemoveAll(ctx, filePath)
		if err != nil {
			return nil, err
		}
	}
	return options.EmptyRPCReply, nil
}
func (x *StoreIo) DeleteFileDataBySpaceIds(ctx context.Context, req *pbrequest.DeleteFileDataBySpaceIds) (*pbmodel.EmptyStruct, error) {
	for _, spaceId := range req.SpaceIds {
		rootDir := x.generateWorkspaceDir(spaceId)
		err := options.FiloIO.RemoveAll(ctx, rootDir)
		if err != nil {
			return nil, err
		}
	}
	return options.EmptyRPCReply, nil
}
