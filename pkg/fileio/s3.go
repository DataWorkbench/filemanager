package fileio

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/DataWorkbench/glog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var _ FileIO = (*S3Client)(nil)

type S3Config struct {
	// An optional endpoint URL (hostname only or fully qualified URI)
	// eg: 'http://s3.gd2.qingstor.com' or `https://s3.gd2.qingstor.com` or 's3.gd2.qingstor.com'
	Endpoint string `json:"endpoint" yaml:"endpoint" env:"ENDPOINT" validate:"required"`

	// The region to send requests to.
	Region string `json:"region" yaml:"region" env:"REGION" validate:"required"`

	// The bucket name.
	Bucket string `json:"bucket" yaml:"bucket" env:"BUCKET" validate:"required"`

	// The access key id.
	AccessKeyId string `json:"access_key_id" yaml:"access_key_id" env:"ACCESS_KEY_ID" validate:"required"`

	// The access secret key id.
	SecretAccessKey string `json:"secret_access_key" yaml:"secret_access_key" env:"SECRET_ACCESS_KEY" validate:"required"`

	// Set this to `true` to disable SSL when sending requests. Defaults
	// to `false`.
	DisableSSL bool `json:"disable_ssl" yaml:"disable_ssl" env:"DISABLE_SSL" validate:"-"`

	// Set this to `true` to force the request to use path-style addressing,
	// i.e., `http://s3.amazonaws.com/BUCKET/KEY`. By default, the S3 client
	// will use virtual hosted bucket addressing when possible
	// (`http://BUCKET.s3.amazonaws.com/KEY`).
	ForcePathStyle bool `json:"force_path_style" yaml:"force_path_style" env:"FORCE_PATH_STYLE" validate:"-"`
}

type S3Client struct {
	svc    *s3.S3
	bucket *string
}

func NewS3Client(ctx context.Context, cfg *S3Config) (FileIO, error) {
	sess, err := session.NewSession(&aws.Config{
		CredentialsChainVerboseErrors: nil,
		Credentials:                   credentials.NewStaticCredentials(cfg.AccessKeyId, cfg.SecretAccessKey, ""),
		Endpoint:                      aws.String(cfg.Endpoint),
		Region:                        aws.String(cfg.Region),
		DisableSSL:                    aws.Bool(cfg.DisableSSL),
		S3ForcePathStyle:              aws.Bool(cfg.ForcePathStyle),
		HTTPClient:                    nil,
		//LogLevel:                      aws.LogLevel(aws.LogDebug),
		Logger:                        nil,
		MaxRetries:                    nil,
		S3DisableContentMD5Validation: nil,
		EnableEndpointDiscovery:       nil,
	})
	if err != nil {
		return nil, err
	}

	cli := &S3Client{
		svc:    s3.New(sess),
		bucket: aws.String(cfg.Bucket),
	}
	return cli, nil
}

func (cli *S3Client) Close() error {
	return nil
}

func (cli *S3Client) MkdirAll(ctx context.Context, dirname string, perm os.FileMode) error {
	// TODO: we need create `directory` in object storage?
	return nil
}

func (cli *S3Client) IsExists(ctx context.Context, name string) (bool, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("s3: check file is exists").String("name", name).Fire()
	_, err := cli.svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: cli.bucket,
		Key:    aws.String(name),
	})
	if err != nil {
		lg.Warn().Msg("s3: stat file failed").Error("error", err).Fire()
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func (cli *S3Client) CreateAndWrite(ctx context.Context, name string, reader io.ReadCloser) (string, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("s3: create new file for write").String("name", name).Fire()

	output, err := cli.svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		//ContentLength: aws.Int64(int64(len(b1) * 3)),
		Bucket: cli.bucket,
		Key:    aws.String(name),
		Body:   aws.ReadSeekCloser(reader),
	})
	if err != nil {
		lg.Error().Msg("s3: create new file failed").Error("error", err).Fire()
		return "", err
	}
	if output.ETag == nil {
		return "", errors.New("no ETag in s3 response")
	}

	eTag := *output.ETag
	eTag = eTag[1 : len(eTag)-1]
	return eTag, nil
}

func (cli *S3Client) OpenForRead(ctx context.Context, name string) (io.ReadCloser, error) {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("s3: open file for read").String("name", name).Fire()
	output, err := cli.svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: cli.bucket,
		Key:    aws.String(name),
	})
	if err != nil {
		lg.Error().Msg("s3: open file failed").Error("error", err).Fire()
		return nil, err
	}
	return output.Body, nil
}

func (cli *S3Client) Remove(ctx context.Context, name string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("s3: remove file").String("name", name).Fire()
	_, err := cli.svc.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: cli.bucket,
		Key:    aws.String(name),
	})
	if err != nil {
		lg.Error().Msg("s3: remove file failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (cli *S3Client) RemoveAll(ctx context.Context, name string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("s3: remove dir and all children").String("name", name).Fire()

	name = strings.TrimPrefix(name, "/")
	input := &s3.ListObjectsInput{
		Bucket:  cli.bucket,
		MaxKeys: aws.Int64(100),
		Prefix:  aws.String(strings.TrimPrefix(name, "/")), // the object key in s3 not contains prefix "/"
	}
	// Create a delete list objects iterator
	iter := s3manager.NewDeleteListIterator(cli.svc, input)
	// Create the BatchDelete client
	batcher := s3manager.NewBatchDeleteWithClient(cli.svc)
	if err := batcher.Delete(ctx, iter); err != nil {
		lg.Error().Msg("s3: remove dir and all children failed").Error("error", err).Fire()
		return err
	}
	return nil
}

func (cli *S3Client) Rename(ctx context.Context, oldName string, newName string) error {
	lg := glog.FromContext(ctx)
	lg.Debug().Msg("s3: rename file").String("old", oldName).String("new", newName).Fire()

	_, err := cli.svc.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:     cli.bucket,
		CopySource: aws.String(oldName),
		Key:        aws.String(newName),
	})
	if err != nil {
		lg.Error().Msg("s3: rename file failed").Error("error", err).Fire()
		return err
	}
	if err = cli.Remove(ctx, oldName); err != nil {
		return err
	}
	return nil
}
