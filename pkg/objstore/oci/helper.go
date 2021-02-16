package oci

import (
	"context"
	"fmt"
	"github.com/oracle/oci-go-sdk/v35/common"
	"github.com/oracle/oci-go-sdk/v35/objectstorage"
	"github.com/oracle/oci-go-sdk/v35/objectstorage/transfer"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

func getAbsolutePath(bkt Bucket, objectName string) *string {
	absolutePath := bkt.objectBasePath + objectName
	return &absolutePath
}

func getRelativePath(bkt Bucket, objectName string) string {
	relativePath := strings.TrimPrefix(objectName, bkt.objectBasePath)
	return relativePath
}

func getNamespace(client objectstorage.ObjectStorageClient, requestMetadata common.RequestMetadata) (namespace *string, err error) {
	response, err := client.GetNamespace(
		context.Background(),
		objectstorage.GetNamespaceRequest{RequestMetadata: requestMetadata},
	)
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}

func getObject(ctx context.Context, bkt Bucket, objectName string, byteRange string) (response objectstorage.GetObjectResponse, err error) {
	// path should not be empty
	if len(objectName) == 0 {
		err = fmt.Errorf("value cannot be empty for field ObjectName in path")
		return
	}
	request := objectstorage.GetObjectRequest{
		NamespaceName:   &bkt.namespace,
		BucketName:      &bkt.name,
		ObjectName:      getAbsolutePath(bkt, objectName),
		RequestMetadata: bkt.requestMetadata,
	}
	if byteRange != "" {
		request.Range = &byteRange
	}
	return bkt.client.GetObject(ctx, request)
}

func listAllObjects(ctx context.Context, bkt Bucket, prefix string) (objectNames []string, err error) {
	var allObjectNames []string

	objectNames, nextStartWith, err := listObjects(ctx, bkt, prefix, nil)
	if err != nil {
		return nil, err
	}
	allObjectNames = append(allObjectNames, objectNames...)

	for nextStartWith != nil {
		objectNames, nextStartWith, err = listObjects(ctx, bkt, prefix, nextStartWith)
		if err != nil {
			return nil, err
		}
		allObjectNames = append(allObjectNames, objectNames...)
	}
	return allObjectNames, nil
}

func listObjects(ctx context.Context, bkt Bucket, prefix string, start *string) (objectNames []string, nextStartWith *string, err error) {
	request := objectstorage.ListObjectsRequest{
		NamespaceName:   &bkt.namespace,
		BucketName:      &bkt.name,
		Delimiter:       common.String(DirDelim),
		Prefix:          getAbsolutePath(bkt, prefix),
		Start:           start,
		RequestMetadata: bkt.requestMetadata,
	}
	response, err := bkt.client.ListObjects(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	for _, object := range response.ListObjects.Objects {
		objectNames = append(objectNames, getRelativePath(bkt, *object.Name))
	}

	for _, objectName := range response.ListObjects.Prefixes {
		objectNames = append(objectNames, getRelativePath(bkt, objectName))
	}

	return objectNames, response.NextStartWith, nil
}

func uplodObject(ctx context.Context, bkt Bucket, objectName string, reader io.Reader) (err error) {
	req := transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:                       common.String(bkt.namespace),
			BucketName:                          common.String(bkt.name),
			ObjectName:                          getAbsolutePath(bkt, objectName),
			EnableMultipartChecksumVerification: common.Bool(true), // TODO: should we check?
			ObjectStorageClient:                 bkt.client,
			RequestMetadata:                     bkt.requestMetadata,
		},
		StreamReader: reader,
	}
	if bkt.partSize > 0 {
		req.UploadRequest.PartSize = &bkt.partSize
	}

	uploadManager := transfer.NewUploadManager()
	_, err = uploadManager.UploadStream(ctx, req)
	return
}

func deleteObject(ctx context.Context, bkt Bucket, objectName string) (err error) {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName:   &bkt.namespace,
		BucketName:      &bkt.name,
		ObjectName:      getAbsolutePath(bkt, objectName),
		RequestMetadata: bkt.requestMetadata,
	}
	_, err = bkt.client.DeleteObject(ctx, request)
	return
}

func createBucket(ctx context.Context, bkt *Bucket, compartmentId string) (err error) {
	request := objectstorage.CreateBucketRequest{
		NamespaceName:   &bkt.namespace,
		RequestMetadata: bkt.requestMetadata,
	}
	request.CompartmentId = &compartmentId
	request.Name = &bkt.name
	request.Metadata = make(map[string]string)
	request.PublicAccessType = objectstorage.CreateBucketDetailsPublicAccessTypeNopublicaccess
	_, err = bkt.client.CreateBucket(ctx, request)
	return
}

func deleteBucket(ctx context.Context, bkt *Bucket) (err error) {
	request := objectstorage.DeleteBucketRequest{
		NamespaceName:   &bkt.namespace,
		BucketName:      &bkt.name,
		RequestMetadata: bkt.requestMetadata,
	}
	_, err = bkt.client.DeleteBucket(ctx, request)
	return
}

func (config *Config) validateConfig() (err error) {
	if config.Tenancy == "" {
		return errors.New("no OCI tenancy ocid specified")
	}
	if config.User == "" {
		return errors.New("no OCI user ocid specified")
	}
	if config.Region == "" {
		return errors.New("no OCI region specified")
	}
	if config.Fingerprint == "" {
		return errors.New("no OCI fingerprint specified")
	}
	if config.PrivateKey == "" {
		return errors.New("no OCI privatekey specified")
	}
	return
}

func getConfigFromEnv() (config Config, err error) {
	config = Config{
		Provider:       strings.ToLower(os.Getenv("OCI_PROVIDER")),
		Bucket:         os.Getenv("OCI_BUCKET"),
		Compartment:    os.Getenv("OCI_COMPARTMENT"),
		Tenancy:        os.Getenv("OCI_TENANCY_OCID"),
		User:           os.Getenv("OCI_USER_OCID"),
		Region:         os.Getenv("OCI_REGION"),
		Fingerprint:    os.Getenv("OCI_FINGERPRINT"),
		PrivateKey:     os.Getenv("OCI_PRIVATEKEY"),
		Passphrase:     os.Getenv("OCI_PASSPHRASE"),
		ObjectBasePath: os.Getenv("OCI_OBJECT_BASE_PATH"),
	}

	if os.Getenv("OCI_PARTSIZE") != "" {
		partSize, err := strconv.ParseInt(os.Getenv("OCI_PARTSIZE"), 10, 64)
		if err != nil {
			return Config{}, err
		}
		config.PartSize = partSize
	}

	if os.Getenv("OCI_MAX_REQUEST_RETRIES") != "" {
		maxRequestRetries, err := strconv.Atoi(os.Getenv("OCI_MAX_REQUEST_RETRIES"))
		if err != nil {
			return Config{}, err
		}
		config.MaxRequestRetries = maxRequestRetries
	}

	return config, nil
}

func getRequestMetadata(maxRequestRetries int) common.RequestMetadata {
	if maxRequestRetries <= 1 {
		policy := common.NoRetryPolicy()
		return common.RequestMetadata{
			RetryPolicy: &policy,
		}
	}

	retryOnAllNon200ResponseCodes := func(r common.OCIOperationResponse) bool {
		return !(r.Error == nil && 199 < r.Response.HTTPResponse().StatusCode && r.Response.HTTPResponse().StatusCode < 300)
	}
	exponentialBackoff := func(r common.OCIOperationResponse) time.Duration {
		return time.Duration(math.Pow(float64(2), float64(r.AttemptNumber-1))) * time.Second
	}
	policy := common.NewRetryPolicy(uint(maxRequestRetries), retryOnAllNon200ResponseCodes, exponentialBackoff)

	return common.RequestMetadata{
		RetryPolicy: &policy,
	}
}
