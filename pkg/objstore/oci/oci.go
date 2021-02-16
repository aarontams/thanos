package oci

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oracle/oci-go-sdk/v35/common"
	"github.com/oracle/oci-go-sdk/v35/common/auth"
	"github.com/oracle/oci-go-sdk/v35/objectstorage"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"gopkg.in/yaml.v2"
	"io"
	"net/http"
	"strings"
	"testing"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

type Provider string

const (
	DefaultConfigProvider           = Provider("default")
	InstancePrincipalConfigProvider = Provider("instance-principal")
	RawConfigProvider               = Provider("raw")
)

// Config stores the configuration for oci bucket.
type Config struct {
	Provider          string `yaml:"provider"`
	Bucket            string `yaml:"bucket"`
	Compartment       string `yaml:"compartment_ocid"`
	Tenancy           string `yaml:"tenancy_ocid"`
	User              string `yaml:"user_ocid"`
	Region            string `yaml:"region"`
	Fingerprint       string `yaml:"fingerprint"`
	PrivateKey        string `yaml:"privatekey"`
	Passphrase        string `yaml:"passphrase"`
	PartSize          int64  `yaml:"partSize"`
	MaxRequestRetries int    `yaml:"maxRequestRetries"`
	ObjectBasePath    string `yaml:"objectBasePath"`
}

// Bucket implements the store.Bucket interface against OCI APIs.
type Bucket struct {
	logger          log.Logger
	name            string
	namespace       string
	client          *objectstorage.ObjectStorageClient
	partSize        int64
	requestMetadata common.RequestMetadata
	objectBasePath  string
}

// Name returns the bucket name for the provider.
func (b *Bucket) Name() string {
	return b.name
}

// Iter calls f for each entry in the given directory (not recursive). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	objectNames, err := listAllObjects(ctx, *b, dir)
	if err != nil {
		return errors.Wrapf(err, "cannot list objects in directory '%s'", dir)
	}

	level.Debug(b.logger).Log("NumberOfObjects", len(objectNames))

	for _, objectName := range objectNames {
		if objectName == "" || objectName == dir {
			continue
		}
		if err := f(objectName); err != nil {
			return err
		}
	}

	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	response, err := getObject(ctx, *b, name, "")
	if err != nil {
		return nil, err
	}
	return response.Content, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	level.Debug(b.logger).Log("msg", "getting object", "name", name, "off", offset, "length", length)

	// A single byte range to fetch, as described in RFC 7233 (https://tools.ietf.org/html/rfc7233#section-2.1).
	byteRange := ""

	if offset >= 0 {
		if length > 0 {
			byteRange = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		} else {
			byteRange = fmt.Sprintf("bytes=%d-", offset)
		}
	} else {
		if length > 0 {
			byteRange = fmt.Sprintf("bytes=-%d", length)
		} else {
			return nil, errors.New(fmt.Sprintf("invalid range specified: offset=%d length=%d", offset, length))
		}
	}

	level.Debug(b.logger).Log("byteRange", byteRange)

	response, err := getObject(ctx, *b, name, byteRange)
	if err != nil {
		return nil, err
	}
	return response.Content, nil
}

// Upload the contents of the reader as an object into the bucket.
// Upload should be idempotent.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return uplodObject(ctx, *b, name, r)
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := getObject(ctx, *b, name, "")
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "cannot get OCI object '%s'", name)
	}
	return true, nil
}

// Delete removes the object with the given name.
// If object does not exists in the moment of deletion, Delete should throw error.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	return deleteObject(ctx, *b, name)
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	failure, isServiceError := common.IsServiceError(err)
	if isServiceError {
		k := failure.GetHTTPStatusCode()
		match := k == http.StatusNotFound
		level.Debug(b.logger).Log("msg", match)
		return failure.GetHTTPStatusCode() == http.StatusNotFound
	}
	return false
}

// ObjectSize returns the size of the specified object.
func (b *Bucket) ObjectSize(ctx context.Context, name string) (uint64, error) {
	response, err := getObject(ctx, *b, name, "")
	if err != nil {
		return 0, err
	}
	return uint64(*response.ContentLength), nil
}

// Close closes bucket.
func (b *Bucket) Close() error {
	return nil
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	response, err := getObject(ctx, *b, name, "")
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return objstore.ObjectAttributes{
		Size:         *response.ContentLength,
		LastModified: response.LastModified.Time,
	}, nil
}

// createBucket creates bucket.
func (b *Bucket) createBucket(ctx context.Context, compartmentId string) (err error) {
	return createBucket(ctx, b, compartmentId)
}

// deleteBucket deletes bucket.
func (b *Bucket) deleteBucket(ctx context.Context) (err error) {
	return deleteBucket(ctx, b)
}

// NewBucket returns a new Bucket using the provided oci config values.
func NewBucket(logger log.Logger, ociConfig []byte) (*Bucket, error) {
	level.Debug(logger).Log("msg", "creating new oci bucket connection")
	var config Config
	var configurationProvider common.ConfigurationProvider
	var err error

	if err := yaml.Unmarshal(ociConfig, &config); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal the given oci configurations")
	}

	provider := Provider(strings.ToLower(config.Provider))
	level.Info(logger).Log("msg", fmt.Sprintf("Creating OCI default '%s' provider", provider))
	switch provider {
	case DefaultConfigProvider:
		configurationProvider = common.DefaultConfigProvider()
	case InstancePrincipalConfigProvider:
		configurationProvider, err = auth.InstancePrincipalConfigurationProvider()
		if err != nil {
			return nil, errors.Wrapf(err, "unable to create OCI instance principal config provider")
		}
	case RawConfigProvider:
		if err := config.validateConfig(); err != nil {
			return nil, errors.Wrapf(err, "invalid oci configurations")
		}
		configurationProvider = common.NewRawConfigurationProvider(config.Tenancy, config.User, config.Region,
			config.Fingerprint, config.PrivateKey, &config.Passphrase)
	default:
		return nil, errors.Wrapf(err, fmt.Sprintf("unsupported OCI provider: %s", provider))
	}

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create ObjectStorage client with the given oci configurations")
	}

	requestMetadata := getRequestMetadata(config.MaxRequestRetries)

	namespace, err := getNamespace(client, requestMetadata)
	if err != nil {
		return nil, err
	}
	level.Debug(logger).Log("msg", fmt.Sprintf("OCI tenancy namespace: %s", *namespace))

	if config.ObjectBasePath != "" {
		config.ObjectBasePath = strings.TrimSuffix(config.ObjectBasePath, DirDelim) + DirDelim
	}

	bkt := Bucket{
		logger:          logger,
		name:            config.Bucket,
		namespace:       *namespace,
		client:          &client,
		partSize:        config.PartSize,
		requestMetadata: requestMetadata,
		objectBasePath:  config.ObjectBasePath,
	}

	return &bkt, nil
}

//NewTestBucket creates test bkt client that before returning creates temporary bucket.
//In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	config, err := getConfigFromEnv()
	ociConfig, err := yaml.Marshal(config)
	if err != nil {
		return nil, nil, err
	}

	bkt, err := NewBucket(log.NewNopLogger(), ociConfig)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	bkt.name = objstore.CreateTemporaryTestBucketName(t)
	if err := createBucket(ctx, bkt, config.Compartment); err != nil {
		t.Errorf("failed to create temporary OCI bucket '%s' for OCI tests", bkt.name)
		return nil, nil, err
	}

	t.Logf("created temporary OCI bucket '%s' for OCI tests", bkt.name)
	return bkt, func() {
		objstore.EmptyBucket(t, ctx, bkt)
		if err := deleteBucket(ctx, bkt); err != nil {
			t.Logf("failed to delete temporary OCI bucket %s for OCI tests: %s", bkt.name, err)
		}
		t.Logf("deleted temporary OCI bucket '%s' for OCI tests", bkt.name)
	}, nil
}
