package agent

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStore is the data-plane client the agent uses to move bytes: input
// fetches (s3:// URIs, alongside the pre-existing bare-HTTP path) and output
// uploads once a task finishes.
//
// This is not a fifth extension seam. Cache and HostProbe already establish
// the pattern this codebase uses for infrastructure every backend shares
// rather than swaps -- real and simulated nodes upload through the identical
// path (see sim.go's writeSimOutput), so there is nothing here for a real vs.
// simulated implementation to diverge on.
type ObjectStore struct {
	client *minio.Client
	bucket string
}

// ObjectStoreConfig configures a static, long-lived-credential client. It is
// used for input fetches and, absent any per-lease STS credentials from the
// control plane, for output uploads too -- the fallback that keeps a small
// or local deployment (docker-compose's MinIO) working without wiring up
// STS at all.
type ObjectStoreConfig struct {
	Endpoint        string
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	// PathStyle forces path-style bucket addressing (host/bucket/key rather
	// than bucket.host/key), which MinIO needs and hosted S3 does not.
	PathStyle bool
}

func (c ObjectStoreConfig) configured() bool {
	return c.Endpoint != "" && c.Bucket != ""
}

// NewObjectStore builds a client from long-lived credentials, or returns a
// nil *ObjectStore (and a nil error) when cfg is not configured -- so a node
// with no ORCH_S3_* environment behaves exactly as it did before this type
// existed, with no uploads and no hard dependency on an object store being
// reachable.
func NewObjectStore(cfg ObjectStoreConfig) (*ObjectStore, error) {
	if !cfg.configured() {
		return nil, nil
	}
	cl, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure:       cfg.UseSSL,
		Region:       cfg.Region,
		BucketLookup: lookupType(cfg.PathStyle),
	})
	if err != nil {
		return nil, fmt.Errorf("build object store client: %w", err)
	}
	return &ObjectStore{client: cl, bucket: cfg.Bucket}, nil
}

// STSCredentials is the wire-agnostic form of orchv1.ObjectCredentials, kept
// separate so this file does not import the generated proto package -- the
// same separation convention internal/api/convert.go uses between domain and
// wire types.
type STSCredentials struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UseSSL          bool
}

// NewObjectStoreFromSTS builds a per-lease client from temporary credentials
// minted by the control plane's STS AssumeRole call. Those credentials are
// scoped to this lease's output prefix and expire with the lease TTL, so a
// zombie from a preempted attempt cannot write anywhere else even if it
// tries -- the enforced version of "a zombie writes where nobody reads"
// rather than just a naming convention.
func NewObjectStoreFromSTS(c STSCredentials) (*ObjectStore, error) {
	cl, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, c.SessionToken),
		Secure: c.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("build sts object store client: %w", err)
	}
	return &ObjectStore{client: cl, bucket: c.Bucket}, nil
}

func lookupType(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupAuto
}

// Get opens an object for reading and reports its size. It is the s3:// half
// of Cache.fetch's scheme dispatch -- the http(s):// half already worked
// with zero code changes, because it was already a bare, unauthenticated
// GET, which is exactly what a presigned URL needs.
func (s *ObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, 0, fmt.Errorf("stat %s: %w", key, err)
	}
	return obj, info.Size, nil
}

// PutFile uploads one local file to key.
func (s *ObjectStore) PutFile(ctx context.Context, key, path, contentType string) error {
	_, err := s.client.FPutObject(ctx, s.bucket, key, path, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}
