package api

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// TestMintCredsScopesToOutputPrefix is a real integration test against a
// live, STS-capable S3 endpoint (MinIO via deploy/docker-compose.yml, or any
// other S3-compatible server with STS AssumeRole enabled). STS behaviour is
// not meaningfully fakeable -- the point of the test is that the server
// itself enforces the inline policy -- so this is skipped unless
// ORCH_TEST_S3_ENDPOINT is set, mirroring the ORCH_TEST_DSN pattern used for
// the Postgres integration tests in internal/store/pgstore.
//
// Bring up a local server and run it with:
//
//	docker compose -f deploy/docker-compose.yml up -d minio
//	ORCH_TEST_S3_ENDPOINT=127.0.0.1:9000 \
//	ORCH_TEST_S3_BUCKET=orch-test \
//	ORCH_TEST_S3_ACCESS_KEY=orch ORCH_TEST_S3_SECRET_KEY=orchorch \
//	go test ./internal/api/ -run TestMintCredsScopesToOutputPrefix -v
//
// The bucket must already exist (mc mb local/orch-test); this test does not
// create it, the same way the real deployment expects deploy/docker-compose.yml's
// createbuckets one-shot service to have run first.
func TestMintCredsScopesToOutputPrefix(t *testing.T) {
	endpoint := os.Getenv("ORCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ORCH_TEST_S3_ENDPOINT to run S3/STS integration tests")
	}
	bucket := envOrDefault("ORCH_TEST_S3_BUCKET", "orch-test")
	accessKey := envOrDefault("ORCH_TEST_S3_ACCESS_KEY", "orch")
	secretKey := envOrDefault("ORCH_TEST_S3_SECRET_KEY", "orchorch")

	h := &Hub{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		s3: &S3Config{
			Endpoint:    endpoint,
			STSEndpoint: endpoint,
			Bucket:      bucket,
			AccessKey:   accessKey,
			SecretKey:   secretKey,
			UseSSL:      false,
		},
	}

	outputPrefix := "jobs/it-job/tasks/it-task/attempts/0/"
	creds := h.mintCreds(outputPrefix, time.Minute)
	if creds == nil {
		t.Fatal("expected non-nil credentials from a configured S3Config -- check the server has STS enabled")
	}
	if creds.GetAccessKeyId() == "" || creds.GetSecretAccessKey() == "" || creds.GetSessionToken() == "" {
		t.Fatal("expected populated access key, secret key and session token")
	}
	if creds.GetBucket() != bucket || creds.GetEndpoint() != endpoint {
		t.Errorf("creds carry bucket=%q endpoint=%q, want %q/%q",
			creds.GetBucket(), creds.GetEndpoint(), bucket, endpoint)
	}

	cl, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(creds.GetAccessKeyId(), creds.GetSecretAccessKey(), creds.GetSessionToken()),
		Secure: creds.GetUseSsl(),
	})
	if err != nil {
		t.Fatalf("build client from minted credentials: %v", err)
	}

	ctx := context.Background()

	// The whole point of STS-per-lease over a static credential: the minted
	// credential can write inside its own output prefix...
	if _, err := cl.PutObject(ctx, bucket, outputPrefix+"out.txt", strings.NewReader("hello"), 5, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("put inside the scoped output prefix should succeed: %v", err)
	}

	// ...and nowhere else, even within the same bucket -- a zombie from a
	// preempted attempt cannot write where nobody reads even if it tries.
	if _, err := cl.PutObject(ctx, bucket, "jobs/some-other-job/out.txt", strings.NewReader("nope"), 4, minio.PutObjectOptions{}); err == nil {
		t.Fatal("put outside the scoped output prefix should have been denied")
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
