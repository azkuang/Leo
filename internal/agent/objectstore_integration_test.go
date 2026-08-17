package agent

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestObjectStorePutAndGetRoundTrip is a real integration test against a
// live S3-compatible server (see internal/api/sts_test.go's doc comment for
// how to bring one up), gated behind ORCH_TEST_S3_ENDPOINT the same way the
// Postgres integration tests are gated behind ORCH_TEST_DSN. It exercises
// the agent's actual upload/download path -- PutFile (used by upload.go)
// and Get (used by cache.go's s3:// scheme dispatch) -- rather than only
// the STS minting half covered by internal/api's test.
func TestObjectStorePutAndGetRoundTrip(t *testing.T) {
	endpoint := os.Getenv("ORCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ORCH_TEST_S3_ENDPOINT to run S3 integration tests")
	}
	bucket := envOrDefault("ORCH_TEST_S3_BUCKET", "orch-test")

	store, err := NewObjectStore(ObjectStoreConfig{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     envOrDefault("ORCH_TEST_S3_ACCESS_KEY", "orch"),
		SecretAccessKey: envOrDefault("ORCH_TEST_S3_SECRET_KEY", "orchorch"),
		PathStyle:       true,
	})
	if err != nil {
		t.Fatalf("NewObjectStore: %v", err)
	}
	if store == nil {
		t.Fatal("expected a configured store, got nil")
	}

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "manifest.json")
	body := []byte(`{"exit_code":0,"objects":[]}`)
	if err := os.WriteFile(srcPath, body, 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	ctx := context.Background()
	key := "jobs/it-job/tasks/it-task/attempts/0/manifest.json"
	if err := store.PutFile(ctx, key, srcPath, "application/json"); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	rc, size, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	if size != int64(len(body)) {
		t.Errorf("Get size = %d, want %d", size, len(body))
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read object body: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("round-tripped body = %q, want %q", got, body)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
