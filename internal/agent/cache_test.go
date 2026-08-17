package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexk/orch/internal/domain"
)

func sha256Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestFetchVerifiesDigest(t *testing.T) {
	body := []byte("hello world")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	defer srv.Close()

	c, err := NewCache(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	digest := sha256Digest(body)
	_, err = c.Stage(context.Background(), []domain.AssetRef{
		{Digest: digest, URI: srv.URL, MountPath: "/in"},
	})
	if err != nil {
		t.Fatalf("Stage with correct digest should succeed: %v", err)
	}
	if !c.Has(digest) {
		t.Error("expected digest to be resident after a verified fetch")
	}
}

func TestFetchRejectsDigestMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("actual content"))
	}))
	defer srv.Close()

	c, err := NewCache(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	wrongDigest := sha256Digest([]byte("something else entirely"))
	_, err = c.Stage(context.Background(), []domain.AssetRef{
		{Digest: wrongDigest, URI: srv.URL, MountPath: "/in"},
	})
	if err == nil {
		t.Fatal("expected a digest mismatch error")
	}
	if c.Has(wrongDigest) {
		t.Error("a digest-mismatched fetch must not be marked resident")
	}
	// And it must not have left a poisoned file under that digest either.
	if _, statErr := os.Stat(c.Path(wrongDigest)); statErr == nil {
		t.Error("digest-mismatched fetch must not leave a file at the final path")
	}
}

func TestFetchSkipsVerificationForURIPseudoDigest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("whatever, no real digest was ever promised"))
	}))
	defer srv.Close()

	c, err := NewCache(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	pseudo := "uri:" + srv.URL
	_, err = c.Stage(context.Background(), []domain.AssetRef{
		{Digest: pseudo, URI: srv.URL, MountPath: "/in"},
	})
	if err != nil {
		t.Fatalf("uri: pseudo-digest must skip verification, got error: %v", err)
	}
}

func TestFetchRejectsS3WithoutObjectStore(t *testing.T) {
	c, err := NewCache(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	_, err = c.Stage(context.Background(), []domain.AssetRef{
		{Digest: "uri:s3://bucket/key", URI: "s3://bucket/key", MountPath: "/in"},
	})
	if err == nil {
		t.Fatal("expected an error fetching s3:// with no object store configured")
	}
}

func buildTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		hdr := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("write tar body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return buf.Bytes()
}

func TestStageExtractsTarAssetIntoDirectory(t *testing.T) {
	archive := buildTar(t, map[string]string{
		"config.json":    `{"model":"demo"}`,
		"weights/w1.bin": "binary-data-1",
		"weights/w2.bin": "binary-data-2",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archive)
	}))
	defer srv.Close()

	c, err := NewCache(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	digest := sha256Digest(archive)
	mounts, err := c.Stage(context.Background(), []domain.AssetRef{
		{Digest: digest, URI: srv.URL + "/weights.tar", MountPath: "/model"},
	})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}
	m := mounts[0]
	if !m.ReadOnly {
		t.Error("expected extracted tar mount to be read-only")
	}
	if m.ContainerPath != "/model" {
		t.Errorf("ContainerPath = %q, want /model", m.ContainerPath)
	}

	for _, rel := range []string{"config.json", filepath.Join("weights", "w1.bin"), filepath.Join("weights", "w2.bin")} {
		if _, statErr := os.Stat(filepath.Join(m.HostPath, rel)); statErr != nil {
			t.Errorf("expected extracted file %s: %v", rel, statErr)
		}
	}
}

func TestStageNonTarAssetMountsSingleFile(t *testing.T) {
	body := []byte("single file content")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	defer srv.Close()

	c, err := NewCache(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	digest := sha256Digest(body)
	mounts, err := c.Stage(context.Background(), []domain.AssetRef{
		{Digest: digest, URI: srv.URL, MountPath: "/in/asset"},
	})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if mounts[0].HostPath != c.Path(digest) {
		t.Errorf("expected a plain file mount at %s, got %s", c.Path(digest), mounts[0].HostPath)
	}
	fi, err := os.Stat(mounts[0].HostPath)
	if err != nil || fi.IsDir() {
		t.Errorf("expected a regular file at the mount host path, stat err=%v isDir=%v", err, fi != nil && fi.IsDir())
	}
}
