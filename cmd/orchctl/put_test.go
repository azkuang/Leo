package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSha256FileMatchesKnownDigest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "asset.bin")
	if err := os.WriteFile(path, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	got, err := sha256File(path)
	if err != nil {
		t.Fatalf("sha256File: %v", err)
	}
	// sha256("hello world"), per `printf 'hello world' | sha256sum`
	want := "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if got != want {
		t.Errorf("sha256File() = %q, want %q", got, want)
	}
}
