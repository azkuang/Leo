package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alexk/orch/internal/agent"
)

// cmdPut uploads a local file straight to the object store and prints a
// ready-made --asset-uri/--asset-digest pair for `orchctl submit`.
//
// This is the client-side half of the data plane: bytes move directly
// between the client and object storage, and the control plane never
// touches them -- it only ever sees the reference this command prints.
func cmdPut(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	var (
		endpoint  = fs.String("s3-endpoint", envOr("ORCH_S3_ENDPOINT", ""), "object store endpoint (host:port)")
		bucket    = fs.String("s3-bucket", envOr("ORCH_S3_BUCKET", ""), "object store bucket")
		region    = fs.String("s3-region", envOr("ORCH_S3_REGION", ""), "object store region")
		accessKey = fs.String("s3-access-key", envOr("ORCH_S3_ACCESS_KEY", ""), "object store access key")
		secretKey = fs.String("s3-secret-key", envOr("ORCH_S3_SECRET_KEY", ""), "object store secret key")
		useSSL    = fs.Bool("s3-use-ssl", false, "use TLS against the object store")
		pathStyle = fs.Bool("s3-path-style", true, "path-style bucket addressing (MinIO needs this)")
		mount     = fs.String("asset-mount", "/inputs/asset", "suggested --asset-mount to print alongside the uploaded URI")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: orchctl put [flags] <file>")
	}
	path := fs.Arg(0)

	if *endpoint == "" || *bucket == "" {
		return errors.New("--s3-endpoint and --s3-bucket are required (or set ORCH_S3_ENDPOINT / ORCH_S3_BUCKET)")
	}

	digest, err := sha256File(path)
	if err != nil {
		return fmt.Errorf("hash %s: %w", path, err)
	}

	store, err := agent.NewObjectStore(agent.ObjectStoreConfig{
		Endpoint:        *endpoint,
		Bucket:          *bucket,
		Region:          *region,
		AccessKeyID:     *accessKey,
		SecretAccessKey: *secretKey,
		UseSSL:          *useSSL,
		PathStyle:       *pathStyle,
	})
	if err != nil {
		return err
	}
	if store == nil {
		return errors.New("object store not configured (missing --s3-endpoint/--s3-bucket)")
	}

	// The digest is also the node cache key (internal/agent/cache.go), so
	// keying the object by it too means a second upload of identical
	// content lands on the same key rather than growing the bucket
	// unbounded.
	key := "inputs/" + strings.TrimPrefix(digest, "sha256:")
	if err := store.PutFile(ctx, key, path, "application/octet-stream"); err != nil {
		return fmt.Errorf("upload %s: %w", path, err)
	}

	uri := fmt.Sprintf("s3://%s/%s", *bucket, key)
	fmt.Printf("uploaded %s -> %s\n\n", path, uri)
	fmt.Printf("  --asset-uri %s --asset-digest %s --asset-mount %s\n", uri, digest, *mount)
	return nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
