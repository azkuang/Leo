package agent

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alexk/orch/internal/domain"
)

// Cache is a content-addressed store of job inputs on the node.
//
// Clients upload inputs to object storage once; nodes pull on first use and
// keep them. The digest is the key, so a second task needing the same scene
// file finds it already there -- and because residency is reported on every
// heartbeat, the scheduler can prefer nodes that are already warm.
//
// This is also why the control plane never touches payloads: it moves
// references, and the data plane moves bytes.
type Cache struct {
	dir string

	mu       sync.RWMutex
	resident map[string]int64
	// inflight collapses concurrent requests for the same digest, so ten tasks
	// starting at once cause one download rather than ten.
	inflight map[string]*sync.WaitGroup
	// extracted / extractInflight are the tar-asset equivalent of
	// resident / inflight: once a .tar asset has been downloaded, extracting
	// it into a directory is itself a first-use cost worth collapsing and
	// remembering.
	extracted       map[string]bool
	extractInflight map[string]*sync.WaitGroup

	client *http.Client
	// objStore serves s3:// URIs. Nil means no object store is configured,
	// in which case only http(s):// URIs (already a bare, unauthenticated
	// GET -- which is exactly what a presigned URL needs) can be fetched.
	objStore *ObjectStore
}

// NewCache opens (or creates) a cache directory and indexes what is already
// there, so an agent restart does not discard a warm cache. objStore may be
// nil.
func NewCache(dir string, objStore *ObjectStore) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	c := &Cache{
		dir:             dir,
		resident:        map[string]int64{},
		inflight:        map[string]*sync.WaitGroup{},
		extracted:       map[string]bool{},
		extractInflight: map[string]*sync.WaitGroup{},
		client:          &http.Client{Timeout: 10 * time.Minute},
		objStore:        objStore,
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		c.resident[decodeDigest(e.Name())] = info.Size()
	}
	return c, nil
}

// Digests returns what is resident, for the heartbeat.
func (c *Cache) Digests() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]string, 0, len(c.resident))
	for d := range c.resident {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// Path returns the on-disk location for a digest.
func (c *Cache) Path(digest string) string {
	return filepath.Join(c.dir, encodeDigest(digest))
}

// Has reports whether a digest is already resident.
func (c *Cache) Has(digest string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.resident[digest]
	return ok
}

// Stage ensures every asset is present locally and returns the mounts to hand
// the executor.
//
// An asset whose URI ends in ".tar" is extracted into a digest-named
// directory on first use and the directory is mounted instead of the
// archive file -- the target workload (batch LLM/diffusion inference) needs
// model weights, which are a directory of many files, not the single file
// this used to support.
//
// The first task on a cold node pays for this; the rest do not. Budget for it
// in demo timing, or pre-warm.
func (c *Cache) Stage(ctx context.Context, assets []domain.AssetRef) ([]Mount, error) {
	mounts := make([]Mount, 0, len(assets))

	for _, a := range assets {
		if err := c.fetch(ctx, a); err != nil {
			return nil, fmt.Errorf("stage %s: %w", a.Digest, err)
		}

		hostPath := c.Path(a.Digest)
		if strings.HasSuffix(a.URI, ".tar") {
			dir, err := c.extract(a)
			if err != nil {
				return nil, fmt.Errorf("stage %s: extract: %w", a.Digest, err)
			}
			hostPath = dir
		}

		mounts = append(mounts, Mount{
			HostPath:      hostPath,
			ContainerPath: a.MountPath,
			ReadOnly:      true,
		})
	}
	return mounts, nil
}

func (c *Cache) fetch(ctx context.Context, a domain.AssetRef) error {
	c.mu.Lock()
	if _, ok := c.resident[a.Digest]; ok {
		c.mu.Unlock()
		return nil
	}
	if wg, ok := c.inflight[a.Digest]; ok {
		c.mu.Unlock()
		wg.Wait()
		return nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.inflight[a.Digest] = wg
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inflight, a.Digest)
		c.mu.Unlock()
		wg.Done()
	}()

	if a.URI == "" {
		return errors.New("asset has no URI")
	}

	body, err := c.open(ctx, a.URI)
	if err != nil {
		return err
	}
	defer body.Close()

	// Download to a temporary name and rename into place, so an interrupted
	// transfer can never be mistaken for a complete cache entry.
	final := c.Path(a.Digest)
	tmp, err := os.CreateTemp(c.dir, ".partial-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	// The digest doubles as the scheduler's cache_residency scoring key
	// (internal/scheduler/scorers.go), so a truncated or malicious URI must
	// not be allowed to poison the cache under a digest it doesn't own.
	// submit.go's client-fabricated "uri:"-prefixed pseudo-digests (used
	// when the caller supplied no real content hash) are not real digests
	// and are exempt from this check by design.
	verify := !strings.HasPrefix(a.Digest, "uri:")
	h := sha256.New()
	w := io.Writer(tmp)
	if verify {
		w = io.MultiWriter(tmp, h)
	}

	n, err := io.Copy(w, body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}

	if verify {
		if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != a.Digest {
			return fmt.Errorf("digest mismatch for %s: got %s, want %s", a.URI, got, a.Digest)
		}
	}

	if err := os.Rename(tmp.Name(), final); err != nil {
		return err
	}

	c.mu.Lock()
	c.resident[a.Digest] = n
	c.mu.Unlock()
	return nil
}

// open dispatches on URI scheme: s3:// goes through the object store client
// (STS-authenticated or static, whichever this node is configured with);
// everything else keeps the original bare, unauthenticated HTTP GET, which
// is what makes a presigned S3 URL work with zero code changes -- the input
// side of the data plane was never the problem.
func (c *Cache) open(ctx context.Context, uri string) (io.ReadCloser, error) {
	if strings.HasPrefix(uri, "s3://") {
		if c.objStore == nil {
			return nil, fmt.Errorf("fetch %s: no object store configured on this node", uri)
		}
		rc, _, err := c.objStore.GetURI(ctx, uri)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", uri, err)
		}
		return rc, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("fetch %s: %s", uri, resp.Status)
	}
	return resp.Body, nil
}

// extract untars the cached archive for digest a into a digest-named
// directory the first time it is needed, and reuses it (or, after an agent
// restart, adopts whatever a previous process already extracted) on every
// later call.
func (c *Cache) extract(a domain.AssetRef) (string, error) {
	dir := c.Path(a.Digest) + ".d"

	c.mu.Lock()
	if c.extracted[a.Digest] {
		c.mu.Unlock()
		return dir, nil
	}
	if wg, ok := c.extractInflight[a.Digest]; ok {
		c.mu.Unlock()
		wg.Wait()
		return dir, nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.extractInflight[a.Digest] = wg
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.extractInflight, a.Digest)
		c.mu.Unlock()
		wg.Done()
	}()

	if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
		c.mu.Lock()
		c.extracted[a.Digest] = true
		c.mu.Unlock()
		return dir, nil
	}

	// Extract into a temporary sibling directory and rename into place, for
	// the same reason fetch downloads to a .partial- name first: an
	// interrupted extraction must never be mistaken for a complete one.
	tmpDir, err := os.MkdirTemp(c.dir, ".extracting-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	f, err := os.Open(c.Path(a.Digest))
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := untar(tmpDir, f); err != nil {
		return "", err
	}
	if err := os.Rename(tmpDir, dir); err != nil {
		return "", err
	}

	c.mu.Lock()
	c.extracted[a.Digest] = true
	c.mu.Unlock()
	return dir, nil
}

// untar extracts a tar stream into destDir, rejecting any entry whose path
// would escape it (a malicious or corrupt archive must not be able to write
// outside the digest-named directory it was granted).
func untar(destDir string, r io.Reader) error {
	base := filepath.Clean(destDir)
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		target := filepath.Join(base, hdr.Name)
		if target != base && !strings.HasPrefix(target, base+string(os.PathSeparator)) {
			return fmt.Errorf("tar entry escapes destination: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode&0o777))
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // size is whatever the archive declares; this is a trusted staging path, not a public upload endpoint
				_ = out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
		default:
			// Symlinks, devices, etc: skip rather than fail the whole stage
			// over an artifact packaging quirk.
		}
	}
}

// Warm marks a digest resident without downloading, for simulated nodes that
// have no real bytes to move but should still exercise residency scoring.
func (c *Cache) Warm(digest string, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resident[digest] = size
}

// encodeDigest makes a digest safe as a filename ("sha256:ab..." -> "sha256-ab...").
func encodeDigest(d string) string {
	out := []byte(d)
	for i, b := range out {
		if b == ':' || b == '/' {
			out[i] = '-'
		}
	}
	return string(out)
}

func decodeDigest(name string) string {
	out := []byte(name)
	for i, b := range out {
		if b == '-' {
			out[i] = ':'
			break
		}
	}
	return string(out)
}
