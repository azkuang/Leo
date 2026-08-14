package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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

	client *http.Client
}

// NewCache opens (or creates) a cache directory and indexes what is already
// there, so an agent restart does not discard a warm cache.
func NewCache(dir string) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	c := &Cache{
		dir:      dir,
		resident: map[string]int64{},
		inflight: map[string]*sync.WaitGroup{},
		client:   &http.Client{Timeout: 10 * time.Minute},
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
// The first task on a cold node pays for this; the rest do not. Budget for it
// in demo timing, or pre-warm.
func (c *Cache) Stage(ctx context.Context, assets []domain.AssetRef) ([]Mount, error) {
	mounts := make([]Mount, 0, len(assets))

	for _, a := range assets {
		if err := c.fetch(ctx, a); err != nil {
			return nil, fmt.Errorf("stage %s: %w", a.Digest, err)
		}
		mounts = append(mounts, Mount{
			HostPath:      c.Path(a.Digest),
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.URI, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: %s", a.URI, resp.Status)
	}

	// Download to a temporary name and rename into place, so an interrupted
	// transfer can never be mistaken for a complete cache entry.
	final := c.Path(a.Digest)
	tmp, err := os.CreateTemp(c.dir, ".partial-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	n, err := io.Copy(tmp, resp.Body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return err
	}

	c.mu.Lock()
	c.resident[a.Digest] = n
	c.mu.Unlock()
	return nil
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
