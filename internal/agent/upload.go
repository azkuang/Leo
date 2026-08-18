package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// manifestObject is one uploaded file's entry in the manifest.
type manifestObject struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// outputManifest is written last, at OutputPrefix+"manifest.json", as the
// commit marker for a consumer: seeing the manifest means every object it
// lists finished uploading.
type outputManifest struct {
	ExitCode   int              `json:"exit_code"`
	Message    string           `json:"message,omitempty"`
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt time.Time        `json:"finished_at"`
	Objects    []manifestObject `json:"objects"`
}

// uploadOutputs uploads a finished task's log and output directory, then the
// manifest. Returns nil without doing anything when store is nil -- no
// object store configured is not an error, it is today's (pre-data-plane)
// behaviour.
func (a *Agent) uploadOutputs(
	ctx context.Context, store *ObjectStore, outputPrefix, outDir, logPath string,
	startedAt time.Time, st Status,
) error {
	if store == nil {
		return nil
	}

	var objects []manifestObject

	if fi, err := os.Stat(logPath); err == nil && !fi.IsDir() {
		obj, err := uploadOne(ctx, store, outputPrefix+"_logs/task.log", logPath)
		if err != nil {
			return fmt.Errorf("upload log: %w", err)
		}
		objects = append(objects, obj)
	}

	if fi, err := os.Stat(outDir); err == nil && fi.IsDir() {
		err := filepath.WalkDir(outDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(outDir, path)
			if err != nil {
				return err
			}
			obj, err := uploadOne(ctx, store, outputPrefix+filepath.ToSlash(rel), path)
			if err != nil {
				return err
			}
			objects = append(objects, obj)
			return nil
		})
		if err != nil {
			return fmt.Errorf("upload outputs: %w", err)
		}
	}

	man := outputManifest{
		ExitCode:   st.ExitCode,
		Message:    st.Message,
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
		Objects:    objects,
	}
	body, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	tmp, err := os.CreateTemp("", "orch-manifest-*.json")
	if err != nil {
		return fmt.Errorf("stage manifest: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("stage manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("stage manifest: %w", err)
	}

	// Written last: its existence is the commit marker. "done" means the
	// bytes are durable, not that the process exited -- a reader must never
	// observe a DONE task with an object listed here that never arrived.
	if err := store.PutFile(ctx, outputPrefix+"manifest.json", tmp.Name(), "application/json"); err != nil {
		return fmt.Errorf("upload manifest: %w", err)
	}
	return nil
}

func uploadOne(ctx context.Context, store *ObjectStore, key, path string) (manifestObject, error) {
	sum, size, err := sha256File(path)
	if err != nil {
		return manifestObject{}, fmt.Errorf("hash %s: %w", path, err)
	}
	if err := store.PutFile(ctx, key, path, "application/octet-stream"); err != nil {
		return manifestObject{}, err
	}
	return manifestObject{Key: key, Size: size, SHA256: sum}, nil
}

func sha256File(path string) (digest string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), n, nil
}
