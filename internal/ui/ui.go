// Package ui serves the operator interface.
//
// The built assets are embedded in the binary so the control plane is a single
// artifact: `docker compose up` and the fleet view is there, with no static
// file server to configure and no version skew between API and UI.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var assets embed.FS

// Handler serves the UI, falling back to index.html so client-side routes
// survive a page reload.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "dist")
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "ui assets missing from binary", http.StatusInternalServerError)
		})
	}

	files := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		if _, err := fs.Stat(sub, path); err != nil {
			// Not a real file: hand it to the SPA rather than 404ing, so a
			// deep link like /jobs/job-abc123 reloads correctly.
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}

		// Hashed asset filenames are immutable; index.html must not be cached
		// or a redeploy leaves stale JS pointing at a missing bundle.
		if strings.HasPrefix(path, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		files.ServeHTTP(w, r)
	})
}
