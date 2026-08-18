package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"connectrpc.com/connect"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/gen/orch/v1/orchv1connect"
)

// cmdSubmit builds a job from flags.
//
// The fan-out is expressed as a range of indices with a parameter name. The
// client decides what that parameter means -- frames, shards, seeds -- and the
// control plane never finds out.
func cmdSubmit(ctx context.Context, c orchv1connect.OrchServiceClient, args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	var (
		name        = fs.String("name", "", "job name")
		owner       = fs.String("owner", "cli", "job owner")
		pool        = fs.String("pool", "default", "pool to submit into")
		image       = fs.String("image", "", "OCI image digest or reference (required)")
		command     = fs.String("command", "", "container command, space separated")
		env         = fs.String("env", "", "comma-separated KEY=VALUE pairs")
		slots       = fs.Int("slots", 1, "slots per task; >1 is a gang reservation on one machine")
		minVRAM     = fs.Int("min-vram-gb", 0, "minimum VRAM per slot")
		minCC       = fs.String("min-compute-capability", "", "minimum compute capability, e.g. 8.9")
		minDriver   = fs.String("min-driver", "", "minimum driver version")
		minCUDA     = fs.String("min-cuda", "", "minimum CUDA version")
		requireECC  = fs.Bool("require-ecc", false, "only slots reporting ECC")
		exclusive   = fs.Bool("exclusive-host", false, "reserve the whole machine")
		labels      = fs.String("require-labels", "", "comma-separated KEY=VALUE node label predicates")
		preemptible = fs.Bool("preemptible", true, "may be preempted while running natively")
		service     = fs.Bool("service", false, "service lease rather than batch")

		count    = fs.Int("count", 1, "number of tasks to fan out into")
		paramKey = fs.String("param", "index", "task parameter name carrying the fan-out index")
		start    = fs.Int("start", 1, "first fan-out index")
		duration = fs.Duration("sim-duration", 0, "hint simulated nodes to take this long per task")

		assetURI    = fs.String("asset-uri", "", "URI of a single input asset")
		assetDigest = fs.String("asset-digest", "", "content address of that asset")
		assetMount  = fs.String("asset-mount", "/inputs/asset", "where to mount it in the container")
		assetSize   = fs.Uint64("asset-size", 0, "asset size in bytes, used to weight cache scoring")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *image == "" {
		return errors.New("--image is required")
	}

	commandArgs, err := shellSplit(*command)
	if err != nil {
		return fmt.Errorf("--command: %w", err)
	}

	mode := orchv1.LeaseMode_LEASE_MODE_BATCH
	if *service {
		mode = orchv1.LeaseMode_LEASE_MODE_SERVICE
	}

	tasks := make([]*orchv1.TaskParams, 0, *count)
	for i := 0; i < *count; i++ {
		p := map[string]string{*paramKey: strconv.Itoa(*start + i)}
		if *duration > 0 {
			p["sim_duration_ms"] = strconv.FormatInt(duration.Milliseconds(), 10)
		}
		tasks = append(tasks, &orchv1.TaskParams{Params: p})
	}

	var assets []*orchv1.AssetRef
	if *assetURI != "" {
		digest := *assetDigest
		if digest == "" {
			// The digest is the cache key, so without one every node would
			// re-download. Deriving a stable stand-in from the URI keeps
			// residency scoring meaningful for casual submissions.
			digest = "uri:" + *assetURI
		}
		assets = append(assets, &orchv1.AssetRef{
			Digest: digest, Uri: *assetURI,
			SizeBytes: *assetSize, MountPath: *assetMount,
		})
	}

	res, err := c.SubmitJob(ctx, connect.NewRequest(&orchv1.SubmitJobRequest{
		Spec: &orchv1.JobSpec{
			Name:        *name,
			Owner:       *owner,
			PoolId:      *pool,
			ImageDigest: *image,
			Command:     commandArgs,
			Env:         parsePairs(*env),
			Mode:        mode,
			Preemptible: *preemptible,
			Assets:      assets,
			Tasks:       tasks,
			Request: &orchv1.ResourceRequest{
				Slots:                uint32(*slots),
				MinVramGb:            uint32(*minVRAM),
				MinComputeCapability: *minCC,
				MinDriver:            *minDriver,
				MinCuda:              *minCUDA,
				RequireEcc:           *requireECC,
				ExclusiveHost:        *exclusive,
				RequiredLabels:       parsePairs(*labels),
			},
		},
	}))
	if err != nil {
		return err
	}

	fmt.Printf("submitted %s with %d task(s)\n",
		res.Msg.GetJobId(), len(res.Msg.GetTaskIds()))
	return nil
}

func parsePairs(s string) map[string]string {
	if s == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && k != "" {
			out[k] = v
		}
	}
	return out
}

// shellSplit tokenizes a command string the way a POSIX shell would split
// unquoted words: whitespace separates tokens, single quotes are literal,
// double quotes allow backslash-escaping \" and \\, and a bare backslash
// escapes the next character outside quotes. This replaces a naive
// strings.Split(s, " "), which could not express any argument containing a
// space at all (a quoted path, a multi-word title).
func shellSplit(s string) ([]string, error) {
	var (
		tokens             []string
		cur                strings.Builder
		hasToken           bool
		inSingle, inDouble bool
	)

	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case inSingle:
			if r == '\'' {
				inSingle = false
			} else {
				cur.WriteRune(r)
			}
		case inDouble:
			switch {
			case r == '"':
				inDouble = false
			case r == '\\' && i+1 < len(runes) && (runes[i+1] == '"' || runes[i+1] == '\\'):
				i++
				cur.WriteRune(runes[i])
			default:
				cur.WriteRune(r)
			}
		case r == '\'':
			inSingle = true
			hasToken = true
		case r == '"':
			inDouble = true
			hasToken = true
		case r == '\\' && i+1 < len(runes):
			i++
			cur.WriteRune(runes[i])
			hasToken = true
		case unicode.IsSpace(r):
			if hasToken {
				tokens = append(tokens, cur.String())
				cur.Reset()
				hasToken = false
			}
		default:
			cur.WriteRune(r)
			hasToken = true
		}
	}
	if inSingle || inDouble {
		return nil, errors.New("unterminated quote")
	}
	if hasToken {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}
