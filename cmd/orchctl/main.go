// Command orchctl is the command-line client.
//
// It exists so the fleet is usable before the UI is open, and so the
// declarative bootstrap from §13 has something to apply it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/gen/orch/v1/orchv1connect"
)

const usage = `orchctl - GPU compute-sharing orchestrator client

Usage:
  orchctl [--server URL] <command> [flags]

Commands:
  fleet                    Show nodes, slots and who is borrowing what
  pools                    Show pools with quota, borrow and lend counters
  jobs                     List recent jobs
  job <job-id>             Show a job and its tasks
  submit                   Submit a job
  cancel <job-id>          Cancel a job
  leases                   List active leases
  policy                   Show scoring weights and registered scorers
  set-policy               Change the scoring preset or weights
  explain <task-id>        Show why a task was placed where it was
  bootstrap <file.yaml>    Apply a declarative fleet definition, idempotently
  trigger <kind>           Fire a trigger-console event
  join-token               Mint a single-use node join token
  watch                    Follow the control plane event stream

Run 'orchctl <command> --help' for command flags.
`

func main() {
	server := flag.String("server", envOr("ORCH_SERVER", "http://localhost:9443"), "control plane URL")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	client := orchv1connect.NewOrchServiceClient(http.DefaultClient, *server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := dispatch(ctx, client, *server, args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dispatch(ctx context.Context, c orchv1connect.OrchServiceClient, server string, args []string) error {
	switch args[0] {
	case "fleet":
		return cmdFleet(ctx, c)
	case "pools":
		return cmdPools(ctx, c)
	case "jobs":
		return cmdJobs(ctx, c)
	case "job":
		if len(args) < 2 {
			return errors.New("usage: orchctl job <job-id>")
		}
		return cmdJob(ctx, c, args[1])
	case "submit":
		return cmdSubmit(ctx, c, args[1:])
	case "cancel":
		if len(args) < 2 {
			return errors.New("usage: orchctl cancel <job-id>")
		}
		_, err := c.CancelJob(ctx, connect.NewRequest(&orchv1.CancelJobRequest{JobId: args[1]}))
		if err == nil {
			fmt.Println("cancelled", args[1])
		}
		return err
	case "leases":
		return cmdLeases(ctx, c)
	case "policy":
		return cmdPolicy(ctx, c)
	case "set-policy":
		return cmdSetPolicy(ctx, c, args[1:])
	case "explain":
		if len(args) < 2 {
			return errors.New("usage: orchctl explain <task-id>")
		}
		return cmdExplain(ctx, c, args[1])
	case "bootstrap":
		if len(args) < 2 {
			return errors.New("usage: orchctl bootstrap <file.yaml>")
		}
		return cmdBootstrap(ctx, c, args[1])
	case "trigger":
		return cmdTrigger(ctx, c, args[1:])
	case "join-token":
		return cmdJoinToken(ctx, c, args[1:])
	case "watch":
		return cmdWatch(ctx, server)
	}
	return fmt.Errorf("unknown command %q", args[0])
}

// ---------------------------------------------------------------------------

func cmdFleet(ctx context.Context, c orchv1connect.OrchServiceClient) error {
	res, err := c.ListNodes(ctx, connect.NewRequest(&orchv1.ListNodesRequest{}))
	if err != nil {
		return err
	}

	w := table()
	fmt.Fprintln(w, "NODE\tPOOL\tSTATE\tSLOTS\tVRAM\tCC\tTEMP\tLEND\tKIND")
	for _, n := range res.Msg.GetNodes() {
		free, used, borrowed := 0, 0, 0
		var maxTemp float64
		var vram, cc []string
		for _, s := range n.GetSlots() {
			switch s.GetState() {
			case "free":
				free++
			case "allocated":
				used++
				if s.GetBorrowed() {
					borrowed++
				}
			}
			if t := s.GetHealth().GetTemperatureC(); t > maxTemp {
				maxTemp = t
			}
			vram = appendUnique(vram, fmt.Sprintf("%d", s.GetVramGb()))
			cc = appendUnique(cc, s.GetComputeCapability())
		}

		kind := "real"
		if n.GetSimulated() {
			// Badged everywhere it appears. A demo that discloses what is
			// simulated up front is stronger than one where the audience finds
			// out later.
			kind = "SIMULATED"
		}
		slots := fmt.Sprintf("%d free / %d used", free, used)
		if borrowed > 0 {
			slots += fmt.Sprintf(" (%d borrowed)", borrowed)
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%.0fC\t%t\t%s\n",
			n.GetHostname(), n.GetPoolId(), n.GetState(), slots,
			strings.Join(vram, ","), strings.Join(cc, ","),
			maxTemp, n.GetLendable(), kind)
	}
	return w.Flush()
}

func cmdPools(ctx context.Context, c orchv1connect.OrchServiceClient) error {
	res, err := c.ListPools(ctx, connect.NewRequest(&orchv1.ListPoolsRequest{}))
	if err != nil {
		return err
	}

	w := table()
	fmt.Fprintln(w, "POOL\tNODES\tSLOTS\tUSED\tGUARANTEED\tLENDABLE\tCEILING\tBORROWED\tLENT")
	for _, p := range res.Msg.GetPools() {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%t\t%d\t%d\t%d\n",
			p.GetPoolId(), len(p.GetNodeIds()), p.GetTotalSlots(), p.GetUsedSlots(),
			p.GetGuaranteedSlots(), p.GetLendable(), p.GetBorrowCeiling(),
			p.GetBorrowedSlots(), p.GetLentSlots())
	}
	return w.Flush()
}

func cmdJobs(ctx context.Context, c orchv1connect.OrchServiceClient) error {
	res, err := c.ListJobs(ctx, connect.NewRequest(&orchv1.ListJobsRequest{}))
	if err != nil {
		return err
	}

	w := table()
	fmt.Fprintln(w, "JOB\tNAME\tOWNER\tPOOL\tSTATE\tDONE\tRUN\tQUEUE\tFAIL\tTOTAL")
	for _, j := range res.Msg.GetJobs() {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\n",
			j.GetJobId(), j.GetName(), j.GetOwner(), j.GetPoolId(), j.GetState(),
			j.GetTasksDone(), j.GetTasksRunning(), j.GetTasksQueued(),
			j.GetTasksFailed(), j.GetTasksTotal())
	}
	return w.Flush()
}

func cmdJob(ctx context.Context, c orchv1connect.OrchServiceClient, jobID string) error {
	res, err := c.GetJob(ctx, connect.NewRequest(&orchv1.GetJobRequest{JobId: jobID}))
	if err != nil {
		return err
	}
	j := res.Msg.GetJob()
	fmt.Printf("%s  %s  pool=%s  state=%s  %d/%d done\n\n",
		j.GetJobId(), j.GetName(), j.GetPoolId(), j.GetState(),
		j.GetTasksDone(), j.GetTasksTotal())

	w := table()
	fmt.Fprintln(w, "IDX\tTASK\tSTATE\tATTEMPT\tPREEMPTIONS\tNODE\tPARAMS")
	for _, t := range res.Msg.GetTasks() {
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%s\t%s\n",
			t.GetIndex(), t.GetTaskId(), shortState(t.GetState()), t.GetAttempt(),
			t.GetPreemptions(), orDash(t.GetNodeId()), formatMap(t.GetParams()))
	}
	return w.Flush()
}

func cmdLeases(ctx context.Context, c orchv1connect.OrchServiceClient) error {
	res, err := c.ListLeases(ctx, connect.NewRequest(&orchv1.ListLeasesRequest{}))
	if err != nil {
		return err
	}

	w := table()
	fmt.Fprintln(w, "LEASE\tPOOL\tNODE\tSLOTS\tMODE\tBORROWED\tCORES\tRAM_GB\tAGE")
	for _, l := range res.Msg.GetLeases() {
		age := time.Since(l.GetGrantedAt().AsTime()).Truncate(time.Second)
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%t\t%.1f\t%d\t%s\n",
			shortID(l.GetLeaseId()), l.GetPoolId(), l.GetNodeId(), len(l.GetSlotIds()),
			shortMode(l.GetMode()), l.GetBorrowed(), l.GetHostShareCores(),
			l.GetHostShareRamGb(), age)
	}
	return w.Flush()
}

func cmdPolicy(ctx context.Context, c orchv1connect.OrchServiceClient) error {
	res, err := c.GetPolicy(ctx, connect.NewRequest(&orchv1.GetPolicyRequest{}))
	if err != nil {
		return err
	}
	p := res.Msg.GetPolicy()
	fmt.Printf("preset: %s  (generation %d)\n", p.GetPreset(), p.GetReloadGeneration())
	fmt.Printf("presets available: %s\n\n", strings.Join(res.Msg.GetPresets(), ", "))

	w := table()
	fmt.Fprintln(w, "SCORER\tWEIGHT")
	for _, name := range res.Msg.GetAvailableScorers() {
		fmt.Fprintf(w, "%s\t%.2f\n", name, p.GetWeights()[name])
	}
	return w.Flush()
}

func cmdSetPolicy(ctx context.Context, c orchv1connect.OrchServiceClient, args []string) error {
	fs := flag.NewFlagSet("set-policy", flag.ExitOnError)
	preset := fs.String("preset", "", "preset name: throughput, latency or fair-share")
	weights := fs.String("weights", "", "comma-separated overrides, e.g. fit=0.5,thermal=0.2")
	if err := fs.Parse(args); err != nil {
		return err
	}

	w := map[string]float64{}
	for _, pair := range strings.Split(*weights, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			continue
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("weight %q: %w", pair, err)
		}
		w[k] = f
	}

	res, err := c.SetPolicy(ctx, connect.NewRequest(&orchv1.SetPolicyRequest{
		Policy: &orchv1.Policy{Preset: *preset, Weights: w},
	}))
	if err != nil {
		return err
	}
	fmt.Printf("policy updated: preset=%s generation=%d\n",
		res.Msg.GetPolicy().GetPreset(), res.Msg.GetPolicy().GetReloadGeneration())
	return nil
}

func cmdExplain(ctx context.Context, c orchv1connect.OrchServiceClient, taskID string) error {
	res, err := c.Explain(ctx, connect.NewRequest(&orchv1.ExplainRequest{TaskId: taskID}))
	if err != nil {
		return err
	}
	e := res.Msg.GetExplanation()
	fmt.Printf("task %s placed on %s (slots %s)\n\n",
		e.GetTaskId(), e.GetChosenNodeId(), strings.Join(e.GetChosenSlotIds(), ","))

	// Collect scorer names from the candidates so this stays correct when a new
	// scoring dimension is registered.
	var names []string
	seen := map[string]bool{}
	for _, cand := range e.GetCandidates() {
		for n := range cand.GetPerScorer() {
			if !seen[n] {
				seen[n] = true
				names = append(names, n)
			}
		}
	}
	sort.Strings(names)

	w := table()
	fmt.Fprintf(w, "CANDIDATE\tTOTAL\tBORROW\t%s\n", strings.ToUpper(strings.Join(names, "\t")))
	for _, cand := range e.GetCandidates() {
		row := []string{cand.GetNodeId(), fmt.Sprintf("%.3f", cand.GetTotal()),
			fmt.Sprintf("%t", cand.GetWouldBorrow())}
		for _, n := range names {
			row = append(row, fmt.Sprintf("%.2f", cand.GetPerScorer()[n]))
		}
		marker := ""
		if cand.GetChosen() {
			marker = "  <- chosen"
		}
		fmt.Fprintln(w, strings.Join(row, "\t")+marker)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	if len(e.GetFilteredOut()) > 0 {
		fmt.Println("\nfiltered out:")
		for _, f := range e.GetFilteredOut() {
			fmt.Println("  " + f)
		}
	}
	return nil
}

func cmdBootstrap(ctx context.Context, c orchv1connect.OrchServiceClient, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	res, err := c.Bootstrap(ctx, connect.NewRequest(&orchv1.BootstrapRequest{Yaml: string(raw)}))
	if err != nil {
		return err
	}
	fmt.Println(res.Msg.GetDetail())
	return nil
}

func cmdTrigger(ctx context.Context, c orchv1connect.OrchServiceClient, args []string) error {
	kinds := map[string]orchv1.TriggerKind{
		"node-failure":     orchv1.TriggerKind_TRIGGER_KIND_NODE_FAILURE,
		"node-recover":     orchv1.TriggerKind_TRIGGER_KIND_NODE_RECOVER,
		"thermal-throttle": orchv1.TriggerKind_TRIGGER_KIND_THERMAL_THROTTLE,
		"clear-throttle":   orchv1.TriggerKind_TRIGGER_KIND_CLEAR_THROTTLE,
		"xid-error":        orchv1.TriggerKind_TRIGGER_KIND_XID_ERROR,
		"reclaim":          orchv1.TriggerKind_TRIGGER_KIND_RECLAIM,
	}

	if len(args) == 0 {
		names := make([]string, 0, len(kinds))
		for k := range kinds {
			names = append(names, k)
		}
		sort.Strings(names)
		return fmt.Errorf("usage: orchctl trigger <%s> [--node N] [--pool P]",
			strings.Join(names, "|"))
	}

	kind, ok := kinds[args[0]]
	if !ok {
		return fmt.Errorf("unknown trigger %q", args[0])
	}

	fs := flag.NewFlagSet("trigger", flag.ExitOnError)
	node := fs.String("node", "", "node ID")
	pool := fs.String("pool", "", "pool ID (reclaim: every machine in the pool hosting a borrower)")
	whole := fs.Bool("whole-machine", true, "reclaim an intact machine rather than a single slot")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	res, err := c.Trigger(ctx, connect.NewRequest(&orchv1.TriggerRequest{
		Kind: kind, NodeId: *node, PoolId: *pool, WholeMachine: *whole,
	}))
	if err != nil {
		return err
	}
	fmt.Println(res.Msg.GetDetail())
	for _, id := range res.Msg.GetAffectedLeaseIds() {
		fmt.Println("  evicted", id)
	}
	return nil
}

func cmdJoinToken(ctx context.Context, c orchv1connect.OrchServiceClient, args []string) error {
	fs := flag.NewFlagSet("join-token", flag.ExitOnError)
	pool := fs.String("pool", "default", "pool the joining node lands in")
	if err := fs.Parse(args); err != nil {
		return err
	}

	res, err := c.IssueJoinToken(ctx, connect.NewRequest(&orchv1.IssueJoinTokenRequest{PoolId: *pool}))
	if err != nil {
		return err
	}
	fmt.Println(res.Msg.GetToken())
	return nil
}

// ---------------------------------------------------------------------------

func table() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
}

func appendUnique(xs []string, v string) []string {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}

func shortState(s orchv1.TaskState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "TASK_STATE_"))
}

func shortMode(m orchv1.LeaseMode) string {
	return strings.ToLower(strings.TrimPrefix(m.String(), "LEASE_MODE_"))
}

func shortID(id string) string {
	if len(id) > 14 {
		return id[:14]
	}
	return id
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func formatMap(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
