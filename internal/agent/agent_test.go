package agent

import (
	"reflect"
	"sort"
	"testing"
)

func TestBuildEnv(t *testing.T) {
	jobEnv := map[string]string{"FOO": "bar", "ORCH_PARAM_FRAME": "job-set"}
	params := map[string]string{"frame": "42", "seed": "7"}
	identity := map[string]string{"ORCH_JOB_ID": "job-1"}

	env, collisions := buildEnv(jobEnv, params, identity)

	if env["FOO"] != "bar" {
		t.Errorf("expected job env FOO to survive, got %q", env["FOO"])
	}
	if env["ORCH_PARAM_FRAME"] != "42" {
		t.Errorf("expected param to win on collision, got %q", env["ORCH_PARAM_FRAME"])
	}
	if env["ORCH_PARAM_SEED"] != "7" {
		t.Errorf("expected ORCH_PARAM_SEED=7, got %q", env["ORCH_PARAM_SEED"])
	}
	if env["ORCH_JOB_ID"] != "job-1" {
		t.Errorf("expected identity var to be set, got %q", env["ORCH_JOB_ID"])
	}

	sort.Strings(collisions)
	want := []string{"ORCH_PARAM_FRAME"}
	if !reflect.DeepEqual(collisions, want) {
		t.Errorf("collisions = %v, want %v", collisions, want)
	}
}

func TestBuildEnvNoCollision(t *testing.T) {
	env, collisions := buildEnv(map[string]string{"A": "1"}, map[string]string{"b": "2"}, nil)
	if len(collisions) != 0 {
		t.Errorf("expected no collisions, got %v", collisions)
	}
	if env["A"] != "1" || env["ORCH_PARAM_B"] != "2" {
		t.Errorf("unexpected env: %v", env)
	}
}

func TestExpandArgv(t *testing.T) {
	env := map[string]string{"ORCH_PARAM_FRAME": "42", "ORCH_TASK_ID": "t-1"}
	command := []string{"render", "--frame", "${ORCH_PARAM_FRAME}", "--task=${ORCH_TASK_ID}", "--literal-dollar"}

	got := expandArgv(command, env)
	want := []string{"render", "--frame", "42", "--task=t-1", "--literal-dollar"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expandArgv() = %v, want %v", got, want)
	}
}

func TestExpandArgvUnknownRefBecomesEmpty(t *testing.T) {
	got := expandArgv([]string{"${NOT_SET}"}, map[string]string{})
	if got[0] != "" {
		t.Errorf("expected unknown reference to expand to empty string, got %q", got[0])
	}
}
