package main

import (
	"reflect"
	"testing"
)

func TestShellSplit(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty", "", nil},
		{"simple", "ffmpeg -i in.mp4 out.mp4", []string{"ffmpeg", "-i", "in.mp4", "out.mp4"}},
		{"double quoted arg with space", `render --title "hello world" --frame 1`,
			[]string{"render", "--title", "hello world", "--frame", "1"}},
		{"single quoted arg with space", `sh -c 'echo hello world'`,
			[]string{"sh", "-c", "echo hello world"}},
		{"escaped space outside quotes", `cmd path\ with\ space`, []string{"cmd", "path with space"}},
		{"escaped quote inside double quotes", `echo "say \"hi\""`, []string{"echo", `say "hi"`}},
		{"extra whitespace collapses", "  a   b  ", []string{"a", "b"}},
		{"argv template reference survives", "blender --frame ${ORCH_PARAM_FRAME}",
			[]string{"blender", "--frame", "${ORCH_PARAM_FRAME}"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := shellSplit(tc.input)
			if err != nil {
				t.Fatalf("shellSplit(%q) error: %v", tc.input, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("shellSplit(%q) = %#v, want %#v", tc.input, got, tc.want)
			}
		})
	}
}

func TestShellSplitUnterminatedQuote(t *testing.T) {
	if _, err := shellSplit(`echo "unterminated`); err == nil {
		t.Error("expected an error for an unterminated double quote")
	}
	if _, err := shellSplit(`echo 'unterminated`); err == nil {
		t.Error("expected an error for an unterminated single quote")
	}
}
