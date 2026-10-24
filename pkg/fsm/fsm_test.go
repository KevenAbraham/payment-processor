package fsm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/KevenAbraham/payment-processor/pkg/fsm"
)

type payload struct {
	visited []string
	score   float64
}

func record(p *payload, name string) {
	p.visited = append(p.visited, name)
}

func TestRunHappyPath(t *testing.T) {
	tests := []struct {
		name      string
		wantTrace int
		wantScore float64
	}{
		{
			name:      "happy path",
			wantTrace: 2,
			wantScore: 0.9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := payload{}
			m := fsm.New[payload]("start").
				Handle("start", func(ctx context.Context, data *payload) (fsm.State, error) {
					record(data, "start")
					return "score", nil
				}).
				Handle("score", func(ctx context.Context, data *payload) (fsm.State, error) {
					record(data, "score")
					data.score = 0.9
					return "done", nil
				}).
				Terminal("done")

			trace, err := m.Run(context.Background(), &p)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(trace) != tt.wantTrace {
				t.Errorf("expected %d transitions, got %d", tt.wantTrace, len(trace))
			}

			if p.score != tt.wantScore {
				t.Errorf("expected score %f, got %f", tt.wantScore, p.score)
			}
		})
	}
}

func TestFallbackKeepsPipelineAvailable(t *testing.T) {
	dependencyDown := errors.New("upstream timeout")
	p := payload{}
	m := fsm.New[payload]("start").
		Handle("start", func(ctx context.Context, data *payload) (fsm.State, error) {
			record(data, "start")
			return "ml", nil
		}).
		Handle("ml", func(ctx context.Context, data *payload) (fsm.State, error) {
			return "", dependencyDown
		}).
		FallbackTo("ml", "rules_only").
		Handle("rules_only", func(ctx context.Context, data *payload) (fsm.State, error) {
			record(data, "rules_only")
			return "done", nil
		}).
		Terminal("done")

	trace, err := m.Run(context.Background(), &p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.visited) == 0 || p.visited[len(p.visited)-1] != "rules_only" {
		t.Errorf("expected last visited state rules_only, got %v", p.visited)
	}
	if len(trace) < 2 {
		t.Fatalf("expected at least 2 transitions, got %d", len(trace))
	}
	if !errors.Is(trace[1].Err, dependencyDown) {
		t.Errorf("expected dependency error in trace, got %v", trace[1].Err)
	}
}

func TestErrorWithoutFallbackAborts(t *testing.T) {
	boom := errors.New("boom")
	p := payload{}
	m := fsm.New[payload]("start").
		Handle("start", func(ctx context.Context, data *payload) (fsm.State, error) {
			return "", boom	
		}).
		Terminal("done")
	
	_, err := m.Run(context.Background(), &p)
	if !errors.Is(err, boom) {
		t.Errorf("unexpected error %v, got %v", boom, err)
	}
}