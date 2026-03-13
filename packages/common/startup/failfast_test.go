package startup

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
)

func TestFailFastWithExit(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	exitCode := 0
	failFastWithExit(logger, func(code int) { exitCode = code }, "edge", "nats", errors.New("connection refused"), "check NATS_URL")

	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	out := buf.String()
	if !strings.Contains(out, "service=edge") {
		t.Fatalf("expected service field in log, got %q", out)
	}
	if !strings.Contains(out, "dependency=nats") {
		t.Fatalf("expected dependency field in log, got %q", out)
	}
	if !strings.Contains(out, "connection refused") {
		t.Fatalf("expected error details in log, got %q", out)
	}
	if !strings.Contains(out, "check NATS_URL") {
		t.Fatalf("expected hint in log, got %q", out)
	}
}

func TestRun(t *testing.T) {
	checker := &fakeHealthCheck{}
	if err := Run(context.Background(), checker); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !checker.validated || !checker.dependenciesChecked {
		t.Fatalf("expected both checks to run")
	}
}

func TestRunValidateError(t *testing.T) {
	checker := &fakeHealthCheck{validateErr: errors.New("bad env")}
	if err := Run(context.Background(), checker); err == nil {
		t.Fatal("expected validation error")
	}
	if checker.dependenciesChecked {
		t.Fatal("expected dependency checks not to run when validation fails")
	}
}

func TestRunDependencyError(t *testing.T) {
	checker := &fakeHealthCheck{dependencyErr: errors.New("nats down")}
	if err := Run(context.Background(), checker); err == nil {
		t.Fatal("expected dependency error")
	}
	if !checker.validated || !checker.dependenciesChecked {
		t.Fatal("expected validation and dependency checks to run")
	}
}

type fakeHealthCheck struct {
	validated           bool
	dependenciesChecked bool
	validateErr         error
	dependencyErr       error
}

func (f *fakeHealthCheck) ValidateConfig(_ context.Context) error {
	f.validated = true
	return f.validateErr
}

func (f *fakeHealthCheck) CheckDependencies(_ context.Context) error {
	f.dependenciesChecked = true
	return f.dependencyErr
}
