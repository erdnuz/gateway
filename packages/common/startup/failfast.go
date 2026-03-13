package startup

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// ExitFunc enables testing fail-fast behavior without calling os.Exit.
type ExitFunc func(code int)

// FailFast logs a structured startup failure with actionable context and exits.
func FailFast(component, dependency string, err error, hint string) {
	failFastWithExit(log.Default(), os.Exit, component, dependency, err, hint)
}

func failFastWithExit(logger *log.Logger, exit ExitFunc, component, dependency string, err error, hint string) {
	if logger == nil {
		logger = log.Default()
	}
	if exit == nil {
		exit = os.Exit
	}

	comp := strings.TrimSpace(component)
	if comp == "" {
		comp = "unknown"
	}
	dep := strings.TrimSpace(dependency)
	if dep == "" {
		dep = "unknown"
	}
	action := strings.TrimSpace(hint)
	if action == "" {
		action = "verify configuration and dependency availability"
	}

	if err != nil {
		logger.Printf("startup_fail service=%s dependency=%s error=%q hint=%q", comp, dep, err.Error(), action)
	} else {
		logger.Printf("startup_fail service=%s dependency=%s error=%q hint=%q", comp, dep, "unknown error", action)
	}
	exit(1)
}

// CheckError wraps a startup check failure with component/dependency labels.
func CheckError(component, dependency string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s check failed for %s: %w", strings.TrimSpace(component), strings.TrimSpace(dependency), err)
}
