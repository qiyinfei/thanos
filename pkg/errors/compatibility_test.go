package errors

import (
	stderr "errors"
	"fmt"
	"testing"
)

// --- Go113+ ---

func TestStdNewAndCause(t *testing.T) {
	testNewAndCause(t, func(format string, args ...interface{}) error {
		return stderr.New(fmt.Sprintf(format, args...))
	})
}

func TestStdErrorfAndCause(t *testing.T) {
	testNewAndCause(t, fmt.Errorf)
}

func TestStdWrapAndCause(t *testing.T) {
	testWrapAndCause(t, func(err error, format string, args ...interface{}) error {
		// Wrapping nil has different semantics.
		if err == nil {
			err = NilErrorWrappedErr
		}

		return fmt.Errorf(format+": %w", append(append([]interface{}{}, args...), err)...)
	})
}

// Critical functions from pkg/error
