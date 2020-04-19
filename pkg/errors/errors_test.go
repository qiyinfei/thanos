package errors

import (
	"fmt"
	"testing"

	"github.com/thanos-io/thanos/pkg/errors/internal/testutil"
)

func TestNewAndCause(t *testing.T) {
	testNewAndCause(t, New)
}

func testNewAndCause(t *testing.T, newFn func(format string, args ...interface{}) error) {
	for _, tcase := range []struct {
		name   string
		format string
		args   []interface{}

		expectedErrMsg         string
		expectedErrMsgExtended string
	}{
		{
			name:   "new empty",
			format: "", args: nil,

			expectedErrMsg:         "", // Should we mention empty err?
			expectedErrMsgExtended: "",
		},
		{
			name:   "new 1",
			format: "1", args: nil,

			expectedErrMsg:         "1",
			expectedErrMsgExtended: "1",
		},
		{
			name:   "new with args",
			format: "%s %d %v", args: []interface{}{"lmao", 1, []int{0, 1, 2}},

			expectedErrMsg:         "lmao 1 [0 1 2]",
			expectedErrMsgExtended: "lmao 1 [0 1 2]",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := newFn(tcase.format, tcase.args...)
			testutil.NotOk(t, err)
			testutil.Equals(t, tcase.expectedErrMsg, err.Error())
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%s", err))
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%v", err))
			testutil.Equals(t, tcase.expectedErrMsgExtended, fmt.Sprintf("%+v", err))

			testutil.Equals(t, err, Cause(err))
		})
	}
}

func TestWrapAndCause(t *testing.T) {
	testWrapAndCause(t, Wrap)
}

func testWrapAndCause(t *testing.T, wrapFn func(err error, format string, args ...interface{}) error) {
	for _, tcase := range []struct {
		name       string
		err        error
		wrapFormat string
		wrapArgs   []interface{}

		expectedErrMsg         string
		expectedErrMsgExtended string
		expectedCause          error
	}{
		{
			name: "nil wrap should wrap NilErrorWrappedErr",
			err:  nil, wrapFormat: "", wrapArgs: nil,

			expectedErrMsg:         ": unknown error (nil error wrapped)",
			expectedErrMsgExtended: ": unknown error (nil error wrapped)",
			expectedCause:          NilErrorWrappedErr,
		},
		{
			name: "nil wrap with wrap fmt should wrap NilErrorWrappedErr",
			err:  nil, wrapFormat: "wrapping error", wrapArgs: nil,

			expectedErrMsg:         "wrapping error: unknown error (nil error wrapped)",
			expectedErrMsgExtended: "wrapping error: unknown error (nil error wrapped)",
			expectedCause:          NilErrorWrappedErr,
		},
		{
			name: "error(nil) wrap with wrap fmt should wrap NilErrorWrappedErr",
			err:  error(nil), wrapFormat: "wrapping error", wrapArgs: nil,

			expectedErrMsg:         "wrapping error: unknown error (nil error wrapped)",
			expectedErrMsgExtended: "wrapping error: unknown error (nil error wrapped)",
			expectedCause:          NilErrorWrappedErr,
		},
		{
			name: "certain error",
			err:  New("some error1"), wrapFormat: "", wrapArgs: nil,

			expectedErrMsg:         ": some error1",
			expectedErrMsgExtended: ": some error1",
			expectedCause:          New("some error1"),
		},
		{
			name: "certain error with single wrap",
			err:  New("some error2"), wrapFormat: "wrapping error", wrapArgs: nil,

			expectedErrMsg:         "wrapping error: some error2",
			expectedErrMsgExtended: "wrapping error: some error2",
			expectedCause:          New("some error2"),
		},
		{
			name: "multiple wraps",
			err:  wrapFn(wrapFn(wrapFn(New("some error3"), "wrap1"), "wrap2"), "wrap3"), wrapFormat: "wrap4", wrapArgs: nil,

			expectedErrMsg:         "wrap4: wrap3: wrap2: wrap1: some error3",
			expectedErrMsgExtended: "wrap4: wrap3: wrap2: wrap1: some error3",
			expectedCause:          New("some error3"),
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := wrapFn(tcase.err, tcase.wrapFormat, tcase.wrapArgs...)
			testutil.NotOk(t, err)
			testutil.Equals(t, tcase.expectedErrMsg, err.Error())
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%s", err))
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%v", err))
			testutil.Equals(t, tcase.expectedErrMsgExtended, fmt.Sprintf("%+v", err))

			testutil.Equals(t, tcase.expectedCause, Cause(err))
		})
	}
}

func TestNewWrap_UnsupportedFormattingVerb(t *testing.T) {
	err := New("error1")
	testutil.Equals(t, "(not supported \"d\" verb used) for string value. ('s' 'v' '+v' allowed only for errFrame value \"error1\")", fmt.Sprintf("%d", err))
	testutil.Equals(t, "(not supported \"d\" verb used) for string value. ('s' 'v' '+v' allowed only for errFrame value \"wrapping: error1\")", fmt.Sprintf("%d", Wrap(err, "wrapping")))
}
