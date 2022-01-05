package zio

import (
	"bytes"
	"io"

	"github.com/brimdata/zed"
)

// WarningFilter returns a WriteCloser wrapping wc that filters Zed warnings
// (Zed errors whose value begins with "warnings: "), invoking warn for each.
func WarningFilter(wc WriteCloser, warn func(*zed.Value) error) WriteCloser {
	return &warningFilter{wc, warn}
}

// WarningFilterWithWriter is like WarningFilter but writes each warning to w
// followed by a newline.
func WarningFilterWithWriter(zwc WriteCloser, w io.Writer) WriteCloser {
	return &warningFilter{zwc, func(warning *zed.Value) error {
		w.Write(warning.Bytes)
		w.Write([]byte("\n"))
		return nil
	}}
}

type warningFilter struct {
	WriteCloser
	warn func(*zed.Value) error
}

func (w *warningFilter) Write(val *zed.Value) error {
	if zed.TypeUnder(val.Type) == zed.TypeError && bytes.HasPrefix(val.Bytes, []byte("warning: ")) {
		return w.warn(val)
	}
	return w.WriteCloser.Write(val)
}
