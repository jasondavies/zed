package zbuf

import (
	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zio"
)

type Control struct {
	Message interface{}
}

var _ error = (*Control)(nil)

func (c *Control) Error() string {
	return "control"
}

type SetChannel int
type EndChannel int

func NoControl(rc zio.ReadCloser) zio.ReadCloser {
	return &noControl{rc}
}

type noControl struct {
	zio.ReadCloser
}

func (n *noControl) Read() (*zed.Value, error) {
	for {
		val, err := n.ReadCloser.Read()
		if _, ok := err.(*Control); ok {
			continue
		}
		return val, err
	}
}

type ProgressReadCloser interface {
	zio.ReadCloser
	Progress() Progress
}

func MeterReader(rc zio.ReadCloser) ProgressReadCloser {
	return &meterReader{ReadCloser: rc}
}

type meterReader struct {
	zio.ReadCloser
	progress Progress
}

func (m *meterReader) Progress() Progress {
	return m.progress.Copy()
}

func (m *meterReader) Read() (*zed.Value, error) {
	for {
		val, err := m.ReadCloser.Read()
		if ctrl, ok := err.(*Control); ok {
			if progress, ok := ctrl.Message.(Progress); ok {
				m.progress = progress
			}
		}
		return val, err
	}
}

func ReadAll(r zio.Reader) (arr *Array, err error) {
	if err := zio.Copy(arr, r); err != nil {
		return nil, err
	}
	return
}
