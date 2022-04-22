package function

import (
	"github.com/brimdata/zed"
	"github.com/brimdata/zed/runtime/expr"
)

func NewCompare() *Compare {
	return &Compare{
		fn: expr.NewValueCompareFn(true),
	}
}

type Compare struct {
	fn expr.CompareFn
}

func (e *Compare) Call(ctx zed.Allocator, args []zed.Value) *zed.Value {
	lhs, rhs := &args[0], &args[1]
	if lhs.IsError() || rhs.IsError() {
		if lhs.IsError() {
			return lhs
		}
		return rhs
	}
	cmp := e.fn(lhs, rhs)
	return ctx.NewValue(zed.TypeInt64, zed.EncodeInt(int64(cmp)))
}
