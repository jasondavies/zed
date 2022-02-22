package runtime

import (
	"context"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/compiler/ast"
	"github.com/brimdata/zed/lakeparse"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/runtime/op"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"go.uber.org/zap"
)

func NewQueryOnReader(ctx context.Context, zctx *zed.Context, program ast.Proc, reader zio.Reader, logger *zap.Logger) (zbuf.Scanner, error) {
	pctx := op.NewContext(ctx, zctx, logger)
	flowgraph, err := compiler.CompileForInternal(pctx, program, reader)
	if err != nil {
		pctx.Cancel()
		return nil, err
	}
	return scanner{flowgraph}, nil
}

func NewQueryOnOrderedReader(ctx context.Context, zctx *zed.Context, program ast.Proc, reader zio.Reader, layout order.Layout, logger *zap.Logger) (zbuf.Scanner, error) {
	pctx := op.NewContext(ctx, zctx, logger)
	flowgraph, err := compiler.CompileForInternalWithOrder(pctx, program, reader, layout)
	if err != nil {
		pctx.Cancel()
		return nil, err
	}
	return scanner{flowgraph}, nil
}

func NewQueryOnFileSystem(ctx context.Context, zctx *zed.Context, program ast.Proc, readers []zio.Reader, adaptor op.DataAdaptor) (zbuf.Scanner, error) {
	pctx := op.NewContext(ctx, zctx, nil)
	flowgraph, err := compiler.CompileForFileSystem(pctx, program, readers, adaptor)
	if err != nil {
		pctx.Cancel()
		return nil, err
	}
	return scanner{flowgraph}, nil
}

func NewQueryOnLake(ctx context.Context, zctx *zed.Context, program ast.Proc, lake op.DataAdaptor, head *lakeparse.Commitish, logger *zap.Logger) (zbuf.Scanner, error) {
	pctx := op.NewContext(ctx, zctx, logger)
	flowgraph, err := compiler.CompileForLake(pctx, program, lake, 0, head)
	if err != nil {
		pctx.Cancel()
		return nil, err
	}
	return scanner{flowgraph}, nil
}

type scanner struct{ runtime *compiler.Runtime }

func (s scanner) Progress() zbuf.Progress { return s.runtime.Meter().Progress() }

func (s scanner) Pull(done bool) (zbuf.Batch, error) {
	if done {
		s.runtime.Context().Cancel()
	}
	return s.runtime.Puller().Pull(done)
}
