package log

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func New() *zap.Logger {
	cfg := zap.NewProductionConfig()

	logger, err := cfg.Build(zap.AddStacktrace(zap.FatalLevel))
	if err != nil {
		panic(err)
	}

	return logger
}

func NewDevelopment() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	logger, err := cfg.Build(zap.AddStacktrace(zap.ErrorLevel))
	if err != nil {
		panic(err)
	}

	return logger
}

// WithPackage adds a package tag to the logger, using the package name of the caller.
func WithPackage(logger *zap.Logger) *zap.Logger {
	const skipOffset = 1 // skip WithPackage

	_, file, _, ok := runtime.Caller(skipOffset)
	if !ok {
		return logger
	}

	packageName := filepath.Base(filepath.Dir(file))
	return logger.With(zap.String("package", packageName))
}

// WithSpan adds datadog span trace id for datadog https://docs.datadoghq.com/tracing/connect_logs_and_traces/go/
func WithSpan(ctx context.Context, logger *zap.Logger) *zap.Logger {
	if span, ok := tracer.SpanFromContext(ctx); ok {
		spanContext := span.Context()
		return logger.With(
			zap.String("dd.trace_id", strconv.Itoa(int(spanContext.TraceID()))),
			zap.String("dd.span_id", strconv.Itoa(int(spanContext.SpanID()))),
		)
	}

	return logger
}
