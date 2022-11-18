package log

import (
	"fmt"

	temporal "go.temporal.io/sdk/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// FromTemporal wraps a temporal logger in a compatibility layer to produce a *zap.Logger.
func FromTemporal(logger temporal.Logger) *zap.Logger {
	wrapper := &zapper{
		logger: logger,
	}
	return zap.New(wrapper)
}

type zapper struct {
	logger temporal.Logger
}

func (z *zapper) Enabled(lvl zapcore.Level) bool {
	// Since we're not sure what levels are enabled on the
	// underlying logger, always return true; this hurts performance but
	// not correctness.
	return true
}

func (z *zapper) With(fs []zapcore.Field) zapcore.Core {
	return &zapper{
		logger: z.with(fs),
	}
}

func (z *zapper) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	// Since Enabled is always true, there's no need to check it here.
	return ce.AddCore(ent, z)
}

func (z *zapper) Write(ent zapcore.Entry, fs []zapcore.Field) error {
	logger := z.with(fs)

	var logFunc func(string, ...interface{})
	switch ent.Level {
	case zapcore.DebugLevel:
		logFunc = logger.Debug
	case zapcore.InfoLevel:
		logFunc = logger.Info
	case zapcore.WarnLevel:
		logFunc = logger.Warn
	case zapcore.ErrorLevel, zapcore.DPanicLevel:
		logFunc = logger.Error
	default:
		return fmt.Errorf("temporal-to-zap compatibility wrapper got unknown level %v", ent.Level)
	}

	// The underlying logger timestamps the entry, so we can drop
	// everything but the message.
	logFunc(ent.Message)
	return nil
}

func (z *zapper) Sync() error {
	// The underlying logger doesn't expose a way to flush buffered messages.
	return nil
}

func (z *zapper) with(fs []zapcore.Field) temporal.Logger {
	if len(fs) == 0 {
		return z.logger
	}

	encoder := zapcore.NewMapObjectEncoder()
	for _, f := range fs {
		f.AddTo(encoder)
	}

	keyvals := make([]interface{}, 0, len(encoder.Fields)*2)
	for k, v := range encoder.Fields {
		keyvals = append(keyvals, k, v)
	}

	return temporal.With(z.logger, keyvals...)
}
