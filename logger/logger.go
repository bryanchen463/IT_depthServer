package logger

import "go.uber.org/zap/zapcore"

type Logger interface {
	Debug(msg string, args ...zapcore.Field)
	Info(msg string, args ...zapcore.Field)
	Warn(msg string, args ...zapcore.Field)
	Error(msg string, args ...zapcore.Field)
	Fatal(msg string, args ...zapcore.Field)
}
