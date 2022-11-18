package services

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/log"
)

type MockSystemManager struct {
	GContext          context.Context
	GLogger           *zap.Logger
	GTracer           opentracing.Tracer
	GServiceContext   context.Context
	GServiceWaitGroup *sync.WaitGroup
	GShutdownHooks    []ShutdownHook
	GPreShutdownHooks []PreShutdownHook
}

func (m *MockSystemManager) Context() context.Context {
	return m.GContext
}

func (m *MockSystemManager) Logger() *zap.Logger {
	return m.GLogger
}

func (m *MockSystemManager) Tracer() opentracing.Tracer {
	return m.GTracer
}

func (m *MockSystemManager) ServiceContext() context.Context {
	return m.GServiceContext
}

func (m *MockSystemManager) ServiceWaitGroup() *sync.WaitGroup {
	return m.GServiceWaitGroup
}

func (m *MockSystemManager) State() ServiceState {
	return Running
}

func (m *MockSystemManager) AddShutdownHook(hook ShutdownHook) {
	m.GShutdownHooks = append(m.GShutdownHooks, hook)
}

func (m *MockSystemManager) AddPreShutdownHook(hook PreShutdownHook) {
	m.GPreShutdownHooks = append(m.GPreShutdownHooks, hook)
}

func (m *MockSystemManager) WaitForInterrupt() {
}

func (m *MockSystemManager) Shutdown() {
	for _, hook := range m.GPreShutdownHooks {
		hook()
	}

	for _, hook := range m.GShutdownHooks {
		hook()
	}
}

func NewMockSystemManager() SystemManager {
	logger := log.NewDevelopment()

	manager := &MockSystemManager{
		GContext:          context.Background(),
		GServiceContext:   context.Background(),
		GServiceWaitGroup: new(sync.WaitGroup),
		GLogger:           logger,
		GTracer:           &opentracing.NoopTracer{},
		GShutdownHooks:    make([]ShutdownHook, 0),
	}
	zap.ReplaceGlobals(manager.GLogger)
	return manager
}
