package utils

import (
	"sort"
	"strings"
	"sync"
)

var (
	_globalEnv *Env
	_once      sync.Once
)

func GlobalEnv() *Env {
	_once.Do(func() {
		_globalEnv = &Env{
			m: make(map[string]string),
		}
	})
	return _globalEnv
}

type Env struct {
	mu sync.RWMutex
	m  map[string]string
}

func (e *Env) Get(k string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.m[k]
	return v, ok
}

func (e *Env) Set(k string, v string) (before string) {
	e.mu.Lock()
	defer e.mu.Lock()
	ret := e.m[k]
	e.m[k] = v
	return ret
}

func (e *Env) GetKeysWithPrefix(keyPrefix string) []string {
	e.mu.RLock()
	defer e.mu.RLock()

	var keys []string
	for k := range e.m {
		if strings.HasPrefix(k, keyPrefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (e *Env) AllKeys() []string {
	e.mu.RLock()
	defer e.mu.RLock()

	var keys []string

	for k := range e.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
