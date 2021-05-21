// Package endpoints ...
// Copyright 2019 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package endpoints

import (
	"encoding/json"
	"io/fs"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var fslog = logrus.WithField("component", "FileSystemWatcher")

type filesystemRetriever struct {
	roots         []string
	refreshPeriod time.Duration
	targets       []Target
	lock          sync.RWMutex
	watching      bool
}

const defaultRefreshPeriod = 5 * time.Second

// FileSystemRetriever creates a TargetRetriever that returns targets defined in a '.d-style' directory on the file system
func FileSystemRetriever(roots []string, refreshPeriod time.Duration) (TargetRetriever, error) {
	if refreshPeriod == 0 {
		refreshPeriod = defaultRefreshPeriod
	}
	return &filesystemRetriever{
		roots:         roots,
		refreshPeriod: refreshPeriod,
	}, nil
}

func (f *filesystemRetriever) GetTargets() ([]Target, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.targets, nil
}

func (f *filesystemRetriever) Watch() error {
	if f.watching {
		return errors.New("already watching")
	}
	if err := f.refresh(); err != nil {
		return err
	}
	go func() {
		for {
			if err := f.refresh(); err != nil {
				fslog.WithError(err).Warnf("failed to refresh. Ignoring")
			}
			time.Sleep(f.refreshPeriod)
		}
	}()
	return nil
}

func (f *filesystemRetriever) refresh() error {
	refreshedTargets := make([]Target, 0)
	for _, root := range f.roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				return nil
			}
			data, err := ioutil.ReadFile(d.Name())
			if err != nil {
				return err
			}
			var targetConfig TargetConfig
			if err := json.Unmarshal(data, &targetConfig); err != nil {
				return err
			}
			target, err := EndpointToTarget(targetConfig)
			if err != nil {
				return err
			}
			refreshedTargets = append(refreshedTargets, target...)
			return nil
		})
		if err != nil {
			return err
		}
	}
	f.lock.Lock()
	f.targets = refreshedTargets
	f.lock.Unlock()
	return nil
}

func (f *filesystemRetriever) Name() string {
	return "filesystem"
}
