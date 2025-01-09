// Copyright 2021 Yandex LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"os"
	"syscall"

	"github.com/yandex-cloud/geesefs/core/cfg"
	"github.com/yandex-cloud/geesefs/core"
)

var signalsToHandle = []os.Signal{ os.Interrupt, syscall.SIGTERM }

func isSigUsr1(s os.Signal) bool {
	return false
}

const canDaemonize = false

type Daemonizer struct {
}

func NewDaemonizer() *Daemonizer {
	return &Daemonizer{}
}

func (p *Daemonizer) Daemonize(logFile string) error {
	return nil
}

func (p *Daemonizer) Cancel() {
}

func (p *Daemonizer) Wait() bool {
	return true
}

func (p *Daemonizer) NotifySuccess(success bool) {
}

// Mount the file system based on the supplied arguments, returning a
// MountedFS that can be joined to wait for unmounting.
func mount(
	ctx context.Context,
	bucketName string,
	flags *cfg.FlagStorage) (fs *core.Goofys, mfs core.MountedFS, err error) {
	return core.MountWin(ctx, bucketName, flags)
}

func messagePath() {
}

func messageArg0() {
}

func setuid(uid int) error {
	return nil
}

func setgid(gid int) error {
	return nil
}
