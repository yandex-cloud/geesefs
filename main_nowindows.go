// +build !windows

// Copyright 2015 - 2017 Ka-Hing Cheung
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
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"github.com/kardianos/osext"

	"github.com/yandex-cloud/geesefs/api/common"
	"github.com/yandex-cloud/geesefs/internal"
)

var signalsToHandle = []os.Signal{ os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1 }

func isSigUsr1(s os.Signal) bool {
	return s == syscall.SIGUSR1
}

var waitedForSignal os.Signal

func kill(pid int, s os.Signal) (err error) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	defer p.Release()

	err = p.Signal(s)
	if err != nil {
		return err
	}
	return
}

func waitForSignal(wg *sync.WaitGroup) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGUSR2)

	wg.Add(1)
	go func() {
		waitedForSignal = <-signalChan
		wg.Done()
	}()
}

func waitedForSignalOk() bool {
	return waitedForSignal == syscall.SIGUSR1
}

func notifyParent(success bool) {
	sig := syscall.SIGUSR1
	if !success {
		sig = syscall.SIGUSR2
	}
	kill(os.Getppid(), sig)
}

// Mount the file system based on the supplied arguments, returning a
// MountedFS that can be joined to wait for unmounting.
func mount(
	ctx context.Context,
	bucketName string,
	flags *common.FlagStorage) (fs *internal.Goofys, mfs internal.MountedFS, err error) {
	if flags.ClusterMode {
		return internal.MountCluster(ctx, bucketName, flags)
	} else {
		return internal.MountFuse(ctx, bucketName, flags)
	}
}

func messagePath() {
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "PATH=") {
			return
		}
	}

	// mount -a seems to run goofys without PATH
	// usually fusermount is in /bin
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func messageArg0() {
	var err error
	os.Args[0], err = osext.Executable()
	if err != nil {
		panic(fmt.Sprintf("Unable to discover current executable: %v", err))
	}
}

func setuid(uid int) error {
	return syscall.Setuid(uid)
}

func setgid(gid int) error {
	return syscall.Setgid(gid)
}
