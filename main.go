// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2015 - 2017 Google Inc. All Rights Reserved.
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
	. "github.com/yandex-cloud/geesefs/api/common"
	. "github.com/yandex-cloud/geesefs/internal"

	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"context"

	"github.com/urfave/cli"

	daemon "github.com/sevlyar/go-daemon"

	"net/http"
	_ "net/http/pprof"
)

var log = GetLogger("main")

func registerSIGINTHandler(fs *Goofys, mfs MountedFS, flags *FlagStorage) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, signalsToHandle...)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			if isSigUsr1(s) {
				log.Infof("Received %v", s)
				fs.SigUsr1()
				continue
			}

			log.Infof("Received %v, attempting to unmount...", s)

			err := mfs.Unmount()
			if err != nil {
				log.Errorf("Failed to unmount in response to %v: %v", s, err)
			} else {
				log.Printf("Successfully unmounted %v in response to %v",
					flags.MountPoint, s)
				return
			}
		}
	}()
}

func main() {
	messagePath()

	app := NewApp()

	var flags *FlagStorage
	var child *os.Process

	app.Action = func(c *cli.Context) (err error) {
		// We should get two arguments exactly. Otherwise error out.
		if len(c.Args()) != 2 {
			fmt.Fprintf(
				os.Stderr,
				"Error: %s takes exactly two arguments.\n\n",
				app.Name)
			cli.ShowAppHelp(c)
			os.Exit(1)
		}

		// Populate and parse flags.
		bucketName := c.Args()[0]
		flags = PopulateFlags(c)
		if flags == nil {
			cli.ShowAppHelp(c)
			err = fmt.Errorf("invalid arguments")
			return
		}
		defer func() {
			time.Sleep(time.Second)
			flags.Cleanup()
		}()

		var notifier *ParentNotifier
		if !flags.Foreground {
			notifier = NewParentNotifier()

			messageArg0()

			ctx := new(daemon.Context)
			if flags.LogFile == "stderr" || flags.LogFile == "/dev/stderr" {
				ctx.LogFileName = "/dev/stderr"
			}
			child, err = ctx.Reborn()

			if err != nil {
				panic(fmt.Sprintf("unable to daemonize: %v", err))
			}

			if flags.LogFile == "" {
				if flags.Foreground || child != nil {
					flags.LogFile = "stderr"
				} else {
					flags.LogFile = "syslog"
				}
			}

			InitLoggers(flags.LogFile)

			if child != nil {
				// attempt to wait for child to notify parent
				if notifier.Wait() {
					return
				} else {
					return syscall.EINVAL
				}
			} else {
				notifier.Cancel()
				defer ctx.Release()
			}

		} else {
			InitLoggers(flags.LogFile)
		}

		pprof := flags.PProf
		if pprof == "" && os.Getenv("PPROF") != "" {
			pprof = os.Getenv("PPROF")
		}
		if pprof != "" {
			go func() {
				addr := pprof
				if strings.Index(addr, ":") == -1 {
					addr = "127.0.0.1:"+addr
				}
				log.Println(http.ListenAndServe(addr, nil))
			}()
		}

		// Mount the file system.
		fs, mfs, err := mount(
			context.Background(),
			bucketName,
			flags)

		if err != nil {
			if !flags.Foreground {
				notifier.Notify(false)
			}
			log.Fatalf("Mounting file system: %v", err)
			// fatal also terminates itself
		} else {
			if !flags.Foreground {
				notifier.Notify(true)
			}
			log.Println("File system has been successfully mounted.")
			// Let the user unmount with Ctrl-C (SIGINT)
			registerSIGINTHandler(fs, mfs, flags)

			// Drop root privileges
			if flags.Setuid != 0 {
				setuid(flags.Setuid)
			}
			if flags.Setgid != 0 {
				setgid(flags.Setgid)
			}

			// Wait for the file system to be unmounted.
			err = mfs.Join(context.Background())
			if err != nil {
				err = fmt.Errorf("MountedFileSystem.Join: %v", err)
				return
			}
			fs.SyncFS(nil)

			log.Println("Successfully exiting.")
		}
		return
	}

	err := app.Run(MessageMountFlags(os.Args))
	if err != nil {
		if flags != nil && !flags.Foreground && child != nil {
			log.Fatalln("Unable to mount file system, see syslog for details")
		}
		os.Exit(1)
	}
}
