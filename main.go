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
	"github.com/yandex-cloud/geesefs/core"
	"github.com/yandex-cloud/geesefs/core/cfg"

	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"context"

	"github.com/urfave/cli"

	"net/http"
	_ "net/http/pprof"
)

var log = cfg.GetLogger("main")

func registerSIGINTHandler(fs *core.Goofys, mfs core.MountedFS, flags *cfg.FlagStorage) {
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

	app := cfg.NewApp()

	var flags *cfg.FlagStorage
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
		flags = cfg.PopulateFlags(c)
		if flags == nil {
			cli.ShowAppHelp(c)
			err = fmt.Errorf("invalid arguments")
			return
		}
		defer func() {
			time.Sleep(time.Second)
			flags.Cleanup()
		}()

		var daemonizer *Daemonizer
		if !canDaemonize {
			flags.Foreground = true
		}
		logFile := flags.LogFile
		if logFile == "" {
			if flags.Foreground {
				logFile = "stderr"
			} else {
				logFile = "syslog"
			}
		}
		if !flags.Foreground {
			daemonizer = NewDaemonizer()
			// Do not close stderr before mounting to print mount errors
			initLogFile := logFile
			if initLogFile == "syslog" {
				initLogFile = "stderr"
			}
			err := daemonizer.Daemonize(initLogFile)
			if err != nil {
				return err
			}
		}
		cfg.InitLoggers(logFile)

		pprof := flags.PProf
		if pprof == "" && os.Getenv("PPROF") != "" {
			pprof = os.Getenv("PPROF")
		}
		if pprof != "" {
			go func() {
				addr := pprof
				if strings.Index(addr, ":") == -1 {
					addr = "127.0.0.1:" + addr
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
				daemonizer.NotifySuccess(false)
			}
			log.Fatalf("Mounting file system: %v", err)
			// fatal also terminates itself
		} else {
			log.Println("File system has been successfully mounted.")
			if !flags.Foreground {
				daemonizer.NotifySuccess(true)
				os.Stderr.Close()
				os.Stdout.Close()
			}
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
			fs.SyncTree(nil)

			log.Println("Successfully exiting.")
		}
		return
	}

	err := app.Run(cfg.MessageMountFlags(os.Args))
	if err != nil {
		if flags != nil && !flags.Foreground && child != nil {
			log.Fatalln("Unable to mount file system, see syslog for details")
		}
		os.Exit(1)
	}
}
