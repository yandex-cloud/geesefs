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
	geesefs "github.com/yandex-cloud/geesefs/api"
	. "github.com/yandex-cloud/geesefs/api/common"
	. "github.com/yandex-cloud/geesefs/internal"
	"github.com/yandex-cloud/geesefs/internal/pb"
	"google.golang.org/grpc"

	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"context"

	"github.com/jacobsa/fuse"
	"github.com/kardianos/osext"
	"github.com/urfave/cli"

	daemon "github.com/sevlyar/go-daemon"

	"net/http"
	_ "net/http/pprof"
)

var log = GetLogger("main")

func registerSIGINTHandler(fs *Goofys, flags *FlagStorage) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			if s == syscall.SIGUSR1 {
				log.Infof("Received %v", s)
				fs.SigUsr1()
				continue
			}

			log.Infof("Received %v, attempting to unmount...", s)

			err := TryUnmount(flags.MountPoint)
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

var waitedForSignal os.Signal

func waitForSignal(wg *sync.WaitGroup) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGUSR2)

	wg.Add(1)
	go func() {
		waitedForSignal = <-signalChan
		wg.Done()
	}()
}

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

// Mount the file system based on the supplied arguments, returning a
// fuse.MountedFileSystem that can be joined to wait for unmounting.
func mount(
	ctx context.Context,
	bucketName string,
	flags *FlagStorage) (fs *Goofys, mfs *fuse.MountedFileSystem, conns *ConnPool, err error) {
	if flags.ClusterMode {
		return geesefs.MountCluster(ctx, bucketName, flags)

	} else {
		fs, mfs, err = geesefs.Mount(ctx, bucketName, flags)
		conns = nil
		return
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

var Version = "use `make build' to fill version hash correctly"

func main() {
	VersionHash = Version

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

		if !flags.Foreground {
			var wg sync.WaitGroup
			waitForSignal(&wg)

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
				wg.Wait()
				if waitedForSignal == syscall.SIGUSR1 {
					return
				} else {
					return fuse.EINVAL
				}
			} else {
				// kill our own waiting goroutine
				kill(os.Getpid(), syscall.SIGUSR1)
				wg.Wait()
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
		var mfs *fuse.MountedFileSystem
		var fs *Goofys
		var conns *ConnPool
		fs, mfs, conns, err = mount(
			context.Background(),
			bucketName,
			flags)

		if err != nil {
			if !flags.Foreground {
				kill(os.Getppid(), syscall.SIGUSR2)
			}
			log.Fatalf("Mounting file system: %v", err)
			// fatal also terminates itself
		} else {
			if !flags.Foreground {
				kill(os.Getppid(), syscall.SIGUSR1)
			}
			log.Println("File system has been successfully mounted.")
			// Let the user unmount with Ctrl-C (SIGINT)
			registerSIGINTHandler(fs, flags)

			// Drop root privileges
			if flags.Setuid != 0 {
				syscall.Setuid(flags.Setuid)
			}
			if flags.Setgid != 0 {
				syscall.Setgid(flags.Setgid)
			}

			// Wait for the file system to be unmounted.
			err = mfs.Join(context.Background())
			if err != nil {
				err = fmt.Errorf("MountedFileSystem.Join: %v", err)
				return
			}
			fs.SyncFS(nil)
			if conns != nil {
				_ = conns.BroadConfigurable(
					func(ctx context.Context, conn *grpc.ClientConn) error {
						_, err := pb.NewRecoveryClient(conn).Unmount(ctx, &pb.UnmountRequest{})
						return err
					},
					false,
				)
			}

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
