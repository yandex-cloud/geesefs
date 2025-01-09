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

//go:build !windows

package cfg

import (
	"log/syslog"
	"os"

	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

var syslogHook *logrus_syslog.SyslogHook

func InitLoggers(logFile string) {
	if logFile == "syslog" {
		var err error
		syslogHook, err = logrus_syslog.NewSyslogHook("", "", syslog.LOG_DEBUG, "")
		if err != nil {
			// we are the child process and we cannot connect to syslog,
			// probably because we are in a container without syslog
			// nothing much we can do here, printing to stderr doesn't work
			return
		}
		for _, l := range loggers {
			l.Hooks.Add(syslogHook)
		}
		appendTime = false
	} else {
		initFileLoggers(logFile)
	}
}

func NewLogger(name string) *LogHandle {
	l := &LogHandle{name: name}
	l.Out = os.Stderr
	l.Formatter = l
	l.Level = logrus.InfoLevel
	l.Hooks = make(logrus.LevelHooks)
	if syslogHook != nil {
		l.Hooks.Add(syslogHook)
	}
	return l
}
