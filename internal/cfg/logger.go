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

package cfg

import (
	"fmt"
	glog "log"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var mu sync.Mutex
var loggers = make(map[string]*LogHandle)

var log = GetLogger("main")
var cloudLogLevel = logrus.InfoLevel
var appendTime bool = true

func initFileLoggers(logFile string) {
	if logFile != "stderr" && logFile != "/dev/stderr" && logFile != "" {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Errorf("Couldn't open file %v for writing logs", logFile)
			return
		}
		for _, l := range loggers {
			l.Out = file
		}
		err = redirectStdout(file)
		if err != nil {
			log.Errorf("Couldn't redirect STDOUT to the log file %v", logFile)
			return
		}
		err = redirectStderr(file)
		if err != nil {
			log.Errorf("Couldn't redirect STDERR to the log file %v", logFile)
			return
		}
	}
	appendTime = true
}

func SetCloudLogLevel(level logrus.Level) {
	cloudLogLevel = level

	for k, logr := range loggers {
		if k != "main" && k != "fuse" {
			logr.Level = level
		}
	}
}

type LogHandle struct {
	logrus.Logger

	name string
	Lvl  *logrus.Level
}

func (l *LogHandle) Format(e *logrus.Entry) ([]byte, error) {
	// Mon Jan 2 15:04:05 -0700 MST 2006
	timestamp := ""
	lvl := e.Level
	if l.Lvl != nil {
		lvl = *l.Lvl
	}

	if appendTime {
		const timeFormat = "2006/01/02 15:04:05.000000"

		timestamp = e.Time.Format(timeFormat) + " "
	}

	str := fmt.Sprintf("%v%v.%v %v",
		timestamp,
		l.name,
		strings.ToUpper(lvl.String()),
		e.Message)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}

	str += "\n"
	return []byte(str), nil
}

// for aws.Logger
func (l *LogHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

func GetLogger(name string) *LogHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		if name != "main" && name != "fuse" {
			logger.Level = cloudLogLevel
		}
		return logger
	} else {
		logger := NewLogger(name)
		loggers[name] = logger
		if name != "main" && name != "fuse" {
			logger.Level = cloudLogLevel
		}
		return logger
	}
}

func GetStdLogger(l *LogHandle, lvl logrus.Level) *glog.Logger {
	return glog.New(l.WriterLevel(lvl), "", 0)
}

// retryablehttp logs messages using Printf("[DEBUG|ERR] message")
// logrus.Logger's Printf maps to INFO level, so this logger parses
// the the message to map to the correct log level

type RetryHTTPLogger struct {
	*LogHandle
}

const DEBUG_TAG = "[DEBUG]"
const ERR_TAG = "[ERR]"

func (logger RetryHTTPLogger) Printf(format string, args ...interface{}) {
	// unfortunately logrus.ParseLevel uses "error" instead of "err"
	// so we have to map this ourselves
	if strings.HasPrefix(format, DEBUG_TAG) {
		logger.LogHandle.Debugf(format[len(DEBUG_TAG)+1:], args...)
	} else if strings.HasPrefix(format, ERR_TAG) {
		logger.LogHandle.Errorf(format[len(ERR_TAG)+1:], args...)
	} else {
		logger.LogHandle.Infof(format, args...)
	}

}
