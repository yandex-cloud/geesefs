// Copyright 2019 Ka-Hing Cheung
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

package core

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const CGROUP_PATH = "/proc/self/cgroup"
const CGROUP_FOLDER_PREFIX = "/sys/fs/cgroup/memory"
const MEM_LIMIT_FILE_SUFFIX = "/memory.limit_in_bytes"
const MEM_USAGE_FILE_SUFFIX = "/memory.usage_in_bytes"

func getCgroupAvailableMem() (retVal uint64, err error) {
	// get the memory cgroup for self and send limit - usage for the cgroup

	data, err := ioutil.ReadFile(CGROUP_PATH)
	if err != nil {
		log.Debugf("Unable to read file %s error: %s", CGROUP_PATH, err)
		return 0, err
	}

	path, err := getMemoryCgroupPath(string(data))
	if err != nil {
		return 0, err
	}

	path = memoryCgroupDir(CGROUP_FOLDER_PREFIX, path)

	mem_limit, err := readFileAndGetValue(filepath.Join(path, MEM_LIMIT_FILE_SUFFIX))
	if err != nil {
		log.Debugf("Unable to get memory limit from cgroup file %v error: %v", path, err)
		return 0, err
	}

	mem_usage, err := readFileAndGetValue(filepath.Join(path, MEM_USAGE_FILE_SUFFIX))
	if err != nil {
		log.Debugf("Unable to get memory usage from cgroup file %v error: %v", path, err)
		return 0, err
	}

	return (mem_limit - mem_usage), nil
}

// memoryCgroupDir returns the directory holding the memory limit/usage files
// for cgroupPath under root. Newer versions of docker mount those files
// directly under root (/sys/fs/cgroup/memory/) rather than under
// root/docker/$container_id/, so fall back to root when the cgroup's own
// directory does not exist.
func memoryCgroupDir(root, cgroupPath string) string {
	dir := filepath.Join(root, cgroupPath)
	if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
		return dir
	}
	return root
}

func getMemoryCgroupPath(data string) (string, error) {

	/*
	   Content of /proc/self/cgroup

	   11:hugetlb:/
	   10:memory:/user.slice
	   9:cpuset:/
	   8:blkio:/user.slice
	   7:perf_event:/
	   6:net_prio,net_cls:/
	   5:cpuacct,cpu:/user.slice
	   4:devices:/user.slice
	   3:freezer:/
	   2:pids:/
	   1:name=systemd:/user.slice/user-1000.slice/session-1759.scope
	*/

	dataArray := strings.Split(data, "\n")
	for index := range dataArray {
		kvArray := strings.Split(dataArray[index], ":")
		if len(kvArray) == 3 {
			if kvArray[1] == "memory" {
				return kvArray[2], nil
			}
		}
	}

	return "", errors.New("Unable to get memory cgroup path")
}

func readFileAndGetValue(path string) (uint64, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Debugf("Unable to read file %v error: %v", path, err)
		return 0, err
	}

	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}
