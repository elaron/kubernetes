/*
Copyright 2014 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//
// utility functions to setup rbd volume using the rbd-nbd client
// mainly implement diskMapper interface
//

package rbd

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/golang/glog"
)

type RBDNbd struct {
	RBDUtil
}

// IsSupported show whether we have the kernel rbd-nbd plugin available or not
func (rk *RBDNbd) IsSupported(plugin *rbdPlugin) bool {
	// rbd-nbd might not be installed in a system. In such situation we'll fallback
	// to krbd. See createDiskMapper() in rbd_util.go
	exec := plugin.host.GetExec(plugin.GetPluginName())
	if _, err := exec.Run("rbd-nbd", []string{"list-mapped"}); err != nil {
		return false
	}
	return true
}
func (rk *RBDNbd) MapDisk(b rbdMounter) (string, error) {
	var err error
	var output []byte

	// modprobe
	_, err = b.plugin.execCommand("modprobe", []string{"nbd"})
	if err != nil {
		return "", fmt.Errorf("rbd: failed to modprobe nbd error:%v", err)
	}

	// TODO: we might avoid mapping the same image twice by examining  sysfs entires.
	// NBD exposes /sys/block/nbd<num>/pid containing a PID of an user-space adapter.
	// Having this information allow us to match /proc/<pid>/cmdline against the imgSpec.
	imgSpec := b.Pool + "/" + b.Image

	// no need to fence readOnly
	var lockSpec string
	if !(&b).GetAttributes().ReadOnly {
		lockSpec = "--exclusive"
	}

	if b.Secret != "" {
		secret_opt = []string{"--key=" + b.Secret}
	} else {
		secret_opt = []string{"-k", b.Keyring}
	}

	// rbd map
	l := len(b.Mon)
	// avoid mount storm, pick a host randomly
	start := rand.Int() % l
	// iterate all hosts until mount succeeds.
	for i := start; i < start+l; i++ {
		mon := b.Mon[i%l]
		glog.V(1).Infof("rbd(nbd): map mon %s", mon)
		args := []string{"map", imgSpec, "--id", b.Id, "-m", mon, lockSpec}
		args = append(args, secret_opt...)
		cmd, err = b.exec.Run("rbd-nbd", args...)
		output = string(cmd)
		if err == nil {
			break
		}

		glog.V(1).Infof("rbd(nbd): map error %v %s", err, string(output))
	}
	if err != nil {
		return "", fmt.Errorf("rbd: map failed %v %s", err, string(output))
	}

	devicePath := strings.TrimSpace(string(output[:]))
	return devicePath, nil
}

func (rk *RBDNbd) UnmapDisk(plugin *rbdPlugin, device string) error {
	// rbd unmap
	exec := plugin.host.GetExec(plugin.GetPluginName())
	_, err := exec.Run("rbd-nbd", []string{"unmap", device})
	if err != nil {
		return fmt.Errorf("rbd: failed to unmap device %s:Error: %v", device, err)
	}

	glog.Infof("rbd: successfully unmap device %s", device)

	return nil
}
