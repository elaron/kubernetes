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
	_, err := plugin.execLocalCommand("rbd-nbd", []string{"list-mapped"})
	if err != nil {
		return false
	}
	return true
}
func (rk *RBDNbd) MapDisk(b rbdMounter) (string, error) {
	// modprobe
	err := b.plugin.modprobeKernelModule("nbd")
	if nil != err {
		return "", err
	}

	// TODO: we might avoid mapping the same image twice by examining  sysfs entires.
	// NBD exposes /sys/block/nbd<num>/pid containing a PID of an user-space adapter.
	// Having this information allow us to match /proc/<pid>/cmdline against the imgSpec.

	//prepare command arguments
	imgSpec := b.Pool + "/" + b.Image
	args := []string{"map", imgSpec, "--id", b.Id}

	// no need to fence readOnly
	if !(&b).GetAttributes().ReadOnly {
		args = append(args, "--exclusive")
	}

	if b.Secret != "" {
		args = append(args, "--key"+b.Secret)
	} else {
		args = append(args, "-k=",b.Keyring)
	}

	output, err := b.plugin.execClusterCommand(b.Mon, "rbd-nbd", args)
	if err != nil {
		return "", fmt.Errorf("rbd: map failed %v %s", err, string(output))
	}

	devicePath := strings.TrimSpace(string(output))
	return devicePath, nil
}

func (rk *RBDNbd) UnmapDisk(plugin *rbdPlugin, device string) error {
	// rbd unmap
	_, err := plugin.execLocalCommand("rbd-nbd", []string{"unmap", device})
	if err != nil {
		return fmt.Errorf("rbd: failed to unmap device %s:Error: %v", device, err)
	}

	glog.Infof("rbd: successfully unmap device %s", device)

	return nil
}
