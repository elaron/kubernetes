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
// utility functions to setup rbd volume using the kernel RBD (krbd) client
// mainly implement diskMapper interface
//

package rbd

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"

	"github.com/golang/glog"
)

const (
	kubeLockMagic = "kubelet_lock_magic_"
)

// search /sys/bus for rbd device that matches given pool and image
func getDevFromImageAndPool(pool, image string) (string, bool) {
	// /sys/bus/rbd/devices/X/name and /sys/bus/rbd/devices/X/pool
	sys_path := "/sys/bus/rbd/devices"
	if dirs, err := ioutil.ReadDir(sys_path); err == nil {
		for _, f := range dirs {
			// pool and name format:
			// see rbd_pool_show() and rbd_name_show() at
			// https://github.com/torvalds/linux/blob/master/drivers/block/rbd.c
			name := f.Name()
			// first match pool, then match name
			poolFile := path.Join(sys_path, name, "pool")
			poolBytes, err := ioutil.ReadFile(poolFile)
			if err != nil {
				glog.V(4).Infof("Error reading %s: %v", poolFile, err)
				continue
			}
			if strings.TrimSpace(string(poolBytes)) != pool {
				glog.V(4).Infof("Device %s is not %q: %q", name, pool, string(poolBytes))
				continue
			}
			imgFile := path.Join(sys_path, name, "name")
			imgBytes, err := ioutil.ReadFile(imgFile)
			if err != nil {
				glog.V(4).Infof("Error reading %s: %v", imgFile, err)
				continue
			}
			if strings.TrimSpace(string(imgBytes)) != image {
				glog.V(4).Infof("Device %s is not %q: %q", name, image, string(imgBytes))
				continue
			}
			// found a match, check if device exists
			devicePath := "/dev/rbd" + name
			if _, err := os.Lstat(devicePath); err == nil {
				return devicePath, true
			}
		}
	}
	return "", false
}

type RBDKernel struct {
	RBDUtil
}

func (rk *RBDKernel) MapDisk(b rbdMounter) (string, error) {
	var err error
	var output []byte

	devicePath, found := waitForPath(b.Pool, b.Image, 1)
	if !found {
		_, err := b.exec.Run("modprobe", "rbd")
		if err != nil {
			glog.Warningf("rbd: failed to load rbd kernel module:%v", err)
		}

		// Currently, we don't acquire advisory lock on image, but for backward
		// compatibility, we need to check if the image is being used by nodes running old kubelet.
		found, rbdOutput, err := util.rbdStatus(&b)
		if err != nil {
			return "", fmt.Errorf("error: %v, rbd output: %v", err, rbdOutput)
		}
		if found {
			glog.Infof("rbd image %s/%s is still being used ", b.Pool, b.Image)
			return "", fmt.Errorf("rbd image %s/%s is still being used. rbd output: %s", b.Pool, b.Image, rbdOutput)
		}

		// rbd map
		l := len(b.Mon)
		// avoid mount storm, pick a host randomly
		start := rand.Int() % l
		// iterate all hosts until mount succeeds.
		for i := start; i < start+l; i++ {
			mon := b.Mon[i%l]
			glog.V(1).Infof("rbd: map mon %s", mon)
			if b.Secret != "" {
				output, err = b.exec.Run("rbd",
					"map", b.Image, "--pool", b.Pool, "--id", b.Id, "-m", mon, "--key="+b.Secret)
			} else {
				output, err = b.exec.Run("rbd",
					"map", b.Image, "--pool", b.Pool, "--id", b.Id, "-m", mon, "-k", b.Keyring)
			}
			if err == nil {
				break
			}
			glog.V(1).Infof("rbd: map error %v, rbd output: %s", err, string(output))
		}
		if err != nil {
			return "", fmt.Errorf("rbd: map failed %v, rbd output: %s", err, string(output))
		}
		devicePath, found = waitForPath(b.Pool, b.Image, 10)
		if !found {
			return "", fmt.Errorf("Could not map image %s/%s, Timeout after 10s", b.Pool, b.Image)
		}
	}

	return devicePath, nil
}

func (rk *RBDKernel) UnmapDisk(b rbdMounter, device string) error {
	// rbd unmap
	_, err := b.plugin.execCommand("rbd", []string{"unmap", device})
	if err != nil {
		return fmt.Errorf("rbd: failed to unmap device %s:Error: %v", device, err)
	}

	rk.defencing(b)

	glog.Infof("rbd: successfully unmap device %s", device)
	return nil
}
