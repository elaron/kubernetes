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
// utility functions to setup rbd volume
// mainly implement diskManager interface
//

package rbd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	fileutil "k8s.io/kubernetes/pkg/util/file"
	"k8s.io/kubernetes/pkg/util/node"
	"k8s.io/kubernetes/pkg/volume"
	volutil "k8s.io/kubernetes/pkg/volume/util"
)

const (
	imageWatcherStr = "watcher="
)

var (
	clientKubeLockMagicRe = regexp.MustCompile("client.* " + kubeLockMagic + ".*")
)

// stat a path, if not exists, retry maxRetries times
func waitForPath(pool, image string, maxRetries int) (string, bool) {
	for i := 0; i < maxRetries; i++ {
		devicePath, found := getDevFromImageAndPool(pool, image)
		if found {
			return devicePath, true
		}
		if i == maxRetries-1 {
			break
		}
		time.Sleep(time.Second)
	}
	return "", false
}

// make a directory like /var/lib/kubelet/plugins/kubernetes.io/pod/rbd/pool-image-image
func makePDNameInternal(host volume.VolumeHost, pool string, image string) string {
	return path.Join(host.GetPluginDir(rbdPluginName), "rbd", pool+"-image-"+image)
}

// RBDUtil implements diskManager interface.
type RBDUtil struct{}

var _ diskManager = &RBDUtil{}

type diskMapper interface {
	IsSupported(plugin *rbdPlugin) bool
	MapDisk(disk rbdMounter) (string, error)
	UnmapDisk(plugin *rbdPlugin, mntPath string) error
}

func createDiskMapper(backendType string, plugin *rbdPlugin) (diskMapper, error) {

	glog.V(1).Infof("rbd: creating diskMapper for backendType %s", backendType)

	switch strings.ToLower(backendType) {
	case BACKEND_TYPE_KRBD:
		return &RBDKernel{}, nil

	case BACKEND_TYPE_NBD:
		nbdMapper := &RBDNbd{}
		if false == nbdMapper.IsSupported(plugin) {
			glog.V(1).Infof("rbd: the rbd-nbd is not availble, fallbacking to krbd")
			return &RBDKernel{}, nil
		}
		return nbdMapper, nil

	default:
		return &RBDKernel{}, nil
	}
}

func (util *RBDUtil) MakeGlobalPDName(rbd rbd) string {
	return makePDNameInternal(rbd.plugin.host, rbd.Pool, rbd.Image)
}

func rbdErrors(runErr, resultErr error) error {
	if err, ok := runErr.(*exec.Error); ok {
		if err.Err == exec.ErrNotFound {
			return fmt.Errorf("rbd: rbd cmd not found")
		}
	}
	return resultErr
}

// best effort clean up orphaned locked if not used
func cleanUpOrphanLock(b rbdMounter, orphanLock string) {

	args := []string{"lock", "remove", b.Image, "--pool", b.Pool, "--id", b.Id}
	if "" != b.Secret {
		args = append(args, "--key="+b.Secret)
	} else {
		args = append(args, "-k", b.Keyring)
	}

	locks := clientKubeLockMagicRe.FindAllStringSubmatch(orphanLock, -1)
	for _, v := range locks {
		if len(v) > 0 {
			lockInfo := strings.Split(v[0], " ")
			if len(lockInfo) > 2 {
				args = append(args, lockInfo[1], lockInfo[0])
				output, err := b.plugin.execClusterCommand(b.Mon, "rbd", args)
				glog.Infof("remove orphaned locker %s from client %s: err %v, rbd output: %s", lockInfo[1], lockInfo[0], err, string(output))
			}
		}
	}
}

func addLockToImage(b rbdMounter, lock_id string) error {
	args := []string{"lock", "add", b.Image, lock_id, "--pool", b.Pool, "--id", b.Id}
	if "" != b.Secret {
		args = append(args, "--key="+b.Secret)
	} else {
		args = append(args, "-k", b.Keyring)
	}

	output, err := b.plugin.execClusterCommand(b.Mon, "rbd", args)
	if err == nil {
		glog.V(4).Infof("rbd: successfully add lock (locker_id: %s) on image: %s/%s with id %s , %s", lock_id, b.Pool, b.Image, b.Id, string(output))
	} else {
		glog.V(4).Infof("rbd: failed of adding lock (locker_id: %s) on image: %s/%s with id %s , %v", lock_id, b.Pool, b.Image, b.Id, err)
	}
	return err
}

func removeLockFromImage(b rbdMounter, lock_id, locker string) error {
	if len(locker) <= 0 {
		return fmt.Errorf("Remove lock %s from image %s fail, locker non-exist.", lock_id, b.Image)
	}

	args := []string{"lock", "remove", b.Image, lock_id, locker, "--pool", b.Pool, "--id", b.Id}
	if "" != b.Secret {
		args = append(args, "--key="+b.Secret)
	} else {
		args = append(args, "-k", b.Keyring)
	}

	output, err := b.plugin.execClusterCommand(b.Mon, "rbd", args)
	if err == nil {
		glog.V(4).Infof("rbd: successfully remove lock (locker_id: %s) on image: %s/%s with id %s, %s", lock_id, b.Pool, b.Image, b.Id, string(output))
	} else {
		glog.V(4).Infof("rbd: failed of remove lock (locker_id: %s) on image: %s/%s with id %s, %v", lock_id, b.Pool, b.Image, b.Id, err)
	}
	return err
}

// defencing, find locker name
func getLockerName(imageLockInfo, lock_id string) string {
	//rbd lock list's output looks like followed:
	// rbd lock list test0 --pool rbd
	//There is 1 exclusive lock on this image.
	//	Locker       ID       Address
	///client.44114 lockid00 192.168.56.131:0/2233191897
	var locker string
	ind := strings.LastIndex(imageLockInfo, lock_id) - 1
	for i := ind; i >= 0; i-- {
		if imageLockInfo[i] == '\n' {
			locker = imageLockInfo[(i + 1):ind]
			break
		}
	}
	return locker
}

// rbdLock acquires a lock on image if lock is true, otherwise releases if a
// lock is found on image.
func (util *RBDUtil) rbdLock(b rbdMounter, lock bool) error {

	// construct lock id using host name and a magic prefix
	lock_id := kubeLockMagic + node.GetHostname("")

	if len(b.adminId) == 0 {
		b.adminId = b.Id
		b.adminSecret = b.Secret
	}

	//get image lock info
	args := []string{"lock", "list", b.Image, "--pool", b.Pool, "--id", b.Id}
	if b.Secret != "" {
		args = append(args, "--key="+b.Secret)
	} else {
		args = append(args, "-k", b.Keyring)
	}
	output, err := b.plugin.execClusterCommand(b.Mon, "rbd", args)
	if nil != err {
		return err
	}
	imageLockInfo := string(output)

	//trying to add lock to image
	if true == lock {
		// check if lock is already held for this host by matching lock_id and rbd lock id
		if strings.Contains(imageLockInfo, lock_id) {
			// this host already holds the lock, exit
			glog.V(1).Infof("rbd: lock already held for %s", lock_id)
			return nil
		}

		// check if image is already used by other node
		used, rbdOutput, statusErr := util.rbdStatus(&b)
		if statusErr != nil {
			return fmt.Errorf("rbdStatus failed error %v, rbd output: %v", statusErr, rbdOutput)
		}
		if used {
			return fmt.Errorf("rbd image: %s/%s is already used by a node other than this node, rbd output: %v", b.Image, b.Pool, imageLockInfo)
		}

		//clean up orphaned locked if not used
		cleanUpOrphanLock(b, imageLockInfo)

		//hold a lock: rbd lock add
		return addLockToImage(b, lock_id)

	}

	//trying to remove lock from image
	if false == lock {
		locker := getLockerName(imageLockInfo, lock_id)

		//remove a lock if found: rbd lock remove
		return removeLockFromImage(b, lock_id, locker)
	}

	return nil
}

// AttachDisk attaches the disk on the node.
// If Volume is not read-only, acquire a lock on image first.
func (util *RBDUtil) AttachDisk(b rbdMounter) (string, error) {

	globalPDPath := util.MakeGlobalPDName(*b.rbd)
	if pathExists, pathErr := volutil.PathExists(globalPDPath); pathErr != nil {
		return "", fmt.Errorf("Error checking if path exists: %v", pathErr)
	} else if !pathExists {
		if err := os.MkdirAll(globalPDPath, 0750); err != nil {
			return "", err
		}
	}

	//map a block device
	mapper, err := createDiskMapper(b.BackendType, b.plugin)
	if err != nil {
		return "", fmt.Errorf("rbd: cannot create diskMapper: %v", err)
	}
	devicePath, err := mapper.MapDisk(b)
	if err != nil {
		return "", fmt.Errorf("rbd: cannot map block device, %v", err)
	}

	return devicePath, nil
}

func getBackendTypeByDevice(device string) string {
	if true == strings.Contains(device, "nbd") {
		return BACKEND_TYPE_NBD
	}
	return BACKEND_TYPE_KRBD
}

// DetachDisk detaches the disk from the node.
// It detaches device from the node if device is provided, and removes the lock
// if there is persisted RBD info under deviceMountPath.
func (util *RBDUtil) DetachDisk(plugin *rbdPlugin, deviceMountPath string, device string) error {

	if len(device) == 0 {
		return fmt.Errorf("DetachDisk %s failed , device is empty", deviceMountPath)
	}
	// rbd unmap
	backendType := getBackendTypeByDevice(device)
	mapper, err := createDiskMapper(backendType, plugin)
	err = mapper.UnmapDisk(plugin, device)
	if nil != err {
		return err
	}

	// Currently, we don't persist rbd info on the disk, but for backward
	// compatbility, we need to clean it if found.
	rbdFile := path.Join(deviceMountPath, "rbd.json")
	return util.cleanOldRBDFile(plugin, rbdFile)
}

// cleanOldRBDFile read rbd info from rbd.json file if it's found, and removes lock if found.
// At last, it removes rbd.json file.
func (util *RBDUtil) cleanOldRBDFile(plugin *rbdPlugin, rbdFile string) error {

	exists, err := fileutil.FileExists(rbdFile)
	if err != nil {
		return fmt.Errorf("Check rbd.json file %s fail, %v ", rbdFile, err)
	}
	if false == exists {
		return nil
	}

	glog.V(3).Infof("rbd: old rbd.json is found under %s, cleaning it", rbdFile)

	mounter := &rbdMounter{
		// util.rbdLock needs it to run command.
		rbd: newRBD("", "", "", "", false, plugin, util),
	}

	fp, err := os.Open(rbdFile)
	if err != nil {
		return fmt.Errorf("rbd: open err %s/%s", rbdFile, err)
	}
	defer fp.Close()

	decoder := json.NewDecoder(fp)
	if err = decoder.Decode(mounter); err != nil {
		return fmt.Errorf("rbd: decode %s err: %v ", rbdFile, err)
	}

	// remove rbd lock if found
	// the disk is not attached to this node anymore, so the lock on image
	// for this node can be removed safely
	err = util.rbdLock(*mounter, false)
	if err != nil {
		glog.Errorf("rbd: failed to clean %s", rbdFile)
		return err
	}
	os.Remove(rbdFile)
	glog.V(3).Infof("rbd: successfully remove %s", rbdFile)

	return nil
}

func (util *RBDUtil) CreateImage(p *rbdVolumeProvisioner) (r *v1.RBDPersistentVolumeSource, size int, err error) {

	capacity := p.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()
	// convert to MB that rbd defaults on
	sz := int(volume.RoundUpSize(volSizeBytes, 1024*1024))
	volSz := fmt.Sprintf("%d", sz)

	//prepare create image arguments
	args := []string{"create", p.rbdMounter.Image, "--size", volSz, "--pool", p.rbdMounter.Pool, "--id", p.rbdMounter.adminId, "--key=" + p.rbdMounter.adminSecret, "--image-format", p.rbdMounter.imageFormat}
	var features string

	if p.rbdMounter.imageFormat == rbdImageFormat2 {
		// if no image features is provided, it results in empty string
		// which disable all RBD image format 2 features as we expected
		features = strings.Join(p.rbdMounter.imageFeatures, ",")
		args = append(args, "--image-feature", features)
	}
	glog.V(4).Infof("rbd: create %s size %s format %s (features: %s) using pool %s id %s key %s", p.rbdMounter.Image, volSz, p.rbdMounter.imageFormat, features, p.rbdMounter.Pool, p.rbdMounter.adminId, p.rbdMounter.adminSecret)

	output, err := p.plugin.execClusterCommand(p.rbdMounter.Mon, "rbd", args)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create rbd image: %v, command output: %s", err, string(output))
	}

	return &v1.RBDPersistentVolumeSource{
		CephMonitors: p.rbdMounter.Mon,
		RBDImage:     p.rbdMounter.Image,
		RBDPool:      p.rbdMounter.Pool,
	}, sz, nil
}

func (util *RBDUtil) DeleteImage(p *rbdVolumeDeleter) error {

	found, rbdOutput, err := util.rbdStatus(p.rbdMounter)
	if err != nil {
		return fmt.Errorf("error %v, rbd output: %v", err, rbdOutput)
	}
	if found {
		glog.Info("rbd is still being used ", p.rbdMounter.Image)
		return fmt.Errorf("rbd image %s/%s is still being used, rbd output: %v", p.rbdMounter.Pool, p.rbdMounter.Image, rbdOutput)
	}

	args := []string{"rm", p.rbdMounter.Image, "--pool", p.rbdMounter.Pool, "--id", p.rbdMounter.adminId, "--key=" + p.rbdMounter.adminSecret}
	output, err := p.plugin.execClusterCommand(p.rbdMounter.Mon, "rbd", args)
	if nil != err {
		return fmt.Errorf("failed to delete rbd image %s, error %v, rbd output: %v", p.rbdMounter.Image, err, string(output))
	}
	return nil
}

// rbdStatus runs `rbd status` command to check if there is watcher on the image.
// cmd "rbd status" list the rbd client watch with the following output:
//
// # there is a watcher (exit=0)
// Watchers:
//   watcher=10.16.153.105:0/710245699 client.14163 cookie=1
//
// # there is no watcher (exit=0)
// Watchers: none
//
// Otherwise, exit is non-zero, for example:
//
// # image does not exist (exit=2)
// rbd: error opening image kubernetes-dynamic-pvc-<UUID>: (2) No such file or directory
//
func (util *RBDUtil) rbdStatus(b *rbdMounter) (bool, string, error) {
	// If we don't have admin id/secret (e.g. attaching), fallback to user id/secret.
	var id, secret string
	if "" == b.adminId {
		id = b.Id
		secret = b.Secret
	} else {
		id = b.adminId
		secret = b.adminSecret
	}

	glog.V(4).Infof("rbd: status %s using pool %s id %s key %s", b.Image, b.Pool, id, secret)
	args := []string{"status", b.Image, "--pool", b.Pool, "--id", id, "--key=" + secret}
	out, err := b.plugin.execClusterCommand(b.Mon, "rbd", args)
	output := string(out)

	if err, ok := err.(*exec.Error); ok {
		if err.Err == exec.ErrNotFound {
			glog.Errorf("rbd cmd not found")
			// fail fast if command not found
			return false, output, err
		}
	}

	// If command never succeed, returns its last error.
	if err != nil {
		return false, output, err
	}

	if strings.Contains(output, imageWatcherStr) {
		glog.V(4).Infof("rbd: watchers on %s: %s", b.Image, output)
		return true, output, nil
	} else {
		glog.Warningf("rbd: no watchers on %s", b.Image)
		return false, output, nil
	}
}
