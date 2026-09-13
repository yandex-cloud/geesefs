// Copyright 2023 Yandex LLC
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

package core

import (
	"fmt"
	"io/fs"

	"github.com/yandex-cloud/geesefs/core/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// logging

func (inode *Inode) info() string {
	inode.mu.Lock()
	defer inode.mu.Unlock()
	return inode.infoUnlocked()
}

func (inode *Inode) infoUnlocked() string {
	return fmt.Sprintf("Inode{Id=%v,Name=%v,Owner=%v,%v}", inode.Id, inode.FullName(), inode.ownerTerm, inode.owner)
}

// inode locks

func (inode *Inode) KeepOwnerLock() {
	inode.ownerMu.RLock()
}

func (inode *Inode) KeepOwnerUnlock() {
	inode.ownerMu.RUnlock()
}

func (inode *Inode) StateLock() {
	inode.ownerMu.RLock()
	inode.mu.Lock()
}

func (inode *Inode) StateUnlock() {
	inode.mu.Unlock()
	inode.ownerMu.RUnlock()
}

// Only for inode in KeepOwnerLock
func (inode *Inode) UpgradeToStateLock() {
	inode.mu.Lock()
}

// Only for inode in StateLock
func (inode *Inode) DowngradeToKeepOwnerLock() {
	inode.mu.Unlock()
}

func (inode *Inode) ChangeOwnerLock() {
	inode.ownerMu.Lock()
}

func (inode *Inode) ChangeOwnerUnlock() {
	inode.ownerMu.Unlock()
}

// inode utils

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (inode *Inode) pbInode() *pb.Inode {
	return &pb.Inode{
		Id:      uint64(inode.Id),
		Name:    inode.Name,
		Dir:     inode.isDir(),
		Symlink: inode.Attributes.Mode&fs.ModeSymlink != 0,
		Owner:   inode.pbOwner(),
	}
}

// REQUIRED_LOCK(inode.StateLock)
func (inode *Inode) pbAttr() *pb.Attributes {
	attr := inode.InflateAttributes()
	pbAttr := &pb.Attributes{
		Size:  attr.Size,
		Mtime: timestamppb.New(attr.Mtime),
		Ctime: timestamppb.New(attr.Ctime),
		Mode:  uint32(attr.Mode),
	}
	return pbAttr
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (inode *Inode) pbOwner() *pb.Owner {
	return &pb.Owner{
		Term:   inode.ownerTerm,
		NodeId: uint64(inode.owner),
	}
}

// REQUIRED_LOCK(inode.ChangeOwnerLock)
func (inode *Inode) applyOwner(owner *pb.Owner) {
	if inode.ownerTerm < owner.Term {
		inode.ownerTerm = owner.Term
		inode.owner = NodeId(owner.NodeId)
	}
}
