// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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

// spaceallocator helper used to allocate disk space.
package spaceallocator

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/utils/fs"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Allocator space allocator interface.
type Allocator interface {
	AllocateSpace(size uint64) (Space, error)
	FreeSpace(size uint64)
	AddOutdatedItem(id string, size uint64, timestamp time.Time) error
	RestoreOutdatedItem(id string)
	Close() error
}

// Space allocated space interface.
type Space interface {
	Accept() error
	Release() error
}

// ItemRemover requests to remove item in order to free space.
type ItemRemover func(id string) error

type allocatorInstance struct {
	sync.Mutex

	path            string
	partLimit       uint
	part            *partition
	remover         ItemRemover
	sizeLimit       uint64
	allocationCount uint
	allocatedSize   uint64
}

type spaceInstance struct {
	size      uint64
	path      string
	allocator *allocatorInstance
}

type partition struct {
	sync.Mutex

	mountPoint      string
	allocatorCount  uint
	allocationCount uint
	availableSize   uint64
	outdatedItems   []outdatedItem
	partLimit       uint
	totalSize       uint64
}

type outdatedItem struct {
	id        string
	size      uint64
	timestamp time.Time
	allocator *allocatorInstance
}

type byTimestamp []outdatedItem

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

//nolint:gochecknoglobals // common partitions storage
var (
	partsMutex sync.Mutex
	partsMap   map[string]*partition
)

var (
	ErrNoAllocation = errors.New("no allocation in progress")
	ErrNoSpace      = errors.New("not enough space")
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new space allocator.
func New(path string, partLimit uint, remover ItemRemover) (Allocator, error) {
	partsMutex.Lock()
	defer partsMutex.Unlock()

	mountPoint, err := fs.GetMountPoint(path)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"path": path, "partLimit": partLimit, "mountPoint": mountPoint}).Debug("Create allocator")

	part, ok := partsMap[mountPoint]
	if !ok {
		if partsMap == nil {
			partsMap = make(map[string]*partition)
		}

		if part, err = newPart(mountPoint); err != nil {
			return nil, err
		}

		partsMap[mountPoint] = part
	}

	if err := part.addPartLimit(partLimit); err != nil {
		return nil, err
	}

	part.allocatorCount++

	allocator := &allocatorInstance{
		path:      path,
		partLimit: partLimit,
		part:      part,
		remover:   remover,
	}

	if partLimit != 0 {
		allocator.sizeLimit = part.totalSize * uint64(partLimit) / 100
	}

	return allocator, nil
}

// Close closes space allocator.
func (allocator *allocatorInstance) Close() (err error) {
	partsMutex.Lock()
	defer partsMutex.Unlock()

	log.WithFields(log.Fields{"path": allocator.path}).Debug("Close allocator")

	if partLimitErr := allocator.part.removePartLimit(allocator.partLimit); partLimitErr != nil {
		err = partLimitErr
	}

	allocator.part.allocatorCount--

	if allocator.part.allocatorCount == 0 {
		log.WithFields(log.Fields{"mountPoint": allocator.part.mountPoint}).Debug("Close space allocation partition")

		delete(partsMap, allocator.part.mountPoint)
	}

	return err
}

// AllocateSpace allocates space in storage.
func (allocator *allocatorInstance) AllocateSpace(size uint64) (Space, error) {
	log.WithFields(log.Fields{"path": allocator.path, "size": size}).Debug("Allocate space")

	if err := allocator.allocateSpace(size); err != nil {
		return nil, err
	}

	if err := allocator.part.allocateSpace(size); err != nil {
		return nil, err
	}

	return &spaceInstance{size: size, path: allocator.path, allocator: allocator}, nil
}

// Accept accepts previously allocated space.
func (space *spaceInstance) Accept() error {
	log.WithFields(log.Fields{"path": space.path, "size": space.size}).Debug("Space accepted")

	if err := space.allocator.allocateDone(); err != nil {
		return err
	}

	if err := space.allocator.part.allocateDone(); err != nil {
		return err
	}

	return nil
}

// Release releases previously allocated space.
func (space *spaceInstance) Release() error {
	log.WithFields(log.Fields{"path": space.path, "size": space.size}).Debug("Space released")

	space.allocator.freeSpace(space.size)
	space.allocator.part.freeSpace(space.size)

	if err := space.allocator.allocateDone(); err != nil {
		return err
	}

	if err := space.allocator.part.allocateDone(); err != nil {
		return err
	}

	return nil
}

// FreeSpace frees space in storage.
// This function should be called when storage item is removed by owner.
func (allocator *allocatorInstance) FreeSpace(size uint64) {
	log.WithFields(log.Fields{"path": allocator.path, "size": size}).Debug("Free space")

	allocator.freeSpace(size)
	allocator.part.freeSpace(size)
}

// AddOutdatedItem adds outdated item.
// If there is no space to allocate, spaceallocator will try to free some space by calling ItemRemover function for
// outdated items. Item owner should remove this item. ItemRemover function is called based on item timestamp:
// oldest item should be removed first. After calling ItemRemover the item is automatically removed from outdated
// item list.
func (allocator *allocatorInstance) AddOutdatedItem(id string, size uint64, timestamp time.Time) error {
	log.WithFields(log.Fields{
		"path": allocator.path, "id": id, "size": size, "timestamp": timestamp,
	}).Debug("Add outdated item")

	if allocator.remover == nil {
		return aoserrors.New("no item remover")
	}

	allocator.part.addOutdatedItem(outdatedItem{id: id, size: size, allocator: allocator, timestamp: timestamp})

	return nil
}

// RestoreOutdatedItem removes item from outdated item list.
func (allocator *allocatorInstance) RestoreOutdatedItem(id string) {
	log.WithFields(log.Fields{"path": allocator.path, "id": id}).Debug("Restore outdated item")

	allocator.part.restoreOutdatedItem(id)
}

/***********************************************************************************************************************
 * byTimestamp
 **********************************************************************************************************************/

func (items byTimestamp) Len() int           { return len(items) }
func (items byTimestamp) Less(i, j int) bool { return items[i].timestamp.Before(items[j].timestamp) }
func (items byTimestamp) Swap(i, j int)      { items[i], items[j] = items[j], items[i] }

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (allocator *allocatorInstance) allocateSpace(size uint64) error {
	if allocator.sizeLimit == 0 {
		return nil
	}

	allocator.Lock()
	defer allocator.Unlock()

	if allocator.allocationCount == 0 {
		allocatedSize, err := fs.GetDirSize(allocator.path)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		allocator.allocatedSize = uint64(allocatedSize)

		log.WithFields(log.Fields{
			"path": allocator.path, "size": allocator.allocatedSize,
		}).Debug("Initial allocated space")
	}

	if allocator.sizeLimit > 0 && allocator.allocatedSize+size > allocator.sizeLimit {
		freedSize, err := allocator.removeOutdatedItems(allocator.allocatedSize + size - allocator.sizeLimit)
		if err != nil {
			return err
		}

		if freedSize > allocator.allocatedSize {
			allocator.allocatedSize = 0
		} else {
			allocator.allocatedSize -= freedSize
		}
	}

	allocator.allocatedSize += size
	allocator.allocationCount++

	log.WithFields(log.Fields{
		"path": allocator.path, "size": allocator.allocatedSize,
	}).Debug("Total allocated space")

	return nil
}

func (allocator *allocatorInstance) freeSpace(size uint64) {
	if allocator.sizeLimit == 0 {
		return
	}

	allocator.Lock()
	defer allocator.Unlock()

	if allocator.allocationCount > 0 {
		if size < allocator.allocatedSize {
			allocator.allocatedSize -= size
		} else {
			allocator.allocatedSize = 0
		}

		log.WithFields(log.Fields{
			"path": allocator.path, "size": allocator.allocatedSize,
		}).Debug("Total allocated space")
	}
}

func (allocator *allocatorInstance) allocateDone() error {
	if allocator.sizeLimit == 0 {
		return nil
	}

	allocator.Lock()
	defer allocator.Unlock()

	if allocator.allocationCount == 0 {
		return ErrNoAllocation
	}

	allocator.allocationCount--

	return nil
}

func (allocator *allocatorInstance) removeOutdatedItems(requiredSize uint64) (freedSize uint64, err error) {
	var totalSize uint64

	for _, item := range allocator.part.outdatedItems {
		if item.allocator != allocator {
			continue
		}

		totalSize += item.size
	}

	if requiredSize > totalSize {
		return 0, ErrNoSpace
	}

	log.WithFields(log.Fields{
		"mountPoint": allocator.part.mountPoint, "requiredSize": requiredSize,
	}).Debug("Remove outdated items")

	sort.Sort(byTimestamp(allocator.part.outdatedItems))

	i := 0

	for _, item := range allocator.part.outdatedItems {
		if item.allocator != allocator || freedSize >= requiredSize {
			allocator.part.outdatedItems[i] = item
			i++

			continue
		}

		log.WithFields(log.Fields{
			"mountPoint": allocator.part.mountPoint, "id": item.id, "size": item.size,
		}).Debug("Remove outdated item")

		if err := item.allocator.remover(item.id); err != nil {
			return freedSize, err
		}

		item.allocator.part.freeSpace(item.size)

		freedSize += item.size
	}

	allocator.part.outdatedItems = allocator.part.outdatedItems[:i]

	return freedSize, nil
}

func newPart(mountPoint string) (*partition, error) {
	totalSize, err := fs.GetTotalSize(mountPoint)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{
		"mountPoint": mountPoint, "totalSize": totalSize,
	}).Debug("Create space allocation partition")

	return &partition{mountPoint: mountPoint, totalSize: uint64(totalSize)}, nil
}

func (part *partition) allocateSpace(size uint64) error {
	part.Lock()
	defer part.Unlock()

	if part.allocationCount == 0 {
		availableSize, err := fs.GetAvailableSize(part.mountPoint)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		part.availableSize = uint64(availableSize)

		log.WithFields(log.Fields{
			"mountPoint": part.mountPoint, "size": part.availableSize,
		}).Debug("Initial partition space")
	}

	if size > part.availableSize {
		freedSize, err := part.removeOutdatedItems(size - part.availableSize)
		if err != nil {
			return err
		}

		part.availableSize += freedSize
	}

	part.availableSize -= size
	part.allocationCount++

	log.WithFields(log.Fields{
		"mountPoint": part.mountPoint, "size": part.availableSize,
	}).Debug("Available partition space")

	return nil
}

func (part *partition) freeSpace(size uint64) {
	part.Lock()
	defer part.Unlock()

	if part.allocationCount > 0 {
		part.availableSize += size

		log.WithFields(log.Fields{
			"mountPoint": part.mountPoint, "size": part.availableSize,
		}).Debug("Available partition space")
	}
}

func (part *partition) allocateDone() error {
	part.Lock()
	defer part.Unlock()

	if part.allocationCount == 0 {
		return ErrNoAllocation
	}

	part.allocationCount--

	return nil
}

func (part *partition) addOutdatedItem(item outdatedItem) {
	part.Lock()
	defer part.Unlock()

	i := 0

	for ; i < len(part.outdatedItems); i++ {
		if part.outdatedItems[i].id == item.id {
			part.outdatedItems[i] = item

			break
		}
	}

	if i == len(part.outdatedItems) {
		part.outdatedItems = append(part.outdatedItems, item)
	}
}

func (part *partition) restoreOutdatedItem(id string) {
	part.Lock()
	defer part.Unlock()

	for i, item := range part.outdatedItems {
		if item.id == id {
			part.outdatedItems = append(part.outdatedItems[:i], part.outdatedItems[i+1:]...)

			break
		}
	}
}

func (part *partition) removeOutdatedItems(requiredSize uint64) (freedSize uint64, err error) {
	var totalSize uint64

	for _, item := range part.outdatedItems {
		totalSize += item.size
	}

	if requiredSize > totalSize {
		return 0, ErrNoSpace
	}

	log.WithFields(log.Fields{
		"mountPoint": part.mountPoint, "requiredSize": requiredSize,
	}).Debug("Remove outdated items")

	sort.Sort(byTimestamp(part.outdatedItems))

	i := 0

	for ; freedSize < requiredSize; i++ {
		item := part.outdatedItems[i]

		log.WithFields(log.Fields{
			"mountPoint": part.mountPoint, "id": item.id, "size": item.size,
		}).Debug("Remove outdated item")

		if err := item.allocator.remover(item.id); err != nil {
			return freedSize, err
		}

		item.allocator.freeSpace(item.size)

		freedSize += item.size
	}

	part.outdatedItems = part.outdatedItems[i:]

	return freedSize, nil
}

func (part *partition) addPartLimit(partLimit uint) error {
	if part.partLimit+partLimit > 100 {
		return aoserrors.New("total part limit exceeds")
	}

	part.partLimit += partLimit

	return nil
}

func (part *partition) removePartLimit(partLimit uint) error {
	if part.partLimit < partLimit {
		return aoserrors.New("can't remove part limit")
	}

	part.partLimit -= partLimit

	return nil
}
