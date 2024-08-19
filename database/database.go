// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2021 Renesas Electronics Corporation.
// Copyright (C) 2021 EPAM Systems, Inc.
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

package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/migration"
	_ "github.com/mattn/go-sqlite3" // ignore lint
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_common/utils/semverutils"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/downloader"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/launcher"
	"github.com/aosedge/aos_communicationmanager/networkmanager"
	"github.com/aosedge/aos_communicationmanager/storagestate"
	"github.com/aosedge/aos_communicationmanager/umcontroller"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	busyTimeout = 60000
	journalMode = "WAL"
	syncMode    = "NORMAL"
)

const dbVersion = 2

const dbFileName = "communicationmanager.db"

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var errNotExist = errors.New("entry does not exist")

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Database structure with database information.
type Database struct {
	sql *sql.DB
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new database handle.
func New(config *config.Config) (db *Database, err error) {
	fileName := filepath.Join(config.WorkingDir, dbFileName)

	log.WithField("fileName", fileName).Debug("Open database")

	if err = os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
		return db, aoserrors.Wrap(err)
	}

	if err = migration.MergeMigrationFiles(config.Migration.MigrationPath,
		config.Migration.MergedMigrationPath); err != nil {
		return db, aoserrors.Wrap(err)
	}

	sqlite, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=%d&_journal_mode=%s&_sync=%s",
		fileName, busyTimeout, journalMode, syncMode))
	if err != nil {
		return db, aoserrors.Wrap(err)
	}

	db = &Database{sqlite}

	defer func() {
		if err != nil {
			db.Close()
		}
	}()

	exists, err := db.isTableExist("config")
	if err != nil {
		return db, aoserrors.Wrap(err)
	}

	if !exists {
		// Set database version if database not exist
		if err = migration.SetDatabaseVersion(sqlite, config.Migration.MigrationPath, dbVersion); err != nil {
			return db, aoserrors.Wrap(err)
		}

		if err := db.createConfigTable(); err != nil {
			return db, aoserrors.Wrap(err)
		}
	} else {
		if err = migration.DoMigrate(db.sql, config.Migration.MergedMigrationPath, dbVersion); err != nil {
			return db, aoserrors.Wrap(err)
		}
	}

	if err := db.createDownloadTable(); err != nil {
		return db, aoserrors.Wrap(err)
	}

	if err := db.createStorageStateTable(); err != nil {
		return db, err
	}

	if err := db.createNetworkTable(); err != nil {
		return db, err
	}

	if err := db.createServiceTable(); err != nil {
		return db, aoserrors.Wrap(err)
	}

	if err := db.createLayersTable(); err != nil {
		return db, aoserrors.Wrap(err)
	}

	if err := db.createInstancesTable(); err != nil {
		return db, err
	}

	if err := db.createNodeStateTable(); err != nil {
		return db, err
	}

	return db, nil
}

// SetJournalCursor stores system logger cursor.
func (db *Database) SetJournalCursor(cursor string) (err error) {
	if err = db.executeQuery(`UPDATE config SET cursor = ?`, cursor); err != nil {
		return err
	}

	return nil
}

// GetJournalCursor retrieves logger cursor.
func (db *Database) GetJournalCursor() (cursor string, err error) {
	if err = db.getDataFromQuery(
		"SELECT cursor FROM config",
		[]any{}, &cursor); err != nil {
		if errors.Is(err, errNotExist) {
			return cursor, downloader.ErrNotExist
		}
	}

	return cursor, err
}

// SetComponentsUpdateInfo store update data for update managers.
func (db *Database) SetComponentsUpdateInfo(updateInfo []umcontroller.ComponentStatus) (err error) {
	dataJSON, err := json.Marshal(&updateInfo)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = db.executeQuery(`UPDATE config SET componentsUpdateInfo = ?`, dataJSON); err != nil {
		return err
	}

	return nil
}

// GetComponentsUpdateInfo returns update data for system components.
func (db *Database) GetComponentsUpdateInfo() (updateInfo []umcontroller.ComponentStatus, err error) {
	stmt, err := db.sql.Prepare("SELECT componentsUpdateInfo FROM config")
	if err != nil {
		return updateInfo, aoserrors.Wrap(err)
	}
	defer stmt.Close()

	var dataJSON []byte

	if err = stmt.QueryRow().Scan(&dataJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return updateInfo, errNotExist
		}

		return updateInfo, aoserrors.Wrap(err)
	}

	if dataJSON == nil {
		return updateInfo, nil
	}

	if len(dataJSON) == 0 {
		return updateInfo, nil
	}

	if err = json.Unmarshal(dataJSON, &updateInfo); err != nil {
		return updateInfo, aoserrors.Wrap(err)
	}

	return updateInfo, nil
}

// SetFirmwareUpdateState sets FOTA update state.
func (db *Database) SetFirmwareUpdateState(state json.RawMessage) (err error) {
	if err = db.executeQuery(`UPDATE config SET fotaUpdateState = ?`, state); err != nil {
		return err
	}

	return nil
}

// GetFirmwareUpdateState returns FOTA update state.
func (db *Database) GetFirmwareUpdateState() (state json.RawMessage, err error) {
	if err = db.getDataFromQuery(
		"SELECT fotaUpdateState FROM config",
		[]any{}, &state); err != nil {
		if errors.Is(err, errNotExist) {
			return state, downloader.ErrNotExist
		}
	}

	return state, err
}

// SetSoftwareUpdateState sets SOTA update state.
func (db *Database) SetSoftwareUpdateState(state json.RawMessage) (err error) {
	if err = db.executeQuery(`UPDATE config SET sotaUpdateState = ?`, state); err != nil {
		return err
	}

	return nil
}

// GetSoftwareUpdateState returns SOTA update state.
func (db *Database) GetSoftwareUpdateState() (state json.RawMessage, err error) {
	if err = db.getDataFromQuery(
		"SELECT sotaUpdateState FROM config",
		[]any{}, &state); err != nil {
		if errors.Is(err, errNotExist) {
			return state, downloader.ErrNotExist
		}
	}

	return state, err
}

// GetDownloadInfo returns download info by file path.
func (db *Database) GetDownloadInfo(filePath string) (downloadInfo downloader.DownloadInfo, err error) {
	if err = db.getDataFromQuery(
		"SELECT * FROM download WHERE path = ?",
		[]any{filePath}, &downloadInfo.Path, &downloadInfo.TargetType,
		&downloadInfo.InterruptReason, &downloadInfo.Downloaded); err != nil {
		if errors.Is(err, errNotExist) {
			return downloadInfo, downloader.ErrNotExist
		}
	}

	return downloadInfo, err
}

// GetDownloadInfos returns all download info.
func (db *Database) GetDownloadInfos() (downloadInfos []downloader.DownloadInfo, err error) {
	rows, err := db.sql.Query("SELECT * FROM download")
	if err != nil {
		return downloadInfos, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	for rows.Next() {
		var downloadInfo downloader.DownloadInfo

		if err = rows.Scan(
			&downloadInfo.Path, &downloadInfo.TargetType,
			&downloadInfo.InterruptReason, &downloadInfo.Downloaded); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		downloadInfos = append(downloadInfos, downloadInfo)
	}

	return downloadInfos, nil
}

// RemoveDownloadInfo removes download info by file path.
func (db *Database) RemoveDownloadInfo(filePath string) (err error) {
	if err = db.executeQuery("DELETE FROM download WHERE path = ?", filePath); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

// SetDownloadInfo stores download info.
func (db *Database) SetDownloadInfo(downloadInfo downloader.DownloadInfo) (err error) {
	var path string

	if err = db.getDataFromQuery(
		"SELECT * FROM download WHERE path = ?",
		[]any{downloadInfo.Path}, &path); err != nil && !errors.Is(err, errNotExist) {
		return err
	}

	if !errors.Is(err, errNotExist) {
		if err = db.executeQuery(`UPDATE download SET targetType = ?,
		    interruptReason = ?, downloaded = ? WHERE path = ?`, downloadInfo.Path,
			downloadInfo.TargetType, downloadInfo.InterruptReason, downloadInfo.Downloaded); err != nil {
			return err
		}

		return nil
	}

	if err = db.executeQuery("INSERT INTO download values(?, ?, ?, ?)", downloadInfo.Path,
		downloadInfo.TargetType, downloadInfo.InterruptReason, downloadInfo.Downloaded); err != nil {
		return err
	}

	return nil
}

// GetServicesInfo returns services info.
func (db *Database) GetServicesInfo() ([]imagemanager.ServiceInfo, error) {
	allServiceVersions, err := db.getServicesFromQuery(`SELECT * FROM services`)
	if err != nil {
		return nil, err
	}

	maxVersionServices := make(map[string]imagemanager.ServiceInfo)

	for _, service := range allServiceVersions {
		if storedService, found := maxVersionServices[service.ServiceID]; found {
			greater, err := semverutils.GreaterThan(service.Version, storedService.Version)
			if err != nil {
				return nil, aoserrors.Wrap(err)
			}

			if greater {
				maxVersionServices[service.ServiceID] = service
			}
		} else {
			maxVersionServices[service.ServiceID] = service
		}
	}

	result := make([]imagemanager.ServiceInfo, 0)
	for _, value := range maxVersionServices {
		result = append(result, value)
	}

	return result, nil
}

// GetServiceInfo returns service info by ID.
func (db *Database) GetServiceInfo(serviceID string) (service imagemanager.ServiceInfo, err error) {
	maxVersion, err := db.getMaxServiceVersion(serviceID)
	if err != nil {
		return service, err
	}

	services, err := db.getServicesFromQuery(
		"SELECT * FROM services WHERE id = ? and version = ?",
		serviceID, maxVersion)
	if err != nil {
		return service, err
	}

	if len(services) == 0 {
		return imagemanager.ServiceInfo{}, imagemanager.ErrNotExist
	}

	if len(services) > 1 {
		return imagemanager.ServiceInfo{}, aoserrors.New("wrong number of services returned")
	}

	return services[0], nil
}

// AddService adds new service.
func (db *Database) AddService(service imagemanager.ServiceInfo) error {
	configJSON, err := json.Marshal(&service.Config)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	layers, err := json.Marshal(&service.Layers)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	exposedPorts, err := json.Marshal(&service.ExposedPorts)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return db.executeQuery("INSERT INTO services values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		service.ServiceID, service.Version, service.ProviderID, service.URL, service.RemoteURL,
		service.Path, service.Size, service.Timestamp, service.Cached,
		configJSON, layers, service.Sha256, exposedPorts, service.GID)
}

// SetServiceCached sets cached status for the service.
func (db *Database) SetServiceCached(serviceID string, cached bool) (err error) {
	if err = db.executeQuery("UPDATE services SET cached = ? WHERE id = ?",
		cached, serviceID); errors.Is(err, errNotExist) {
		return imagemanager.ErrNotExist
	}

	return err
}

// RemoveService removes existing service.
func (db *Database) RemoveService(serviceID string, version string) (err error) {
	if err = db.executeQuery("DELETE FROM services WHERE id = ? AND version = ?",
		serviceID, version); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

// GetAllServiceVersions returns all service versions.
func (db *Database) GetServiceVersions(serviceID string) (services []imagemanager.ServiceInfo, err error) {
	if services, err = db.getServicesFromQuery(
		"SELECT * FROM services WHERE id = ?", serviceID); err != nil {
		return nil, err
	}

	if len(services) == 0 {
		return nil, imagemanager.ErrNotExist
	}

	sort.SliceStable(services, func(left, right int) bool {
		res, _ := semverutils.LessThan(services[left].Version, services[right].Version)

		return res
	})

	return services, nil
}

// GetLayersInfo returns layers info.
func (db *Database) GetLayersInfo() ([]imagemanager.LayerInfo, error) {
	return db.getLayersFromQuery("SELECT * FROM layers")
}

// GetLayerInfo returns layer info by ID.
func (db *Database) GetLayerInfo(digest string) (layer imagemanager.LayerInfo, err error) {
	if err = db.getDataFromQuery("SELECT * FROM layers WHERE digest = ?",
		[]any{digest}, &layer.Digest, &layer.LayerID, &layer.Version,
		&layer.URL, &layer.RemoteURL, &layer.Path, &layer.Size, &layer.Timestamp, &layer.Sha256,
		&layer.Cached); err != nil {
		if errors.Is(err, errNotExist) {
			return layer, imagemanager.ErrNotExist
		}

		return layer, err
	}

	return layer, nil
}

// AddLayer adds new layer.
func (db *Database) AddLayer(layer imagemanager.LayerInfo) error {
	return db.executeQuery("INSERT INTO layers values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		layer.Digest, layer.LayerID, layer.Version, layer.URL, layer.RemoteURL,
		layer.Path, layer.Size, layer.Timestamp, layer.Sha256, layer.Cached)
}

// SetLayerCached sets cached status for the layer.
func (db *Database) SetLayerCached(digest string, cached bool) (err error) {
	if err = db.executeQuery("UPDATE layers SET cached = ? WHERE digest = ?",
		cached, digest); errors.Is(err, errNotExist) {
		return imagemanager.ErrNotExist
	}

	return err
}

// RemoveLayer removes existing layer.
func (db *Database) RemoveLayer(digest string) (err error) {
	if err = db.executeQuery("DELETE FROM layers WHERE digest = ?", digest); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

// AddInstance adds instanace info.
func (db *Database) AddInstance(instanceInfo launcher.InstanceInfo) error {
	return db.executeQuery("INSERT INTO instances values(?, ?, ?, ?, ?, ?, ?, ?)",
		instanceInfo.ServiceID, instanceInfo.SubjectID, instanceInfo.Instance, instanceInfo.NodeID,
		instanceInfo.PrevNodeID, instanceInfo.UID, instanceInfo.Timestamp, instanceInfo.Cached)
}

// UpdateInstance updates instance info.
func (db *Database) UpdateInstance(instanceInfo launcher.InstanceInfo) error {
	if err := db.executeQuery("UPDATE instances SET "+
		"nodeID = ? prevNodeID = ? uid = ? timestamp = ? cached = ? "+
		"WHERE serviceId = ? AND subjectId = ? AND  instance = ?",
		instanceInfo.NodeID, instanceInfo.PrevNodeID, instanceInfo.UID, instanceInfo.Timestamp, instanceInfo.Cached,
		instanceInfo.ServiceID, instanceInfo.SubjectID, instanceInfo.Instance); err != nil {
		if errors.Is(err, errNotExist) {
			return launcher.ErrNotExist
		}
	}

	return nil
}

// GetInstance returns instance by instance ident.
func (db *Database) GetInstance(instance aostypes.InstanceIdent) (launcher.InstanceInfo, error) {
	instanceInfo := launcher.InstanceInfo{
		InstanceIdent: instance,
	}

	if err := db.getDataFromQuery("SELECT nodeID, prevNodeID, uid, timestamp, cached "+
		"FROM instances WHERE serviceId = ? AND subjectId = ? AND instance = ?",
		[]any{instance.ServiceID, instance.SubjectID, instance.Instance},
		&instanceInfo.NodeID, &instanceInfo.PrevNodeID, &instanceInfo.UID,
		&instanceInfo.Timestamp, &instanceInfo.Cached); err != nil {
		if errors.Is(err, errNotExist) {
			return instanceInfo, launcher.ErrNotExist
		}

		return instanceInfo, err
	}

	return instanceInfo, nil
}

// GetInstances gets all instances.
func (db *Database) GetInstances() ([]launcher.InstanceInfo, error) {
	rows, err := db.sql.Query("SELECT * FROM instances")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	var instances []launcher.InstanceInfo

	for rows.Next() {
		var instance launcher.InstanceInfo

		if err = rows.Scan(&instance.ServiceID, &instance.SubjectID, &instance.Instance, &instance.NodeID,
			&instance.PrevNodeID, &instance.UID, &instance.Timestamp, &instance.Cached); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

// RemoveInstance removes existing instance.
func (db *Database) RemoveInstance(instance aostypes.InstanceIdent) error {
	return db.executeQuery("DELETE FROM instances WHERE serviceId = ? AND subjectId = ? AND  instance = ?",
		instance.ServiceID, instance.SubjectID, instance.Instance)
}

// GetStorageStateInfo returns storage and state info by instance ident.
func (db *Database) GetStorageStateInfo(
	instanceIdent aostypes.InstanceIdent,
) (info storagestate.StorageStateInstanceInfo, err error) {
	if err = db.getDataFromQuery(
		"SELECT * FROM storagestate WHERE serviceID = ? AND subjectID = ? AND instance = ?",
		[]any{instanceIdent.ServiceID, instanceIdent.SubjectID, instanceIdent.Instance},
		&info.InstanceID, &info.ServiceID, &info.SubjectID, &info.Instance, &info.StorageQuota,
		&info.StateQuota, &info.StateChecksum); err != nil {
		if errors.Is(err, errNotExist) {
			return info, storagestate.ErrNotExist
		}

		return info, err
	}

	return info, nil
}

// GetStorageStateInfo returns storage and state infos.
func (db *Database) GetAllStorageStateInfo() (infos []storagestate.StorageStateInstanceInfo, err error) {
	rows, err := db.sql.Query("SELECT * FROM storagestate")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	for rows.Next() {
		var storageStateInfo storagestate.StorageStateInstanceInfo

		if err = rows.Scan(
			&storageStateInfo.InstanceID, &storageStateInfo.ServiceID, &storageStateInfo.SubjectID,
			&storageStateInfo.Instance, &storageStateInfo.StorageQuota, &storageStateInfo.StateQuota,
			&storageStateInfo.StateChecksum); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		infos = append(infos, storageStateInfo)
	}

	return infos, nil
}

// SetStorageStateQuotas sets state storage quota info by instance ident.
func (db *Database) SetStorageStateQuotas(
	instanceIdent aostypes.InstanceIdent, storageQuota, stateQuota uint64,
) (err error) {
	if err = db.executeQuery(`UPDATE storagestate SET storageQuota = ?, stateQuota =?
	    WHERE serviceID = ? AND subjectID = ? AND instance = ?`,
		storageQuota, stateQuota, instanceIdent.ServiceID,
		instanceIdent.SubjectID, instanceIdent.Instance); errors.Is(err, errNotExist) {
		return aoserrors.Wrap(storagestate.ErrNotExist)
	}

	return err
}

// AddStorageStateInfo adds storage and state info.
func (db *Database) AddStorageStateInfo(info storagestate.StorageStateInstanceInfo) error {
	return db.executeQuery("INSERT INTO storagestate values(?, ?, ?, ?, ?, ?, ?)",
		info.InstanceID, info.ServiceID, info.SubjectID, info.Instance, info.StorageQuota,
		info.StateQuota, info.StateChecksum)
}

// SetStateChecksum updates state checksum by instance ident.
func (db *Database) SetStateChecksum(instanceIdent aostypes.InstanceIdent, checksum []byte) (err error) {
	if err = db.executeQuery(`UPDATE storagestate SET stateChecksum = ?
	    WHERE serviceID = ? AND subjectID = ? AND instance = ?`,
		checksum, instanceIdent.ServiceID, instanceIdent.SubjectID,
		instanceIdent.Instance); errors.Is(err, errNotExist) {
		return aoserrors.Wrap(storagestate.ErrNotExist)
	}

	return err
}

// RemoveStorageStateInfo removes storage and state info by instance ident.
func (db *Database) RemoveStorageStateInfo(instanceIdent aostypes.InstanceIdent) (err error) {
	if err = db.executeQuery(
		"DELETE FROM storagestate WHERE serviceID = ? AND subjectID = ? AND instance = ?",
		instanceIdent.ServiceID, instanceIdent.SubjectID,
		instanceIdent.Instance); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

func (db *Database) AddNetworkInfo(networkInfo aostypes.NetworkParameters) error {
	return db.executeQuery("INSERT INTO network values(?, ?, ?, ?)",
		networkInfo.NetworkID, networkInfo.IP, networkInfo.Subnet, networkInfo.VlanID)
}

func (db *Database) RemoveNetworkInfo(networkID string) (err error) {
	if err = db.executeQuery("DELETE FROM network WHERE networkID = ?", networkID); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

func (db *Database) GetNetworksInfo() ([]aostypes.NetworkParameters, error) {
	rows, err := db.sql.Query("SELECT * FROM network")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	var networks []aostypes.NetworkParameters

	for rows.Next() {
		var networkInfo aostypes.NetworkParameters

		if err = rows.Scan(&networkInfo.NetworkID, &networkInfo.IP, &networkInfo.Subnet, &networkInfo.VlanID); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		networks = append(networks, networkInfo)
	}

	return networks, nil
}

// AddNetworkInstanceInfo adds network instance info.
func (db *Database) AddNetworkInstanceInfo(networkInfo networkmanager.InstanceNetworkInfo) error {
	ports, err := json.Marshal(&networkInfo.Rules)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return db.executeQuery("INSERT INTO instance_network values(?, ?, ?, ?, ?, ?, ?, ?)",
		networkInfo.ServiceID, networkInfo.SubjectID, networkInfo.Instance, networkInfo.NetworkID,
		networkInfo.IP, networkInfo.Subnet, networkInfo.VlanID, ports)
}

// RemoveNetworkInstanceInfo removes network instance info.
func (db *Database) RemoveNetworkInstanceInfo(instanceIdent aostypes.InstanceIdent) (err error) {
	if err = db.executeQuery(
		"DELETE FROM instance_network WHERE serviceID = ? AND subjectID = ? AND instance = ?",
		instanceIdent.ServiceID, instanceIdent.SubjectID,
		instanceIdent.Instance); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

// GetNetworkInstancesInfo returns network instances info.
func (db *Database) GetNetworkInstancesInfo() (networkInfos []networkmanager.InstanceNetworkInfo, err error) {
	rows, err := db.sql.Query("SELECT * FROM instance_network")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	for rows.Next() {
		var networkInfo networkmanager.InstanceNetworkInfo

		var ports []byte

		if err = rows.Scan(&networkInfo.ServiceID, &networkInfo.SubjectID, &networkInfo.Instance,
			&networkInfo.NetworkID, &networkInfo.IP, &networkInfo.Subnet,
			&networkInfo.VlanID, &ports); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		if err = json.Unmarshal(ports, &networkInfo.Rules); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		networkInfos = append(networkInfos, networkInfo)
	}

	return networkInfos, nil
}

// Close closes database.
func (db *Database) Close() {
	db.sql.Close()
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (db *Database) getDataFromQuery(query string, queryParams []interface{}, result ...interface{}) error {
	stmt, err := db.sql.Prepare(query)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer stmt.Close()

	if err = stmt.QueryRow(queryParams...).Scan(result...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errNotExist
		}

		return aoserrors.Wrap(err)
	}

	return nil
}

func (db *Database) executeQuery(query string, args ...interface{}) error {
	stmt, err := db.sql.Prepare(query)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer stmt.Close()

	result, err := stmt.Exec(args...)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if count == 0 {
		return aoserrors.Wrap(errNotExist)
	}

	return nil
}

func (db *Database) createDownloadTable() (err error) {
	log.Info("Create download table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS download (path TEXT NOT NULL PRIMARY KEY,
                                                               targetType TEXT NOT NULL,
                                                               interruptReason TEXT,
                                                               downloaded INTEGER)`)

	return aoserrors.Wrap(err)
}

func (db *Database) createServiceTable() (err error) {
	log.Info("Create service table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS services (id TEXT NOT NULL ,
                                                               version TEXT NOT NULL,
                                                               providerId TEXT,
                                                               localURL TEXT,
                                                               remoteURL TEXT,
                                                               path TEXT,
                                                               size INTEGER,
                                                               timestamp TIMESTAMP,
                                                               cached INTEGER,
                                                               config BLOB,
                                                               layers BLOB,
                                                               sha256 BLOB,
                                                               exposedPorts BLOB,
                                                               gid INTEGER,
                                                               PRIMARY KEY(id, version))`)

	return aoserrors.Wrap(err)
}

func (db *Database) createLayersTable() (err error) {
	log.Info("Create layers table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS layers (digest TEXT NOT NULL PRIMARY KEY,
                                                             layerId TEXT,
                                                             version TEXT,
                                                             localURL   TEXT,
                                                             remoteURL  TEXT,
                                                             Path       TEXT,
                                                             Size       INTEGER,
                                                             Timestamp  TIMESTAMP,
                                                             sha256 BLOB,
                                                             cached INTEGER)`)

	return aoserrors.Wrap(err)
}

func (db *Database) createInstancesTable() (err error) {
	log.Info("Create instances table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS instances (serviceId TEXT,
                                                                subjectId TEXT,
                                                                instance INTEGER,
                                                                nodeID TEXT,
                                                                prevNodeID TEXT,
                                                                uid integer,
                                                                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                cached INTEGER DEFAULT 0,
                                                                PRIMARY KEY(serviceId, subjectId, instance))`)

	return aoserrors.Wrap(err)
}

func (db *Database) createNetworkTable() (err error) {
	log.Info("Create network table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS instance_network (serviceId TEXT,
                                                              subjectId TEXT,
                                                              instance INTEGER,
                                                              networkID TEXT,
                                                              ip TEXT,
                                                              subnet TEXT,
                                                              vlanID INTEGER,
                                                              port BLOB,
                                                              PRIMARY KEY(serviceId, subjectId, instance))`)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS network (networkID TEXT NOT NULL PRIMARY KEY,
                                                              ip TEXT,
                                                              subnet TEXT,
                                                              vlanID INTEGER)`)

	return aoserrors.Wrap(err)
}

func (db *Database) createStorageStateTable() (err error) {
	log.Info("Create storagestate table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS storagestate (instanceID TEXT,
                                                                   serviceID TEXT,
                                                                   subjectID TEXT,
                                                                   instance INTEGER,
                                                                   storageQuota INTEGER,
                                                                   stateQuota INTEGER,
                                                                   stateChecksum BLOB,
                                                                   PRIMARY KEY(instance, subjectID, serviceID))`)

	return aoserrors.Wrap(err)
}

func (db *Database) createNodeStateTable() (err error) {
	log.Info("Create nodes table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS nodes (nodeID TEXT NOT NULL PRIMARY KEY,
                                                            state BLOB)`)

	return aoserrors.Wrap(err)
}

func (db *Database) isTableExist(name string) (result bool, err error) {
	rows, err := db.sql.Query("SELECT * FROM sqlite_master WHERE name = ? and type='table'", name)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}
	defer rows.Close()

	result = rows.Next()

	return result, aoserrors.Wrap(rows.Err())
}

func (db *Database) createConfigTable() (err error) {
	log.Info("Create config table")

	if _, err = db.sql.Exec(
		`CREATE TABLE config (
			cursor TEXT,
			componentsUpdateInfo BLOB,
			fotaUpdateState BLOB,
			sotaUpdateState BLOB,
			desiredInstances BLOB)`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = db.sql.Exec(
		`INSERT INTO config (
			cursor,
			componentsUpdateInfo,
			fotaUpdateState,
			sotaUpdateState,
			desiredInstances) values(?, ?, ?, ?, ?)`,
		"", "", json.RawMessage{}, json.RawMessage{}, json.RawMessage("[]")); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (db *Database) getServicesFromQuery(
	query string, args ...interface{},
) (services []imagemanager.ServiceInfo, err error) {
	rows, err := db.sql.Query(query, args...)
	if err != nil {
		return services, aoserrors.Wrap(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			service      imagemanager.ServiceInfo
			configJSON   []byte
			layers       []byte
			exposedPorts []byte
		)

		if err = rows.Scan(&service.ServiceID, &service.Version, &service.ProviderID, &service.URL, &service.RemoteURL,
			&service.Path, &service.Size, &service.Timestamp, &service.Cached, &configJSON, &layers,
			&service.Sha256, &exposedPorts, &service.GID); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		if err = json.Unmarshal(configJSON, &service.Config); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		if err = json.Unmarshal(layers, &service.Layers); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		if err = json.Unmarshal(exposedPorts, &service.ExposedPorts); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		services = append(services, service)
	}

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	return services, nil
}

func (db *Database) getMaxServiceVersion(serviceID string) (version string, err error) {
	var rows *sql.Rows
	if serviceID != "" {
		rows, err = db.sql.Query("SELECT version FROM services WHERE id = ?", serviceID)
	} else {
		rows, err = db.sql.Query("SELECT version FROM services")
	}

	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	defer rows.Close()

	if rows.Err() != nil {
		return "", aoserrors.Wrap(rows.Err())
	}

	maxVersion := "0.0.0"

	for rows.Next() {
		var version string
		if err = rows.Scan(&version); err != nil {
			return "", aoserrors.Wrap(err)
		}

		greater, err := semverutils.GreaterThan(version, maxVersion)
		if err != nil {
			return "", aoserrors.Wrap(err)
		}

		if greater {
			maxVersion = version
		}
	}

	return maxVersion, nil
}

func (db *Database) getLayersFromQuery(
	query string, args ...interface{},
) (layers []imagemanager.LayerInfo, err error) {
	rows, err := db.sql.Query(query, args...)
	if err != nil {
		return layers, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	for rows.Next() {
		var layer imagemanager.LayerInfo

		if err = rows.Scan(
			&layer.Digest, &layer.LayerID, &layer.Version,
			&layer.URL, &layer.RemoteURL, &layer.Path, &layer.Size, &layer.Timestamp, &layer.Sha256,
			&layer.Cached); err != nil {
			return layers, aoserrors.Wrap(err)
		}

		layers = append(layers, layer)
	}

	return layers, nil
}
