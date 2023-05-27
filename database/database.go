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
	"path"
	"path/filepath"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/migration"
	_ "github.com/mattn/go-sqlite3" // ignore lint
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/downloader"
	"github.com/aoscloud/aos_communicationmanager/imagemanager"
	"github.com/aoscloud/aos_communicationmanager/launcher"
	"github.com/aoscloud/aos_communicationmanager/networkmanager"
	"github.com/aoscloud/aos_communicationmanager/storagestate"
	"github.com/aoscloud/aos_communicationmanager/umcontroller"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	busyTimeout = 60000
	journalMode = "WAL"
	syncMode    = "NORMAL"
)

const dbVersion = 0

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
	fileName := path.Join(config.WorkingDir, dbFileName)

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
func (db *Database) SetComponentsUpdateInfo(updateInfo []umcontroller.SystemComponent) (err error) {
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
func (db *Database) GetComponentsUpdateInfo() (updateInfo []umcontroller.SystemComponent, err error) {
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

// SetDesiredInstances sets desired instances status.
func (db *Database) SetDesiredInstances(instances json.RawMessage) (err error) {
	if err = db.executeQuery(`UPDATE config SET desiredInstances = ?`, instances); err != nil {
		return err
	}

	return nil
}

// GetDesiredInstances returns desired instances.
func (db *Database) GetDesiredInstances() (instances json.RawMessage, err error) {
	if err = db.getDataFromQuery(
		"SELECT desiredInstances FROM config",
		[]any{}, &instances); err != nil {
		if errors.Is(err, errNotExist) {
			return instances, launcher.ErrNotExist
		}
	}

	return instances, err
}

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

func (db *Database) RemoveDownloadInfo(filePath string) (err error) {
	if err = db.executeQuery("DELETE FROM download WHERE path = ?", filePath); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

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
	return db.getServicesFromQuery(`SELECT * FROM services WHERE(id, aosVersion)
                                    IN (SELECT id, MAX(aosVersion) FROM services GROUP BY id)`)
}

// GetServiceInfo returns service info by ID.
func (db *Database) GetServiceInfo(serviceID string) (service imagemanager.ServiceInfo, err error) {
	var (
		configJSON   []byte
		layers       []byte
		exposedPorts []byte
	)

	if err = db.getDataFromQuery(
		"SELECT * FROM services WHERE aosVersion = (SELECT MAX(aosVersion) FROM services WHERE id = ?) AND id = ?",
		[]any{serviceID, serviceID},
		&service.ID, &service.AosVersion, &service.ProviderID, &service.VendorVersion, &service.Description,
		&service.URL, &service.RemoteURL, &service.Path, &service.Size, &service.Timestamp, &service.Cached,
		&configJSON, &layers, &service.Sha256, &service.Sha512, &exposedPorts, &service.GID); err != nil {
		if errors.Is(err, errNotExist) {
			return service, imagemanager.ErrNotExist
		}

		return service, err
	}

	if err = json.Unmarshal(configJSON, &service.Config); err != nil {
		return service, aoserrors.Wrap(err)
	}

	if err = json.Unmarshal(layers, &service.Layers); err != nil {
		return service, aoserrors.Wrap(err)
	}

	if err = json.Unmarshal(exposedPorts, &service.ExposedPorts); err != nil {
		return service, aoserrors.Wrap(err)
	}

	return service, nil
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

	return db.executeQuery("INSERT INTO services values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		service.ID, service.AosVersion, service.ProviderID, service.VendorVersion, service.Description,
		service.URL, service.RemoteURL, service.Path, service.Size, service.Timestamp, service.Cached,
		configJSON, layers, service.Sha256, service.Sha512, exposedPorts, service.GID)
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
func (db *Database) RemoveService(serviceID string, aosVersion uint64) (err error) {
	if err = db.executeQuery("DELETE FROM services WHERE id = ? AND aosVersion = ?",
		serviceID, aosVersion); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

// GetAllServiceVersions returns all service versions.
func (db *Database) GetServiceVersions(serviceID string) (services []imagemanager.ServiceInfo, err error) {
	if services, err = db.getServicesFromQuery(
		"SELECT * FROM services WHERE id = ? ORDER BY aosVersion", serviceID); err != nil {
		return nil, err
	}

	if len(services) == 0 {
		return nil, imagemanager.ErrNotExist
	}

	return services, nil
}

// GetLayersInfo returns layers info.
func (db *Database) GetLayersInfo() ([]imagemanager.LayerInfo, error) {
	return db.getLayersFromQuery("SELECT * FROM layers")
}

// GetLayerInfo returns layer info by ID.
func (db *Database) GetLayerInfo(digest string) (layer imagemanager.LayerInfo, err error) {
	if err = db.getDataFromQuery("SELECT * FROM layers WHERE digest = ?",
		[]any{digest}, &layer.Digest, &layer.ID, &layer.AosVersion, &layer.VendorVersion, &layer.Description,
		&layer.URL, &layer.RemoteURL, &layer.Path, &layer.Size, &layer.Timestamp, &layer.Sha256, &layer.Sha512,
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
	return db.executeQuery("INSERT INTO layers values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		layer.Digest, layer.ID, layer.AosVersion, layer.VendorVersion, layer.Description, layer.URL, layer.RemoteURL,
		layer.Path, layer.Size, layer.Timestamp, layer.Sha256, layer.Sha512, layer.Cached)
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

// AddInstance adds instanace with uid.
func (db *Database) AddInstance(instance aostypes.InstanceIdent, uid int) error {
	return db.executeQuery("INSERT INTO instances values(?, ?, ?, ?)",
		instance.ServiceID, instance.SubjectID, instance.Instance, uid)
}

// GetInstanceUID gets uid by instanace ident.
func (db *Database) GetInstanceUID(instance aostypes.InstanceIdent) (int, error) {
	var uid int

	if err := db.getDataFromQuery("SELECT uid FROM instances WHERE serviceId = ? AND subjectId = ? AND  instance = ?",
		[]any{instance.ServiceID, instance.SubjectID, instance.Instance}, &uid); err != nil {
		if errors.Is(err, errNotExist) {
			return uid, launcher.ErrNotExist
		}

		return uid, err
	}

	return uid, nil
}

// GetAllUIDs gets all used uids.
func (db *Database) GetAllUIDs() (uids []int, err error) {
	rows, err := db.sql.Query("SELECT uid FROM instances")
	if err != nil {
		return uids, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	for rows.Next() {
		var uid int

		if err = rows.Scan(&uid); err != nil {
			return uids, aoserrors.Wrap(err)
		}

		uids = append(uids, uid)
	}

	return uids, nil
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

func (db *Database) AddNetworkInfo(networkInfo networkmanager.NetworkInfo) error {
	return db.executeQuery("INSERT INTO network values(?, ?, ?, ?)",
		networkInfo.NetworkID, networkInfo.IP, networkInfo.Subnet, networkInfo.VlanID)
}

func (db *Database) RemoveNetworkInfo(networkID string) (err error) {
	if err = db.executeQuery("DELETE FROM network WHERE networkID = ?", networkID); errors.Is(err, errNotExist) {
		return nil
	}

	return err
}

func (db *Database) GetNetworksInfo() ([]networkmanager.NetworkInfo, error) {
	rows, err := db.sql.Query("SELECT * FROM network")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	var networks []networkmanager.NetworkInfo

	for rows.Next() {
		var networkInfo networkmanager.NetworkInfo

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
	log.Info("Create service table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS download (path TEXT NOT NULL PRIMARY KEY,
                                                               targetType TEXT NOT NULL,
                                                               interruptReason TEXT,
                                                               downloaded INTEGER)`)

	return aoserrors.Wrap(err)
}

func (db *Database) createServiceTable() (err error) {
	log.Info("Create service table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS services (id TEXT NOT NULL ,
                                                               aosVersion INTEGER,
                                                               providerId TEXT,
                                                               vendorVersion TEXT,
                                                               description TEXT,
                                                               localURL   TEXT,
                                                               remoteURL  TEXT,
                                                               path TEXT,
                                                               size INTEGER,
                                                               timestamp TIMESTAMP,
                                                               cached INTEGER,
                                                               config BLOB,
                                                               layers BLOB,
                                                               sha256 BLOB,
                                                               sha512 BLOB,
                                                               exposedPorts BLOB,
                                                               gid INTEGER,
                                                               PRIMARY KEY(id, aosVersion))`)

	return aoserrors.Wrap(err)
}

func (db *Database) createLayersTable() (err error) {
	log.Info("Create layers table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS layers (digest TEXT NOT NULL PRIMARY KEY,
                                                             layerId TEXT,
                                                             aosVersion INTEGER,
                                                             vendorVersion TEXT,
                                                             description TEXT,
                                                             localURL   TEXT,
                                                             remoteURL  TEXT,
                                                             Path       TEXT,
                                                             Size       INTEGER,
                                                             Timestamp  TIMESTAMP,
                                                             sha256 BLOB,
                                                             sha512 BLOB,
                                                             cached INTEGER)`)

	return aoserrors.Wrap(err)
}

func (db *Database) createInstancesTable() (err error) {
	log.Info("Create instances table")

	_, err = db.sql.Exec(`CREATE TABLE IF NOT EXISTS instances (serviceId TEXT,
                                                                subjectId TEXT,
                                                                instance INTEGER,
                                                                uid integer,
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
		"", "", json.RawMessage{}, json.RawMessage{}, json.RawMessage{}); err != nil {
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

	if rows.Err() != nil {
		return nil, aoserrors.Wrap(rows.Err())
	}

	for rows.Next() {
		var (
			service      imagemanager.ServiceInfo
			configJSON   []byte
			layers       []byte
			exposedPorts []byte
		)

		if err = rows.Scan(&service.ID, &service.AosVersion, &service.ProviderID, &service.VendorVersion,
			&service.Description, &service.URL, &service.RemoteURL, &service.Path, &service.Size,
			&service.Timestamp, &service.Cached, &configJSON, &layers, &service.Sha256,
			&service.Sha512, &exposedPorts, &service.GID); err != nil {
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

	return services, nil
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
			&layer.Digest, &layer.ID, &layer.AosVersion, &layer.VendorVersion, &layer.Description,
			&layer.URL, &layer.RemoteURL, &layer.Path, &layer.Size, &layer.Timestamp, &layer.Sha256, &layer.Sha512,
			&layer.Cached); err != nil {
			return layers, aoserrors.Wrap(err)
		}

		layers = append(layers, layer)
	}

	return layers, nil
}
