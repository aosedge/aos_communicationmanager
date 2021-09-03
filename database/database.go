// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2021 Renesas Inc.
// Copyright 2021 EPAM Systems Inc.
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

	_ "github.com/mattn/go-sqlite3" //ignore lint
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/migration"

	"aos_communicationmanager/config"
	"aos_communicationmanager/umcontroller"
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

// Database structure with database information
type Database struct {
	sql *sql.DB
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new database handle
func New(config *config.Config) (db *Database, err error) {
	fileName := path.Join(config.WorkingDir, dbFileName)

	log.WithField("fileName", fileName).Debug("Open database")

	if err = os.MkdirAll(filepath.Dir(fileName), 0755); err != nil {
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

	return db, nil
}

// SetJournalCursor stores system logger cursor
func (db *Database) SetJournalCursor(cursor string) (err error) {
	result, err := db.sql.Exec("UPDATE config SET cursor = ?", cursor)
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

// GetJournalCursor retrieves logger cursor
func (db *Database) GetJournalCursor() (cursor string, err error) {
	stmt, err := db.sql.Prepare("SELECT cursor FROM config")
	if err != nil {
		return cursor, aoserrors.Wrap(err)
	}
	defer stmt.Close()

	err = stmt.QueryRow().Scan(&cursor)
	if err != nil {
		if err == sql.ErrNoRows {
			return cursor, aoserrors.Wrap(errNotExist)
		}

		return cursor, aoserrors.Wrap(err)
	}

	return cursor, nil
}

// SetComponentsUpdateInfo store update data for update managers
func (db *Database) SetComponentsUpdateInfo(updateInfo []umcontroller.SystemComponent) (err error) {
	dataJSON, err := json.Marshal(&updateInfo)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	result, err := db.sql.Exec("UPDATE config SET componentsUpdateInfo = ?", dataJSON)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if count == 0 {
		return errNotExist
	}

	return nil
}

// GetComponentsUpdateInfo returns update data for system components
func (db *Database) GetComponentsUpdateInfo() (updateInfo []umcontroller.SystemComponent, err error) {
	stmt, err := db.sql.Prepare("SELECT componentsUpdateInfo FROM config")
	if err != nil {
		return updateInfo, aoserrors.Wrap(err)
	}
	defer stmt.Close()

	var dataJSON []byte

	if err = stmt.QueryRow().Scan(&dataJSON); err != nil {
		if err == sql.ErrNoRows {
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

// Close closes database
func (db *Database) Close() {
	db.sql.Close()
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

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
			componentsUpdateInfo BLOB)`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = db.sql.Exec(
		`INSERT INTO config (
			cursor,
			componentsUpdateInfo) values(?, ?)`, "", ""); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
