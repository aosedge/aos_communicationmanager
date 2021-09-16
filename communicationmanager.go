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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/journal"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"

	"aos_communicationmanager/alerts"
	amqp "aos_communicationmanager/amqphandler"
	"aos_communicationmanager/boardconfig"
	"aos_communicationmanager/config"
	"aos_communicationmanager/database"
	"aos_communicationmanager/downloader"
	"aos_communicationmanager/fcrypt"
	"aos_communicationmanager/fileserver"
	"aos_communicationmanager/iamclient"
	"aos_communicationmanager/monitoring"
	"aos_communicationmanager/smcontroller"
	"aos_communicationmanager/umcontroller"
	"aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const reconnectTimeout = 10 * time.Second

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type communicationManager struct {
	db            *database.Database
	amqp          *amqp.AmqpHandler
	iam           *iamclient.Client
	crypt         *fcrypt.CryptoContext
	alerts        *alerts.Alerts
	monitor       *monitoring.Monitor
	downloader    *downloader.Downloader
	fileServer    *fileserver.FileServer
	smController  *smcontroller.Controller
	umController  *umcontroller.Controller
	boardConfig   *boardconfig.Instance
	statusHandler *unitstatushandler.Instance
}

type journalHook struct {
	severityMap map[log.Level]journal.Priority
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// GitSummary provided by govvv at compile-time
var GitSummary = "Unknown"

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true})
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * CommunicationManager
 **********************************************************************************************************************/

func newCommunicationManager(cfg *config.Config) (cm *communicationManager, err error) {
	defer func() {
		if err != nil {
			cm.close()
			cm = nil
		}
	}()

	cm = &communicationManager{}

	if cm.db, err = database.New(cfg); err != nil {
		// Try again after reset

		log.Errorf("Can't create DB: %s", err)

		if err = reset(cfg); err != nil {
			log.Errorf("Can't reset CM: %s", err)
		}

		if cm.db, err = database.New(cfg); err != nil {
			return cm, aoserrors.Wrap(err)
		}
	}

	// Create AMQP handler
	if cm.amqp, err = amqp.New(); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create IAM client
	if cm.iam, err = iamclient.New(cfg, cm.amqp, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create crypto context
	if cm.crypt, err = fcrypt.New(cfg.Crypt, cm.iam); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create alerts
	if cm.alerts, err = alerts.New(cfg, cm.amqp, cm.db); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create monitor
	if cm.monitor, err = monitoring.New(cfg, cm.alerts, nil, cm.amqp); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create downloader
	if cm.downloader, err = downloader.New("CM", cfg, cm.crypt, cm.alerts); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create file server
	if cm.fileServer, err = fileserver.New(cfg); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create SM controller
	if cm.smController, err = smcontroller.New(cfg, cm.amqp, cm.alerts, cm.monitor, cm.fileServer, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create UM controller
	if cm.umController, err = umcontroller.New(cfg, cm.db, cm.fileServer, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create board config
	if cm.boardConfig, err = boardconfig.New(cfg, cm.smController); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create unit status handler
	if cm.statusHandler, err = unitstatushandler.New(cfg, cm.boardConfig, cm.umController, cm.smController,
		cm.downloader, cm.amqp); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	return cm, nil
}

func (cm *communicationManager) close() {
	// Close unit status handler
	if cm.statusHandler != nil {
		cm.statusHandler.Close()
	}

	// Close UM controller
	if cm.umController != nil {
		cm.umController.Close()
	}

	// Close SM controller
	if cm.smController != nil {
		cm.smController.Close()
	}

	// Close file server
	if cm.fileServer != nil {
		cm.fileServer.Close()
	}

	// Close monitor
	if cm.monitor != nil {
		cm.monitor.Close()
	}

	// Close alerts
	if cm.alerts != nil {
		cm.alerts.Close()
	}

	// Close crypto context
	if cm.crypt != nil {
		cm.crypt.Close()
	}

	// Close iam
	if cm.iam != nil {
		cm.iam.Close()
	}

	// Close amqp
	if cm.amqp != nil {
		cm.amqp.Close()
	}

	// Close DB
	if cm.db != nil {
		cm.db.Close()
	}
}

func (cm *communicationManager) run() {
}

/***********************************************************************************************************************
 * Systemd journal hook
 **********************************************************************************************************************/

func newJournalHook() (hook *journalHook) {
	hook = &journalHook{
		severityMap: map[log.Level]journal.Priority{
			log.DebugLevel: journal.PriDebug,
			log.InfoLevel:  journal.PriInfo,
			log.WarnLevel:  journal.PriWarning,
			log.ErrorLevel: journal.PriErr,
			log.FatalLevel: journal.PriCrit,
			log.PanicLevel: journal.PriEmerg,
		}}

	return hook
}

func (hook *journalHook) Fire(entry *log.Entry) (err error) {
	if entry == nil {
		return aoserrors.New("log entry is nil")
	}

	logMessage, err := entry.String()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	err = journal.Print(hook.severityMap[entry.Level], logMessage)

	return aoserrors.Wrap(err)
}

func (hook *journalHook) Levels() []log.Level {
	return []log.Level{
		log.PanicLevel,
		log.FatalLevel,
		log.ErrorLevel,
		log.WarnLevel,
		log.InfoLevel,
		log.DebugLevel,
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func reset(cfg *config.Config) (err error) {
	log.Info("Cleanup working directory")

	if err := os.RemoveAll(cfg.WorkingDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func main() {
	// Initialize command line flags
	configFile := flag.String("c", "aos_communicationmanager.cfg", "path to config file")
	strLogLevel := flag.String("v", "info", `log level: "debug", "info", "warn", "error", "fatal", "panic"`)
	doReset := flag.Bool("reset", false, `cleanup working directory`)
	showVersion := flag.Bool("version", false, `show communication manager version`)
	useJournal := flag.Bool("j", false, "output logs to systemd journal")

	flag.Parse()

	// Show version

	if *showVersion {
		fmt.Printf("Version: %s\n", GitSummary)

		return
	}

	// Set log output

	if *useJournal {
		log.AddHook(newJournalHook())
		log.SetOutput(ioutil.Discard)
	} else {
		log.SetOutput(os.Stdout)
	}

	// Set log level

	logLevel, err := log.ParseLevel(*strLogLevel)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	log.SetLevel(logLevel)

	// Parse config

	cfg, err := config.New(*configFile)
	if err != nil {
		// Config is important to make CM works properly. If we can't parse the config no reason to continue.
		// If the error is temporary CM will be restarted by systemd.
		log.Fatalf("Can't parse config: %s", err)
	}

	// Do reset

	if *doReset {
		if err = reset(cfg); err != nil {
			// Try to continue even if reset failed.
			log.Errorf("Can't perform reset: %s", err)

			os.Exit(1)
		}

		log.Info("CM reset successfully")

		os.Exit(0)
	}

	log.WithFields(log.Fields{"configFile": *configFile, "version": GitSummary}).Info("Start communication manager")

	cm, err := newCommunicationManager(cfg)
	if err != nil {
		log.Fatalf("Can't create communication manager: %s", err)
	}
	defer cm.close()

	go cm.run()

	// Handle SIGTERM

	terminateChannel := make(chan os.Signal, 1)

	signal.Notify(terminateChannel, os.Interrupt, syscall.SIGTERM)

	<-terminateChannel

	cm.close()

	os.Exit(0)
}
