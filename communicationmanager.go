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

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/journalalerts"
	"github.com/aoscloud/aos_common/resourcemonitor"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	"github.com/aoscloud/aos_common/utils/retryhelper"
	"github.com/coreos/go-systemd/daemon"
	"github.com/coreos/go-systemd/journal"
	"github.com/google/go-tpm/tpm2"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/alerts"
	amqp "github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/cmserver"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/database"
	"github.com/aoscloud/aos_communicationmanager/downloader"
	"github.com/aoscloud/aos_communicationmanager/fcrypt"
	"github.com/aoscloud/aos_communicationmanager/iamclient"
	"github.com/aoscloud/aos_communicationmanager/imagemanager"
	"github.com/aoscloud/aos_communicationmanager/launcher"
	"github.com/aoscloud/aos_communicationmanager/monitorcontroller"
	"github.com/aoscloud/aos_communicationmanager/networkmanager"
	"github.com/aoscloud/aos_communicationmanager/smcontroller"
	"github.com/aoscloud/aos_communicationmanager/storagestate"
	"github.com/aoscloud/aos_communicationmanager/umcontroller"
	"github.com/aoscloud/aos_communicationmanager/unitconfig"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	initReconnectTimeout = 10 * time.Second
	maxReconnectTimeout  = 10 * time.Minute
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type communicationManager struct {
	db                *database.Database
	amqp              *amqp.AmqpHandler
	iam               *iamclient.Client
	crypt             *fcrypt.CryptoHandler
	cryptoContext     *cryptutils.CryptoContext
	journalAlerts     *journalalerts.JournalAlerts
	alerts            *alerts.Alerts
	monitorcontroller *monitorcontroller.MonitorController
	resourcemonitor   *resourcemonitor.ResourceMonitor
	downloader        *downloader.Downloader
	smController      *smcontroller.Controller
	umController      *umcontroller.Controller
	unitConfig        *unitconfig.Instance
	statusHandler     *unitstatushandler.Instance
	launcher          *launcher.Launcher
	imagemanager      *imagemanager.Imagemanager
	network           *networkmanager.NetworkManager
	storageState      *storagestate.StorageState
	cmServer          *cmserver.CMServer
}

type journalHook struct {
	severityMap map[log.Level]journal.Priority
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// GitSummary provided by govvv at compile-time.
var GitSummary = "Unknown" // nolint:gochecknoglobals

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * CommunicationManager
 **********************************************************************************************************************/

func newCommunicationManager(cfg *config.Config) (cm *communicationManager, err error) { //nolint:funlen
	defer func() {
		if err != nil {
			cm.close()
			cm = nil
		}
	}()

	cm = &communicationManager{}

	// Try again after reset
	if cm.db, err = database.New(cfg); err != nil {
		log.Errorf("Can't create DB: %s", err)

		if err = reset(cfg); err != nil {
			log.Errorf("Can't reset CM: %s", err)
		}

		if cm.db, err = database.New(cfg); err != nil {
			return cm, aoserrors.Wrap(err)
		}
	}

	if cm.amqp, err = amqp.New(); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.cryptoContext, err = cryptutils.NewCryptoContext(cfg.Crypt.CACert); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if cm.iam, err = iamclient.New(cfg, cm.amqp, cm.cryptoContext, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if err = initPKCS(cfg.Crypt); err != nil {
		return nil, err
	}

	if cm.crypt, err = fcrypt.New(cm.iam, cm.cryptoContext, cfg.ServiceDiscoveryURL); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.alerts, err = alerts.New(cfg.Alerts, cm.amqp); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cfg.Alerts.JournalAlerts != nil {
		if cm.journalAlerts, err = journalalerts.New(*cfg.Alerts.JournalAlerts, nil, cm.db, cm.alerts); err != nil {
			return cm, aoserrors.Wrap(err)
		}
	}

	if cm.monitorcontroller, err = monitorcontroller.New(cfg, cm.amqp); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cfg.Monitoring.MonitorConfig != nil {
		if cm.resourcemonitor, err = resourcemonitor.New(cm.iam.GetNodeID(), *cfg.Monitoring.MonitorConfig,
			cm.alerts, cm.monitorcontroller, nil); err != nil {
			return cm, aoserrors.Wrap(err)
		}
	}

	if cm.downloader, err = downloader.New("CM", cfg, cm.alerts, cm.db); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.smController, err = smcontroller.New(
		cfg, cm.amqp, cm.alerts, cm.monitorcontroller, cm.iam, cm.cryptoContext, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.umController, err = umcontroller.New(cfg, cm.db, cm.iam, cm.cryptoContext, cm.crypt, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.unitConfig, err = unitconfig.New(cfg, cm.smController); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.storageState, err = storagestate.New(cfg, cm.amqp, cm.db); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.imagemanager, err = imagemanager.New(cfg, cm.db, cm.storageState, cm.crypt); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.network, err = networkmanager.New(cm.db, cm.smController, cfg); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.launcher, err = launcher.New(
		cfg, cm.db, cm.smController, cm.imagemanager, cm.unitConfig, cm.storageState, cm.network); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.statusHandler, err = unitstatushandler.New(cfg, cm.unitConfig, cm.umController, cm.imagemanager, cm.launcher,
		cm.downloader, cm.db, cm.amqp); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	if cm.cmServer, err = cmserver.New(cfg, cm.statusHandler, cm.iam, cm.cryptoContext, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	return cm, nil
}

func initPKCS(cfg config.Crypt) (err error) {
	cryptutils.DefaultPKCS11Library = cfg.Pkcs11Library

	// Open TPM Device
	if cfg.TpmDevice != "" {
		if cryptutils.DefaultTPMDevice, err = tpm2.OpenTPM(cfg.TpmDevice); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (cm *communicationManager) close() {
	// Close CM server
	if cm.cmServer != nil {
		cm.cmServer.Close()
	}

	// Close unit status handler
	if cm.statusHandler != nil {
		cm.statusHandler.Close()
	}

	// Close CM launcher
	if cm.launcher != nil {
		cm.launcher.Close()
	}

	// Close CM image manager
	if cm.imagemanager != nil {
		cm.imagemanager.Close()
	}

	// Close CM storage state
	if cm.storageState != nil {
		cm.storageState.Close()
	}

	// Close UM controller
	if cm.umController != nil {
		cm.umController.Close()
	}

	// Close SM controller
	if cm.smController != nil {
		cm.smController.Close()
	}

	// Close resourcemonitor
	if cm.resourcemonitor != nil {
		cm.resourcemonitor.Close()
	}

	// Close journal alerts
	if cm.journalAlerts != nil {
		cm.journalAlerts.Close()
	}

	// Close alerts
	if cm.alerts != nil {
		cm.alerts.Close()
	}

	// Close monitorcontroller
	if cm.monitorcontroller != nil {
		cm.monitorcontroller.Close()
	}

	// Close downloader
	if cm.downloader != nil {
		cm.downloader.Close()
	}

	// Close iam
	if cm.iam != nil {
		cm.iam.Close()
	}

	// Close amqp
	if cm.amqp != nil {
		cm.amqp.Close()
	}

	// Close crypto context
	if cm.cryptoContext != nil {
		cm.cryptoContext.Close()
	}

	// Close TPM Device
	if cryptutils.DefaultTPMDevice != nil {
		cryptutils.DefaultTPMDevice.Close()
	}

	// Close DB
	if cm.db != nil {
		cm.db.Close()
	}
}

func (cm *communicationManager) processMessage(message amqp.Message) (err error) {
	switch data := message.(type) {
	case *cloudprotocol.DesiredStatus:
		log.Info("Receive desired status message")

		cm.statusHandler.ProcessDesiredStatus(*data)

		return nil

	case *cloudprotocol.OverrideEnvVars:
		log.Info("Receive override env vars message")

		if err = cm.smController.OverrideEnvVars(cm.iam.GetNodeID(), *data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.StateAcceptance:
		log.WithFields(log.Fields{
			"serviceID": data.ServiceID,
			"result":    data.Result,
		}).Info("Receive state acceptance message")

		if err = cm.storageState.StateAcceptance(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.UpdateState:
		log.WithFields(log.Fields{
			"serviceID": data.ServiceID,
			"checksum":  data.Checksum,
		}).Info("Receive update state message")

		if err = cm.storageState.UpdateState(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.RequestLog:
		log.WithFields(log.Fields{
			"LogID":   data.LogID,
			"LogType": data.LogType,
			"from":    data.Filter.From,
			"till":    data.Filter.Till,
		}).Info("Receive request service log message")

		if err = cm.smController.GetLog(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.RenewCertsNotification:
		log.Info("Receive renew certificates notification message")

		if data.UnitSecret.Version != cloudprotocol.UnitSecretVersion {
			return aoserrors.New("unit secure version mismatch")
		}

		if err = cm.iam.RenewCertificatesNotification(
			data.UnitSecret.Data.OwnerPassword, data.Certificates); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.IssuedUnitCerts:
		log.Info("Receive issued unit certificates message")

		if err = cm.iam.InstallCertificates(data.Certificates, cm.crypt); err != nil {
			return aoserrors.Wrap(err)
		}

	default:
		log.Warnf("Receive unsupported amqp message: %s", reflect.TypeOf(data))
	}

	return nil
}

func (cm *communicationManager) handleMessages(ctx context.Context) {
	for {
		select {
		case message := <-cm.amqp.MessageChannel:
			if err, ok := message.(error); ok {
				log.Warnf("AMQP error: %s", err)
				return
			}

			if err := cm.processMessage(message); err != nil {
				log.Errorf("Error processing message: %s", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (cm *communicationManager) handleConnection(ctx context.Context, serviceDiscoveryURLs []string) {
	for {
		_ = retryhelper.Retry(ctx,
			func() (err error) {
				for _, serviceDiscoveryURL := range serviceDiscoveryURLs {
					if err = cm.amqp.Connect(cm.crypt, serviceDiscoveryURL, cm.iam.GetSystemID(), false); err == nil {
						return nil
					} else {
						log.Warnf("Can't connect to SD: %v", err)
					}
				}

				return aoserrors.Wrap(err)
			},
			func(retryCount int, delay time.Duration, err error) {
				log.Errorf("Can't establish connection: %s", err)
				log.Debugf("Retry connection in %v", delay)
			},
			0, initReconnectTimeout, maxReconnectTimeout)

		if err := cm.statusHandler.SendUnitStatus(); err != nil {
			log.Errorf("Can't send unit status: %s", err)
		}

		cm.handleMessages(ctx)

		if err := cm.amqp.Disconnect(); err != nil {
			log.Errorf("Disconnect error: %s", err)
		}

		if ctx.Err() != nil {
			return
		}
	}
}

func (cm *communicationManager) handleStatusChannels(ctx context.Context) {
	for {
		select {
		case runStatus := <-cm.launcher.GetRunStatusesChannel():
			if err := cm.statusHandler.ProcessRunStatus(runStatus); err != nil {
				log.Errorf("Can't process run statusL %v", err)
			}

		case instanceStatus := <-cm.smController.GetUpdateInstancesStatusChannel():
			cm.statusHandler.ProcessUpdateInstanceStatus(instanceStatus)

		case <-ctx.Done():
			return
		}
	}
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
		},
	}

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
		fmt.Printf("Version: %s\n", GitSummary) // nolint:forbidigo // logs are't initialized

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

	// Notify systemd
	if _, err = daemon.SdNotify(false, daemon.SdNotifyReady); err != nil {
		log.Errorf("Can't notify systemd: %s", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	go cm.handleConnection(ctx, cm.crypt.GetServiceDiscoveryURLs())
	go cm.handleStatusChannels(ctx)

	// Handle SIGTERM

	terminateChannel := make(chan os.Signal, 1)

	signal.Notify(terminateChannel, os.Interrupt, syscall.SIGTERM)

	<-terminateChannel

	cancelFunc()
}
