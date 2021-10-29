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
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/journal"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/retryhelper"

	"aos_communicationmanager/alerts"
	amqp "aos_communicationmanager/amqphandler"
	"aos_communicationmanager/boardconfig"
	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/cmserver"
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

const (
	initReconnectTimeout = 10 * time.Second
	maxReconnectTimeout  = 10 * time.Minute
)

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
	cmServer      *cmserver.CMServer
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
		cm.downloader, cm.db, cm.amqp); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	// Create CM server
	if cm.cmServer, err = cmserver.New(cfg, cm.statusHandler, false); err != nil {
		return cm, aoserrors.Wrap(err)
	}

	return cm, nil
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

func (cm *communicationManager) getServiceDiscoveryURL(cfg *config.Config) (serviceDiscoveryURL string) {
	// Get organization names from certificate and use it as discovery URL
	orgNames, err := cm.crypt.GetOrganization()
	if err != nil {
		log.Warningf("Organization name will be taken from config file: %s", err)

		return cfg.ServiceDiscoveryURL
	}

	if len(orgNames) == 0 || orgNames[0] == "" {
		log.Warn("Certificate organization name is empty or organization is not a single")

		return cfg.ServiceDiscoveryURL
	}

	url := url.URL{
		Scheme: "https",
		Host:   orgNames[0],
	}

	return url.String() + ":9000"
}

func (cm *communicationManager) processMessage(message amqp.Message) (err error) {
	switch data := message.Data.(type) {
	case *cloudprotocol.DecodedDesiredStatus:
		log.Info("Receive desired status message")

		cm.statusHandler.ProcessDesiredStatus(*data)

		return nil

	case *cloudprotocol.DecodedOverrideEnvVars:
		log.Info("Receive override env vars message")

		if err = cm.smController.OverrideEnvVars(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.StateAcceptance:
		log.WithFields(log.Fields{
			"serviceID": data.ServiceID,
			"result":    data.Result}).Info("Receive state acceptance message")

		if err = cm.smController.ServiceStateAcceptance(message.CorrelationID, *data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.UpdateState:
		log.WithFields(log.Fields{
			"serviceID": data.ServiceID,
			"checksum":  data.Checksum}).Info("Receive update state message")

		if err = cm.smController.SetServiceState(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.RequestServiceLog:
		log.WithFields(log.Fields{
			"serviceID": data.ServiceID,
			"from":      data.From,
			"till":      data.Till}).Info("Receive request service log message")

		if err = cm.smController.GetServiceLog(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.RequestServiceCrashLog:
		log.WithFields(log.Fields{
			"serviceID": data.ServiceID}).Info("Receive request service crash log message")

		if err = cm.smController.GetServiceCrashLog(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.RequestSystemLog:
		log.WithFields(log.Fields{
			"from": data.From,
			"till": data.Till}).Info("Receive request system log message")

		if err = cm.smController.GetSystemLog(*data); err != nil {
			return aoserrors.Wrap(err)
		}

	case *cloudprotocol.RenewCertsNotificationWithPwd:
		log.Info("Receive renew certificates notification message")

		if err = cm.iam.RenewCertificatesNotification(data.Password, data.Certificates); err != nil {
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
			if err, ok := message.Data.(error); ok {
				log.Errorf("Receive error: %s", err)
			}

			if err := cm.processMessage(message); err != nil {
				log.Errorf("Error processing message: %s", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (cm *communicationManager) handleConnection(ctx context.Context, serviceDiscoveryURL string) {
	for {
		retryhelper.Retry(ctx,
			func() (err error) {
				if err = cm.amqp.Connect(cm.crypt, serviceDiscoveryURL,
					cm.iam.GetSystemID(), cm.iam.GetUsers()); err != nil {
					return aoserrors.Wrap(err)
				}

				return nil
			},
			func(retryCount int, delay time.Duration, err error) {
				log.Errorf("Can't establish connection: %s", err)
				log.Debugf("Retry connection in %v", delay)
			},
			0, initReconnectTimeout, maxReconnectTimeout)

		if err := cm.statusHandler.SetUsers(cm.iam.GetUsers()); err != nil {
			log.Errorf("Can't set users: %s", err)
		}

		if err := cm.statusHandler.SendUnitStatus(); err != nil {
			log.Errorf("Can't send unit status: %s", err)
		}

		cm.handleMessages(ctx)

		if ctx.Err() != nil {
			return
		}
	}
}

func (cm *communicationManager) handleUsers(ctx context.Context) {
	readyChannel := make(chan struct{}, 1)

	go func() {
		cm.smController.WaitForReady()
		cm.umController.WaitForReady()

		readyChannel <- struct{}{}
	}()

	isReady := false

	for {
		select {
		case <-readyChannel:
			if err := cm.statusHandler.SetUsers(cm.iam.GetUsers()); err != nil {
				log.Errorf("Can't set users: %s", err)
			}

			isReady = true

		case users := <-cm.iam.UsersChangedChannel():
			if isReady {
				if err := cm.statusHandler.SetUsers(users); err != nil {
					log.Errorf("Can't set users: %s", err)
				}
			}

			if err := cm.amqp.Disconnect(); err != nil {
				log.Errorf("Can't disconnect: %s", err)
			}

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

	ctx, cancelFunc := context.WithCancel(context.Background())

	go cm.handleConnection(ctx, cm.getServiceDiscoveryURL(cfg))
	go cm.handleUsers(ctx)

	// Handle SIGTERM

	terminateChannel := make(chan os.Signal, 1)

	signal.Notify(terminateChannel, os.Interrupt, syscall.SIGTERM)

	<-terminateChannel

	cm.close()

	cancelFunc()

	os.Exit(0)
}
