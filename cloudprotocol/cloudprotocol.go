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

package cloudprotocol

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"

	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

// ProtocolVersion specifies supported protocol version
const ProtocolVersion = 3

// UnitSecretVersion specifies supported version of UnitSecret message
const UnitSecretVersion = 2

// Cloud message types
const (
	DesiredStatusType          = "desiredStatus"
	RequestServiceCrashLogType = "requestServiceCrashLog"
	RequestServiceLogType      = "requestServiceLog"
	RequestSystemLogType       = "requestSystemLog"
	ServiceDiscoveryType       = "serviceDiscovery"
	StateAcceptanceType        = "stateAcceptance"
	UpdateStateType            = "updateState"
	DeviceErrors               = "deviceErrors"
	RenewCertsNotificationType = "renewCertificatesNotification"
	IssuedUnitCertsType        = "issuedUnitCertificates"
	OverrideEnvVarsType        = "overrideEnvVars"
)

// Device message types
const (
	AlertsType                       = "alerts"
	MonitoringDataType               = "monitoringData"
	NewStateType                     = "newState"
	PushLogType                      = "pushLog"
	StateRequestType                 = "stateRequest"
	UnitStatusType                   = "unitStatus"
	IssueUnitCertsType               = "issueUnitCertificates"
	InstallUnitCertsConfirmationType = "installUnitCertificatesConfirmation"
	OverrideEnvVarsStatusType        = "overrideEnvVarsStatus"
)

// Alert tags
const (
	AlertTagSystemError = "systemError"
	AlertTagResource    = "resourceAlert"
	AlertTagAosCore     = "aosCore"
)

// Unit statuses
const (
	UnknownStatus     = "unknown"
	PendingStatus     = "pending"
	DownloadingStatus = "downloading"
	DownloadedStatus  = "downloaded"
	InstallingStatus  = "installing"
	InstalledStatus   = "installed"
	RemovingStatus    = "removing"
	RemovedStatus     = "removed"
	ErrorStatus       = "error"
)

// SOTA/FOTA schedule type
const (
	ForceUpdate     = "force"
	TriggerUpdate   = "trigger"
	TimetableUpdate = "timetable"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Message structure for AOS messages
type Message struct {
	Header MessageHeader `json:"header"`
	Data   interface{}   `json:"data"`
}

// MessageHeader message header
type MessageHeader struct {
	Version     uint64 `json:"version"`
	SystemID    string `json:"systemId"`
	MessageType string `json:"messageType"`
}

// ServiceDiscoveryRequest service discovery request
type ServiceDiscoveryRequest struct {
	Users []string `json:"users"`
}

// ServiceDiscoveryResponse service discovery response
type ServiceDiscoveryResponse struct {
	Version    uint64         `json:"version"`
	Connection ConnectionInfo `json:"connection"`
}

// ConnectionInfo AMQP connection info
type ConnectionInfo struct {
	SendParams    SendParams    `json:"sendParams"`
	ReceiveParams ReceiveParams `json:"receiveParams"`
}

// SendParams AMQP send parameters
type SendParams struct {
	Host      string         `json:"host"`
	User      string         `json:"user"`
	Password  string         `json:"password"`
	Mandatory bool           `json:"mandatory"`
	Immediate bool           `json:"immediate"`
	Exchange  ExchangeParams `json:"exchange"`
}

// ExchangeParams AMQP exchange parameters
type ExchangeParams struct {
	Name       string `json:"name"`
	Durable    bool   `json:"durable"`
	AutoDetect bool   `json:"autoDetect"`
	Internal   bool   `json:"internal"`
	NoWait     bool   `json:"noWait"`
}

// ReceiveParams AMQP receive parameters
type ReceiveParams struct {
	Host      string    `json:"host"`
	User      string    `json:"user"`
	Password  string    `json:"password"`
	Consumer  string    `json:"consumer"`
	AutoAck   bool      `json:"autoAck"`
	Exclusive bool      `json:"exclusive"`
	NoLocal   bool      `json:"noLocal"`
	NoWait    bool      `json:"noWait"`
	Queue     QueueInfo `json:"queue"`
}

// QueueInfo AMQP queue info
type QueueInfo struct {
	Name             string `json:"name"`
	Durable          bool   `json:"durable"`
	DeleteWhenUnused bool   `json:"deleteWhenUnused"`
	Exclusive        bool   `json:"exclusive"`
	NoWait           bool   `json:"noWait"`
}

// DesiredStatus desired status message
type DesiredStatus struct {
	BoardConfig       []byte             `json:"boardConfig"`
	Services          []byte             `json:"services"`
	Layers            []byte             `json:"layers"`
	Components        []byte             `json:"components"`
	FOTASchedule      []byte             `json:"fotaSchedule"`
	SOTASchedule      []byte             `json:"sotaSchedule"`
	CertificateChains []CertificateChain `json:"certificateChains,omitempty"`
	Certificates      []Certificate      `json:"certificates,omitempty"`
}

// RequestServiceCrashLog request service crash log message
type RequestServiceCrashLog struct {
	ServiceID string     `json:"serviceId"`
	LogID     string     `json:"logID"`
	From      *time.Time `json:"from"`
	Till      *time.Time `json:"till"`
}

// RequestServiceLog request service log message
type RequestServiceLog struct {
	ServiceID string     `json:"serviceId"`
	LogID     string     `json:"logID"`
	From      *time.Time `json:"from"`
	Till      *time.Time `json:"till"`
}

// RequestSystemLog request system log message
type RequestSystemLog struct {
	LogID string     `json:"logID"`
	From  *time.Time `json:"from"`
	Till  *time.Time `json:"till"`
}

// StateAcceptance state acceptance message
type StateAcceptance struct {
	ServiceID string `json:"serviceId"`
	Checksum  string `json:"checksum"`
	Result    string `json:"result"`
	Reason    string `json:"reason"`
}

// DecryptionInfo update decryption info
type DecryptionInfo struct {
	BlockAlg     string `json:"blockAlg"`
	BlockIv      []byte `json:"blockIv"`
	BlockKey     []byte `json:"blockKey"`
	AsymAlg      string `json:"asymAlg"`
	ReceiverInfo *struct {
		Serial string `json:"serial"`
		Issuer []byte `json:"issuer"`
	} `json:"receiverInfo"`
}

// Signs message signature
type Signs struct {
	ChainName        string   `json:"chainName"`
	Alg              string   `json:"alg"`
	Value            []byte   `json:"value"`
	TrustedTimestamp string   `json:"trustedTimestamp"`
	OcspValues       []string `json:"ocspValues"`
}

// CertificateChain  certificate chain
type CertificateChain struct {
	Name         string   `json:"name"`
	Fingerprints []string `json:"fingerprints"`
}

// Certificate certificate structure
type Certificate struct {
	Fingerprint string `json:"fingerprint"`
	Certificate []byte `json:"certificate"`
}

// DecryptDataStruct struct contains how to decrypt data
type DecryptDataStruct struct {
	URLs           []string        `json:"urls"`
	Sha256         []byte          `json:"sha256"`
	Sha512         []byte          `json:"sha512"`
	Size           uint64          `json:"size"`
	DecryptionInfo *DecryptionInfo `json:"decryptionInfo,omitempty"`
	Signs          *Signs          `json:"signs,omitempty"`
}

// UpdateState state update message
type UpdateState struct {
	ServiceID string `json:"serviceId"`
	Checksum  string `json:"stateChecksum"`
	State     string `json:"state"`
}

// SystemAlert system alert structure
type SystemAlert struct {
	Message string `json:"message"`
}

// DownloadAlert download alert structure
type DownloadAlert struct {
	Message         string `json:"message"`
	Progress        string `json:"progress"`
	URL             string `json:"url"`
	DownloadedBytes string `json:"downloadedBytes"`
	TotalBytes      string `json:"totalBytes"`
}

// ResourceAlert resource alert structure
type ResourceAlert struct {
	Parameter string `json:"parameter"`
	Value     uint64 `json:"value"`
}

// ResourceValidateError resource validate error structure
type ResourceValidateError struct {
	Name   string   `json:"name"`
	Errors []string `json:"error"`
}

// ResourceValidateAlert resource validate alert structure
type ResourceValidateAlert struct {
	Type    string                  `json:"type"`
	Message []ResourceValidateError `json:"message"`
}

// AlertItem alert item structure
type AlertItem struct {
	Timestamp  time.Time   `json:"timestamp"`
	Tag        string      `json:"tag"`
	Source     string      `json:"source"`
	AosVersion uint64      `json:"aosVersion,omitempty"`
	Payload    interface{} `json:"payload"`
}

// Alerts alerts message structure
type Alerts []AlertItem

// NewState new state structure
type NewState struct {
	ServiceID string `json:"serviceId"`
	Checksum  string `json:"stateChecksum"`
	State     string `json:"state"`
}

// GlobalMonitoringData global monitoring data for service
type GlobalMonitoringData struct {
	RAM        uint64 `json:"ram"`
	CPU        uint64 `json:"cpu"`
	UsedDisk   uint64 `json:"usedDisk"`
	InTraffic  uint64 `json:"inTraffic"`
	OutTraffic uint64 `json:"outTraffic"`
}

// ServiceMonitoringData monitoring data for service
type ServiceMonitoringData struct {
	ServiceID  string `json:"serviceId"`
	RAM        uint64 `json:"ram"`
	CPU        uint64 `json:"cpu"`
	UsedDisk   uint64 `json:"usedDisk"`
	InTraffic  uint64 `json:"inTraffic"`
	OutTraffic uint64 `json:"outTraffic"`
}

// MonitoringData monitoring data structure
type MonitoringData struct {
	Timestamp    time.Time               `json:"timestamp"`
	Global       GlobalMonitoringData    `json:"global"`
	ServicesData []ServiceMonitoringData `json:"servicesData"`
}

// PushLog push service log structure
type PushLog struct {
	LogID     string `json:"logID"`
	PartCount uint64 `json:"partCount,omitempty"`
	Part      uint64 `json:"part,omitempty"`
	Data      []byte `json:"data,omitempty"`
	Error     string `json:"error,omitempty"`
}

// StateRequest state request structure
type StateRequest struct {
	ServiceID string `json:"serviceId"`
	Default   bool   `json:"default"`
}

// UnitStatus unit status structure
type UnitStatus struct {
	BoardConfig []BoardConfigInfo `json:"boardConfig"`
	Services    []ServiceInfo     `json:"services"`
	Layers      []LayerInfo       `json:"layers,omitempty"`
	Components  []ComponentInfo   `json:"components"`
}

// BoardConfigInfo board config information
type BoardConfigInfo struct {
	VendorVersion string `json:"vendorVersion"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
}

// ServiceInfo struct with service information
type ServiceInfo struct {
	ID            string `json:"id"`
	AosVersion    uint64 `json:"aosVersion"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	StateChecksum string `json:"stateChecksum,omitempty"`
}

// LayerInfo struct with layer info and status
type LayerInfo struct {
	ID         string `json:"id"`
	AosVersion uint64 `json:"aosVersion"`
	Digest     string `json:"digest"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

// ComponentInfo struct with system component info and status
type ComponentInfo struct {
	ID            string `json:"id"`
	AosVersion    uint64 `json:"aosVersion"`
	VendorVersion string `json:"vendorVersion"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
}

// ServiceAlertRules define service monitoring alerts rules
type ServiceAlertRules struct {
	RAM        *config.AlertRule `json:"ram,omitempty"`
	CPU        *config.AlertRule `json:"cpu,omitempty"`
	UsedDisk   *config.AlertRule `json:"usedDisk,omitempty"`
	InTraffic  *config.AlertRule `json:"inTraffic,omitempty"`
	OutTraffic *config.AlertRule `json:"outTraffic,omitempty"`
}

// VersionFromCloud common version structure
type VersionFromCloud struct {
	AosVersion    uint64 `json:"aosVersion"`
	VendorVersion string `json:"vendorVersion"`
	Description   string `json:"description"`
}

// ServiceInfoFromCloud decrypted service info
type ServiceInfoFromCloud struct {
	ID         string `json:"id"`
	ProviderID string `json:"providerId"`
	VersionFromCloud
	AlertRules *ServiceAlertRules `json:"alertRules,omitempty"`
	DecryptDataStruct
}

// LayerInfoFromCloud decrypted layer info
type LayerInfoFromCloud struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
	VersionFromCloud
	DecryptDataStruct
}

// ComponentInfoFromCloud decrypted component info
type ComponentInfoFromCloud struct {
	ID          string          `json:"id"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
	VersionFromCloud
	DecryptDataStruct
}

// Time represents time in format "00:00:00"
type Time struct {
	time.Time
}

// TimeSlot time slot with start and finish time
type TimeSlot struct {
	Start  Time `json:"start"`
	Finish Time `json:"finish"`
}

// TimetableEntry entry for update timetable
type TimetableEntry struct {
	DayOfWeek uint       `json:"dayOfWeek"`
	TimeSlots []TimeSlot `json:"timeSlots"`
}

// ScheduleRule rule for performing schedule update
type ScheduleRule struct {
	TTL       uint64           `json:"ttl"`
	Type      string           `json:"type"`
	Timetable []TimetableEntry `json:"timetable"`
}

// DecodedDesiredStatus decoded desired status
type DecodedDesiredStatus struct {
	BoardConfig       json.RawMessage
	Layers            []LayerInfoFromCloud
	Services          []ServiceInfoFromCloud
	Components        []ComponentInfoFromCloud
	FOTASchedule      ScheduleRule
	SOTASchedule      ScheduleRule
	CertificateChains []CertificateChain
	Certificates      []Certificate
}

// RenewCertData renew certificate data
type RenewCertData struct {
	Type      string    `json:"type"`
	Serial    string    `json:"serial"`
	ValidTill time.Time `json:"validTill"`
}

// RenewCertsNotification renew certificate notification from cloud
type RenewCertsNotification struct {
	Certificates   []RenewCertData `json:"certificates"`
	UnitSecureData []byte          `json:"unitSecureData"`
}

// RenewCertsNotificationWithPwd renew certificate notification from cloud with extracted pwd
type RenewCertsNotificationWithPwd struct {
	Certificates []RenewCertData `json:"certificates"`
	Password     string          `json:"password"`
}

// IssueCertData issue certificate data
type IssueCertData struct {
	Type string `json:"type"`
	Csr  string `json:"csr"`
}

// IssueUnitCerts issue unit certificates request
type IssueUnitCerts struct {
	Requests []IssueCertData `json:"requests"`
}

// IssuedCertData issued unit certificate data
type IssuedCertData struct {
	Type             string `json:"type"`
	CertificateChain string `json:"certificateChain"`
}

// IssuedUnitCerts issued unit certificates info
type IssuedUnitCerts struct {
	Certificates []IssuedCertData `json:"certificates"`
}

// InstallCertData install certificate data
type InstallCertData struct {
	Type        string `json:"type"`
	Serial      string `json:"serial"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

// InstallUnitCertsConfirmation install unit certificates confirmation
type InstallUnitCertsConfirmation struct {
	Certificates []InstallCertData `json:"certificates"`
}

// OverrideEnvVars request to override service environment variables
type OverrideEnvVars struct {
	OverrideEnvVars []byte `json:"overrideEnvVars"`
}

// DecodedOverrideEnvVars decoded service environment variables
type DecodedOverrideEnvVars struct {
	OverrideEnvVars []OverrideEnvsFromCloud `json:"overrideEnvVars"`
}

// OverrideEnvsFromCloud struct with envs and related service and user
type OverrideEnvsFromCloud struct {
	ServiceID string       `json:"serviceId"`
	SubjectID string       `json:"subjectId"`
	EnvVars   []EnvVarInfo `json:"envVars"`
}

// EnvVarInfo env info with id and time to live
type EnvVarInfo struct {
	ID       string     `json:"id"`
	Variable string     `json:"variable"`
	TTL      *time.Time `json:"TTL"`
}

// OverrideEnvVarsStatus override env status
type OverrideEnvVarsStatus struct {
	OverrideEnvVarsStatus []EnvVarInfoStatus `json:"overrideEnvVarsStatus"`
}

// EnvVarInfoStatus struct with envs status and related service and user
type EnvVarInfoStatus struct {
	ServiceID string         `json:"serviceId"`
	SubjectID string         `json:"subjectId"`
	Statuses  []EnvVarStatus `json:"statuses"`
}

// EnvVarStatus env status with error message
type EnvVarStatus struct {
	ID    string `json:"id"`
	Error string `json:"error,omitempty"`
}

// UnitSecret keeps unit secret used to decode secure device password
type UnitSecret struct {
	Version int `json:"version"`
	Data    struct {
		OwnerPassword string `json:"ownerPassword"`
	} `json:"data"`
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func (service ServiceInfoFromCloud) String() string {
	return fmt.Sprintf("{id: %s, vendorVersion: %s aosVersion: %d, description: %s}",
		service.ID, service.VendorVersion, service.AosVersion, service.Description)
}

func (layer LayerInfoFromCloud) String() string {
	return fmt.Sprintf("{id: %s, digest: %s, vendorVersion: %s aosVersion: %d, description: %s}",
		layer.ID, layer.Digest, layer.VendorVersion, layer.AosVersion, layer.Description)
}

func (component ComponentInfoFromCloud) String() string {
	return fmt.Sprintf("{id: %s, annotations: %s, vendorVersion: %s aosVersion: %d, description: %s}",
		component.ID, component.Annotations, component.VendorVersion, component.AosVersion, component.Description)
}

// MarshalJSON marshals JSON Time type
func (t Time) MarshalJSON() (b []byte, err error) {
	return json.Marshal(t.Format("15:04:05"))
}

// UnmarshalJSON unmarshals JSON Time type
func (t *Time) UnmarshalJSON(b []byte) (err error) {
	const errFormat = "invalid time value: %v"

	var v interface{}

	if err := json.Unmarshal(b, &v); err != nil {
		return aoserrors.Wrap(err)
	}

	switch value := v.(type) {
	case string:
		// Convert ISO 8601 to time.Time

		var strFields []string

		if strings.Contains(value, ":") {
			strFields = strings.Split(strings.TrimLeft(value, "T"), ":")
		} else {
			if !strings.HasPrefix(value, "T") {
				return aoserrors.Errorf(errFormat, value)
			}

			for i := 1; i < len(value); i = i + 2 {
				strFields = append(strFields, value[i:i+2])
			}
		}

		if len(strFields) == 0 {
			return aoserrors.Errorf(errFormat, value)
		}

		intFields := make([]int, 3)

		for i, field := range strFields {
			if intFields[i], err = strconv.Atoi(field); err != nil {
				return aoserrors.Errorf(errFormat, value)
			}
		}

		t.Time = time.Date(0, 1, 1, intFields[0], intFields[1], intFields[2], 0, time.Local)

		return nil

	default:
		return aoserrors.Errorf(errFormat, value)
	}
}
