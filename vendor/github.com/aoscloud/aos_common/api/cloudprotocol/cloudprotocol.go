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
	"time"

	"github.com/aoscloud/aos_common/aostypes"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

// ProtocolVersion specifies supported protocol version.
const ProtocolVersion = 4

// UnitSecretVersion specifies supported version of UnitSecret message.
const UnitSecretVersion = 2

// Cloud message types.
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

// Device message types.
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

// Alert tags.
const (
	AlertTagSystemError      = "systemAlert"
	AlertTagAosCore          = "coreAlert"
	AlertTagResourceValidate = "resourceValidateAlert"
	AlertTagDeviceAllocate   = "deviceAllocateAlert"
	AlertTagSystemQuota      = "systemQuotaAlert"
	AlertTagInstanceQuota    = "instanceQuotaAlert"
	AlertTagDownloadProgress = "downloadProgressAlert"
	AlertTagServiceInstance  = "serviceInstanceAlert"
)

// Unit statuses.
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

// SOTA/FOTA schedule type.
const (
	ForceUpdate     = "force"
	TriggerUpdate   = "trigger"
	TimetableUpdate = "timetable"
)

// Service instance states.
const (
	InstanceStateActive = "active"
	InstanceStateFailed = "failed"
)

// Download target types.
const (
	DownloadTargetComponent = "component"
	DownloadTargetLayer     = "layer"
	DownloadTargetService   = "service"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Message structure for AOS messages.
type Message struct {
	Header MessageHeader `json:"header"`
	Data   interface{}   `json:"data"`
}

// MessageHeader message header.
type MessageHeader struct {
	Version     uint64 `json:"version"`
	SystemID    string `json:"systemId"`
	MessageType string `json:"messageType"`
}

// ServiceDiscoveryRequest service discovery request.
type ServiceDiscoveryRequest struct{}

// ServiceDiscoveryResponse service discovery response.
type ServiceDiscoveryResponse struct {
	Version    uint64         `json:"version"`
	Connection ConnectionInfo `json:"connection"`
}

// ConnectionInfo AMQP connection info.
type ConnectionInfo struct {
	SendParams    SendParams    `json:"sendParams"`
	ReceiveParams ReceiveParams `json:"receiveParams"`
}

// SendParams AMQP send parameters.
type SendParams struct {
	Host      string         `json:"host"`
	User      string         `json:"user"`
	Password  string         `json:"password"`
	Mandatory bool           `json:"mandatory"`
	Immediate bool           `json:"immediate"`
	Exchange  ExchangeParams `json:"exchange"`
}

// ExchangeParams AMQP exchange parameters.
type ExchangeParams struct {
	Name       string `json:"name"`
	Durable    bool   `json:"durable"`
	AutoDetect bool   `json:"autoDetect"`
	Internal   bool   `json:"internal"`
	NoWait     bool   `json:"noWait"`
}

// ReceiveParams AMQP receive parameters.
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

// QueueInfo AMQP queue info.
type QueueInfo struct {
	Name             string `json:"name"`
	Durable          bool   `json:"durable"`
	DeleteWhenUnused bool   `json:"deleteWhenUnused"`
	Exclusive        bool   `json:"exclusive"`
	NoWait           bool   `json:"noWait"`
}

// DesiredStatus desired status message.
type DesiredStatus struct {
	BoardConfig       []byte             `json:"boardConfig"`
	Services          []byte             `json:"services"`
	Layers            []byte             `json:"layers"`
	Instances         []byte             `json:"instances"`
	Components        []byte             `json:"components"`
	FOTASchedule      []byte             `json:"fotaSchedule"`
	SOTASchedule      []byte             `json:"sotaSchedule"`
	CertificateChains []CertificateChain `json:"certificateChains,omitempty"`
	Certificates      []Certificate      `json:"certificates,omitempty"`
}

// InstanceFilter instance filter structure.
type InstanceFilter struct {
	ServiceID string  `json:"serviceId"`
	SubjectID *string `json:"subjectId,omitempty"`
	Instance  *uint64 `json:"instance,omitempty"`
}

// InstanceIdent instance identification information.
type InstanceIdent struct {
	ServiceID string `json:"serviceId"`
	SubjectID string `json:"subjectId"`
	Instance  uint64 `json:"instance"`
}

// RequestServiceCrashLog request service crash log message.
type RequestServiceCrashLog struct {
	InstanceFilter
	LogID string     `json:"logId"`
	From  *time.Time `json:"from"`
	Till  *time.Time `json:"till"`
}

// RequestServiceLog request service log message.
type RequestServiceLog struct {
	InstanceFilter
	LogID string     `json:"logId"`
	From  *time.Time `json:"from"`
	Till  *time.Time `json:"till"`
}

// RequestSystemLog request system log message.
type RequestSystemLog struct {
	LogID string     `json:"logId"`
	From  *time.Time `json:"from"`
	Till  *time.Time `json:"till"`
}

// DecryptionInfo update decryption info.
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

// Signs message signature.
type Signs struct {
	ChainName        string   `json:"chainName"`
	Alg              string   `json:"alg"`
	Value            []byte   `json:"value"`
	TrustedTimestamp string   `json:"trustedTimestamp"`
	OcspValues       []string `json:"ocspValues"`
}

// CertificateChain  certificate chain.
type CertificateChain struct {
	Name         string   `json:"name"`
	Fingerprints []string `json:"fingerprints"`
}

// Certificate certificate structure.
type Certificate struct {
	Fingerprint string `json:"fingerprint"`
	Certificate []byte `json:"certificate"`
}

// DecryptDataStruct struct contains how to decrypt data.
type DecryptDataStruct struct {
	URLs           []string        `json:"urls"`
	Sha256         []byte          `json:"sha256"`
	Sha512         []byte          `json:"sha512"`
	Size           uint64          `json:"size"`
	DecryptionInfo *DecryptionInfo `json:"decryptionInfo,omitempty"`
	Signs          *Signs          `json:"signs,omitempty"`
}

// StateAcceptance state acceptance message.
type StateAcceptance struct {
	InstanceIdent
	Checksum string `json:"checksum"`
	Result   string `json:"result"`
	Reason   string `json:"reason"`
}

// UpdateState state update message.
type UpdateState struct {
	InstanceIdent
	Checksum string `json:"stateChecksum"`
	State    string `json:"state"`
}

// NewState new state structure.
type NewState struct {
	InstanceIdent
	Checksum string `json:"stateChecksum"`
	State    string `json:"state"`
}

// StateRequest state request structure.
type StateRequest struct {
	InstanceIdent
	Default bool `json:"default"`
}

// SystemAlert system alert structure.
type SystemAlert struct {
	Message string `json:"message"`
}

// CoreAlert system alert structure.
type CoreAlert struct {
	CoreComponent string `json:"coreComponent"`
	Message       string `json:"message"`
}

// DownloadAlert download alert structure.
type DownloadAlert struct {
	TargetType          string `json:"targetType"`
	TargetID            string `json:"targetId"`
	TargetAosVersion    uint64 `json:"targetAosVersion"`
	TargetVendorVersion string `json:"targetVendorVersion"`
	Message             string `json:"message"`
	Progress            string `json:"progress"`
	URL                 string `json:"url"`
	DownloadedBytes     string `json:"downloadedBytes"`
	TotalBytes          string `json:"totalBytes"`
}

// SystemQuotaAlert system quota alert structure.
type SystemQuotaAlert struct {
	Parameter string `json:"parameter"`
	Value     uint64 `json:"value"`
}

// InstanceQuotaAlert instance quota alert structure.
type InstanceQuotaAlert struct {
	InstanceIdent
	Parameter string `json:"parameter"`
	Value     uint64 `json:"value"`
}

// DeviceAllocateAlert device allocate alert structure.
type DeviceAllocateAlert struct {
	InstanceIdent
	Device  string `json:"device"`
	Message string `json:"message"`
}

// ResourceValidateError resource validate error structure.
type ResourceValidateError struct {
	Name   string   `json:"name"`
	Errors []string `json:"error"`
}

// ResourceValidateAlert resource validate alert structure.
type ResourceValidateAlert struct {
	ResourcesErrors []ResourceValidateError `json:"resourcesErrors"`
}

// ServiceInstanceAlert system alert structure.
type ServiceInstanceAlert struct {
	InstanceIdent
	AosVersion uint64 `json:"aosVersion"`
	Message    string `json:"message"`
}

// AlertItem alert item structure.
type AlertItem struct {
	Timestamp time.Time   `json:"timestamp"`
	Tag       string      `json:"tag"`
	Payload   interface{} `json:"payload"`
}

// Alerts alerts message structure.
type Alerts []AlertItem

// GlobalMonitoringData global monitoring data for service.
type GlobalMonitoringData struct {
	RAM        uint64 `json:"ram"`
	CPU        uint64 `json:"cpu"`
	UsedDisk   uint64 `json:"usedDisk"`
	InTraffic  uint64 `json:"inTraffic"`
	OutTraffic uint64 `json:"outTraffic"`
}

// InstanceMonitoringData monitoring data for service.
type InstanceMonitoringData struct {
	InstanceIdent
	RAM        uint64 `json:"ram"`
	CPU        uint64 `json:"cpu"`
	UsedDisk   uint64 `json:"usedDisk"`
	InTraffic  uint64 `json:"inTraffic"`
	OutTraffic uint64 `json:"outTraffic"`
}

// MonitoringData monitoring data structure.
type MonitoringData struct {
	Timestamp        time.Time                `json:"timestamp"`
	Global           GlobalMonitoringData     `json:"global"`
	ServiceInstances []InstanceMonitoringData `json:"serviceInstances"`
}

// PushLog push service log structure.
type PushLog struct {
	LogID     string `json:"logId"`
	PartCount uint64 `json:"partCount,omitempty"`
	Part      uint64 `json:"part,omitempty"`
	Data      []byte `json:"data,omitempty"`
	Error     string `json:"error,omitempty"`
}

// UnitStatus unit status structure.
type UnitStatus struct {
	BoardConfig  []BoardConfigStatus `json:"boardConfig"`
	Services     []ServiceStatus     `json:"services"`
	Layers       []LayerStatus       `json:"layers,omitempty"`
	Components   []ComponentStatus   `json:"components"`
	Instances    []InstanceStatus    `json:"instances"`
	UnitSubjects []string            `json:"unitSubjects"`
}

// ErrorInfo error information.
type ErrorInfo struct {
	AosCode  int    `json:"aosCode"`
	ExitCode int    `json:"exitCode"`
	Message  string `json:"message,omitempty"`
}

// InstanceStatus service instance runtime status.
type InstanceStatus struct {
	InstanceIdent
	AosVersion    uint64     `json:"aosVersion"`
	StateChecksum string     `json:"stateChecksum,omitempty"`
	RunState      string     `json:"runState"`
	ErrorInfo     *ErrorInfo `json:"errorInfo,omitempty"`
}

// BoardConfigStatus board config status.
type BoardConfigStatus struct {
	VendorVersion string     `json:"vendorVersion"`
	Status        string     `json:"status"`
	ErrorInfo     *ErrorInfo `json:"errorInfo,omitempty"`
}

// ServiceStatus service status.
type ServiceStatus struct {
	ID         string     `json:"id"`
	AosVersion uint64     `json:"aosVersion"`
	Status     string     `json:"status"`
	ErrorInfo  *ErrorInfo `json:"errorInfo,omitempty"`
}

// LayerStatus layer status.
type LayerStatus struct {
	ID         string     `json:"id"`
	AosVersion uint64     `json:"aosVersion"`
	Digest     string     `json:"digest"`
	Status     string     `json:"status"`
	ErrorInfo  *ErrorInfo `json:"errorInfo,omitempty"`
}

// ComponentStatus component status.
type ComponentStatus struct {
	ID            string     `json:"id"`
	AosVersion    uint64     `json:"aosVersion"`
	VendorVersion string     `json:"vendorVersion"`
	Status        string     `json:"status"`
	ErrorInfo     *ErrorInfo `json:"errorInfo,omitempty"`
}

// VersionInfo common version structure.
type VersionInfo struct {
	AosVersion    uint64 `json:"aosVersion"`
	VendorVersion string `json:"vendorVersion"`
	Description   string `json:"description"`
}

// ServiceInfo decrypted service info.
type ServiceInfo struct {
	VersionInfo
	ID         string `json:"id"`
	ProviderID string `json:"providerId"`
	DecryptDataStruct
}

// LayerInfo decrypted layer info.
type LayerInfo struct {
	VersionInfo
	ID     string `json:"id"`
	Digest string `json:"digest"`
	DecryptDataStruct
}

// ComponentInfo decrypted component info.
type ComponentInfo struct {
	VersionInfo
	ID          string          `json:"id"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
	DecryptDataStruct
}

// InstanceInfo decrypted desired instance runtime info.
type InstanceInfo struct {
	ServiceID    string `json:"serviceId"`
	SubjectID    string `json:"subjectId"`
	NumInstances uint64 `json:"numInstances"`
}

// TimeSlot time slot with start and finish time.
type TimeSlot struct {
	Start  aostypes.Time `json:"start"`
	Finish aostypes.Time `json:"finish"`
}

// TimetableEntry entry for update timetable.
type TimetableEntry struct {
	DayOfWeek uint       `json:"dayOfWeek"`
	TimeSlots []TimeSlot `json:"timeSlots"`
}

// ScheduleRule rule for performing schedule update.
type ScheduleRule struct {
	TTL       uint64           `json:"ttl"`
	Type      string           `json:"type"`
	Timetable []TimetableEntry `json:"timetable"`
}

// DecodedDesiredStatus decoded desired status.
type DecodedDesiredStatus struct {
	BoardConfig       json.RawMessage
	Components        []ComponentInfo
	Layers            []LayerInfo
	Services          []ServiceInfo
	Instances         []InstanceInfo
	FOTASchedule      ScheduleRule
	SOTASchedule      ScheduleRule
	CertificateChains []CertificateChain
	Certificates      []Certificate
}

// RenewCertData renew certificate data.
type RenewCertData struct {
	Type      string    `json:"type"`
	Serial    string    `json:"serial"`
	ValidTill time.Time `json:"validTill"`
}

// RenewCertsNotification renew certificate notification from cloud.
type RenewCertsNotification struct {
	Certificates   []RenewCertData `json:"certificates"`
	UnitSecureData []byte          `json:"unitSecureData"`
}

// RenewCertsNotificationWithPwd renew certificate notification from cloud with extracted pwd.
type RenewCertsNotificationWithPwd struct {
	Certificates []RenewCertData `json:"certificates"`
	Password     string          `json:"password"`
}

// IssueCertData issue certificate data.
type IssueCertData struct {
	Type string `json:"type"`
	Csr  string `json:"csr"`
}

// IssueUnitCerts issue unit certificates request.
type IssueUnitCerts struct {
	Requests []IssueCertData `json:"requests"`
}

// IssuedCertData issued unit certificate data.
type IssuedCertData struct {
	Type             string `json:"type"`
	CertificateChain string `json:"certificateChain"`
}

// IssuedUnitCerts issued unit certificates info.
type IssuedUnitCerts struct {
	Certificates []IssuedCertData `json:"certificates"`
}

// InstallCertData install certificate data.
type InstallCertData struct {
	Type        string `json:"type"`
	Serial      string `json:"serial"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

// InstallUnitCertsConfirmation install unit certificates confirmation.
type InstallUnitCertsConfirmation struct {
	Certificates []InstallCertData `json:"certificates"`
}

// OverrideEnvVars request to override service environment variables.
type OverrideEnvVars struct {
	OverrideEnvVars []byte `json:"overrideEnvVars"`
}

// DecodedOverrideEnvVars decoded service environment variables.
type DecodedOverrideEnvVars struct {
	OverrideEnvVars []EnvVarsInstanceInfo `json:"overrideEnvVars"`
}

// EnvVarsInstanceInfo struct with envs and related service and user.
type EnvVarsInstanceInfo struct {
	InstanceFilter
	EnvVars []EnvVarInfo `json:"envVars"`
}

// EnvVarInfo env info with id and time to live.
type EnvVarInfo struct {
	ID       string     `json:"id"`
	Variable string     `json:"variable"`
	TTL      *time.Time `json:"ttl"`
}

// OverrideEnvVarsStatus override env status.
type OverrideEnvVarsStatus struct {
	OverrideEnvVarsStatus []EnvVarsInstanceStatus `json:"overrideEnvVarsStatus"`
}

// EnvVarsInstanceStatus struct with envs status and related service and user.
type EnvVarsInstanceStatus struct {
	InstanceFilter
	Statuses []EnvVarStatus `json:"statuses"`
}

// EnvVarStatus env status with error message.
type EnvVarStatus struct {
	ID    string `json:"id"`
	Error string `json:"error,omitempty"`
}

// UnitSecret keeps unit secret used to decode secure device password.
type UnitSecret struct {
	Version int `json:"version"`
	Data    struct {
		OwnerPassword string `json:"ownerPassword"`
	} `json:"data"`
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func (service ServiceInfo) String() string {
	return fmt.Sprintf("{id: %s, vendorVersion: %s aosVersion: %d, description: %s}",
		service.ID, service.VendorVersion, service.AosVersion, service.Description)
}

func (layer LayerInfo) String() string {
	return fmt.Sprintf("{id: %s, digest: %s, vendorVersion: %s aosVersion: %d, description: %s}",
		layer.ID, layer.Digest, layer.VendorVersion, layer.AosVersion, layer.Description)
}

func (component ComponentInfo) String() string {
	return fmt.Sprintf("{id: %s, annotations: %s, vendorVersion: %s aosVersion: %d, description: %s}",
		component.ID, component.Annotations, component.VendorVersion, component.AosVersion, component.Description)
}

func NewInstanceFilter(serviceID, subjectID string, instance int64) (filter InstanceFilter) {
	filter.ServiceID = serviceID

	if subjectID != "" {
		filter.SubjectID = &subjectID
	}

	if instance != -1 {
		localInstance := (uint64)(instance)

		filter.Instance = &localInstance
	}

	return filter
}
