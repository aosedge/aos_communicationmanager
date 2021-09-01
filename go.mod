module aos_communicationmanager

go 1.14

replace github.com/ThalesIgnite/crypto11 => github.com/xen-troops/crypto11 v1.2.5-0.20210607075540-0b6da74b5450

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20210608160410-67692ebc98de
	github.com/Microsoft/hcsshim v0.8.22 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/ThalesIgnite/crypto11 v1.2.4
	github.com/cavaliercoder/grab v2.0.0+incompatible
	github.com/containerd/containerd v1.4.9 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf
	github.com/coreos/go-systemd/v22 v22.3.2
	github.com/docker/docker v17.12.0-ce-rc1.0.20200618181300-9dc6525e6118+incompatible // indirect
	github.com/golang/protobuf v1.5.2
	github.com/google/go-tpm v0.3.2
	github.com/google/uuid v1.3.0
	github.com/looplab/fsm v0.3.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/opencontainers/runtime-spec v1.0.3-0.20200929063507-e6143ca7d51d // indirect
	github.com/shirou/gopsutil v3.21.8+incompatible
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v1.0.0
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	gitpct.epam.com/epmd-aepr/aos_common v0.0.0-20210928130140-41cdb2842108
	google.golang.org/grpc v1.33.2
	google.golang.org/protobuf v1.26.0
)
