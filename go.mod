module github.com/aoscloud/aos_communicationmanager

go 1.14

replace github.com/ThalesIgnite/crypto11 => github.com/xen-troops/crypto11 v1.2.5-0.20210607075540-0b6da74b5450

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20211005130812-5bb3c17173e5
	github.com/ThalesIgnite/crypto11 v1.2.4
	github.com/aoscloud/aos_common v0.0.0-20211220123623-d23b9d38e838
	github.com/cavaliercoder/grab v2.0.0+incompatible
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf
	github.com/coreos/go-systemd/v22 v22.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/go-tpm v0.3.2
	github.com/google/uuid v1.3.0
	github.com/looplab/fsm v0.3.0
	github.com/mattn/go-sqlite3 v1.14.9
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v1.0.0
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)
