module github.com/aoscloud/aos_communicationmanager

go 1.14

replace github.com/ThalesIgnite/crypto11 => github.com/aoscloud/crypto11 v1.0.3-0.20220217163524-ddd0ace39e6f

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20211005130812-5bb3c17173e5
	github.com/aoscloud/aos_common v0.0.0-20220519144115-e4d62a88d016
	github.com/cavaliercoder/grab v2.0.0+incompatible
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f
	github.com/golang/protobuf v1.5.2
	github.com/google/go-tpm v0.3.2
	github.com/google/uuid v1.3.0
	github.com/looplab/fsm v0.3.0
	github.com/mattn/go-sqlite3 v1.14.9
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v1.0.0
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.28.0
)
