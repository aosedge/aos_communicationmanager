# Aos Communication Manager

[![CI](https://github.com/aoscloud/aos_communicationmanager/workflows/CI/badge.svg)](https://github.com/aoscloud/aos_communicationmanager/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/aoscloud/aos_communicationmanager/branch/main/graph/badge.svg?token=oTxsU7fc1y)](https://codecov.io/gh/aoscloud/aos_communicationmanager)

Aos Communication Manager (CM) is a part of Aos system which responsible of the following tasks:

* communicate with the backend;
* download, verify and decrypt services, layers and component updates;
* monitor system resource usage;
* serve Update Managers (UM's) and Service Managers (SM's).

See architecture [document](doc/architecture.md) for more details.

## Build

### Required GO packages

All requires GO packages exist under `vendor` folder. Content of this folder is created with GO modules:


```bash
go mod init
go mod vendor
```

### Native build

```bash
go build
```

### ARM 64 build

Install arm64 toolchain:

```bash
sudo apt install gcc-aarch64-linux-gnu
```

Build:

```bash
CC=aarch64-linux-gnu-gcc CGO_ENABLED=1 GOOS=linux GOARCH=arm64 go build
```

## Configuration

CM is configured through a configuration file. The file `aos_communicationmanager.cfg` should be either in current directory or specified with command line option as following:

```bash
./aos_communicationmanager -c aos_communicationmanager.cfg
```

The configuration file has JSON format described [here] ([doc/config.md](https://kb.epam.com/display/EPMDAEPRA/Communication+Manager+Configuration)). Example configuration file could be found in `aos_communication.cfg`

To increase log level use option -v:

```bash
./aos_communicationmanager -c aos_communicationmanager.cfg -v debug
```

## Run

## Required packages

CM needs Aos Identity and Access Manager (IAM) to be running and configured (see aos_iamanager [readme](https://github.com/aoscloud/aos_iamanager/blob/main/README.md)) before start.

## Test required packages

* [libssl-dev]  - headers for TPM simulator
* [rabbitmq-server] - AMQP server
* [pyftpdlib] - light python ftp server

## Test

Test all packages:

```bash
sudo -E go test ./... -v
```
