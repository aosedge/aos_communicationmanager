# AOS Communication Manager

[![pipeline status](https://gitpct.epam.com/epmd-aepr/aos_communicationmanager/badges/master/pipeline.svg)](https://gitpct.epam.com/epmd-aepr/aos_communicationmanager/commits/master)
[![coverage report](https://gitpct.epam.com/epmd-aepr/aos_communicationmanager/badges/master/coverage.svg)](https://gitpct.epam.com/epmd-aepr/aos_communicationmanager/commits/master)  

AOS Communication Manager (CM) is a part of AoS system which responsible of the following tasks:

* communicate with the backend;
* download, verify and decrypt services, layers and component updates;
* monitor system resource usage;
* serve Update Managers (UM's) and Service Managers (SM's).

See architecture [document](doc/architecture.md) for more details.

## Build

### Required GO packages

All requires GO packages exist under `vendor` folder. Content of this folder is created with GO modules:

```bash
export GOPRIVATE=gitpct.epam.com/*
```

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

CM needs AoS Identity and Access Manager (IAM) to be running and configured (see aos_iamanager [readme](https://gitpct.epam.com/epmd-aepr/aos_iamanager/blob/master/README.md)) before start.

## Test required packages

* [libssl-dev]  - headers for TPM simulator
* [rabbitmq-server] - AMQP server
* [pyftpdlib] - light python ftp server

## Test

Install all necessary dependencies:

```bash
./ci/setup_env.sh
```

Test all packages:

```bash
sudo -E go test ./... -v
```
