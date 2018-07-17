#!/bin/bash

BASEPATH=`pwd`
cd $BASEPATH

export GOPATH=$BASEPATH

go clean
go build
