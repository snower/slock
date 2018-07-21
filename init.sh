#!/bin/bash

BASEPATH=`pwd`
cd $BASEPATH

export GOPATH=$BASEPATH

cd src/slock

go get -v

cd ../../