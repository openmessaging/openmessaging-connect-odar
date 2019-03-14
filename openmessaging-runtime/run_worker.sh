#!/bin/bash
export OMS_RMQ_DIRECT_NAME_SRV=true
echo "run rumtime worker"
cd target/ConnectStartup/ConnectStartup/ && java -cp .:./conf/:./lib/* io.openmessaging.connect.runtime.ConnectStartup -c conf/connect.conf

