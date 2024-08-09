#!/bin/bash
apt update
# Install wget
apt install -y wget

# Install unzip
apt install -y unzip

mkdir -p /opt/oracle
cd /opt/oracle
wget https://download.oracle.com/otn_software/linux/instantclient/19800/instantclient-basic-linux.x64-19.8.0.0.0dbru.zip
unzip instantclient-basic-linux.x64-19.8.0.0.0dbru.zip
apt install libaio1
sh -c "echo /opt/oracle/instantclient_19_8 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_8:$LD_LIBRARY_PATH