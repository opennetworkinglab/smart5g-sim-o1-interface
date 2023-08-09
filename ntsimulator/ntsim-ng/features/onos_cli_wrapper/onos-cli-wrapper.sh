#!/bin/bash

# Usage: onos-cli-wrapper.sh <server_address> <cell enbid> <power dB>

onos ransim set cell $2 --tx-power $3 --service-address $1
