#!/bin/bash -e

# Copyright 2016 HTC Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MASTER_IP=$2
SOLVER=${SOLVER:-caffe/examples/speedo/solver.prototxt}

# Prints the usage for this script and exits.
function print_usage() {
cat <<EOF
      Usage:

      For master:
      	docker run -d --name=<container_name> --net=host obdg/speedo master <master_ip> <num_workers> [strategy]
      For slave:
        docker run -d --name=<container_name> --net=host obdg/speedo master <master_ip> <worker_ip>

      strategy(optinal):
        	"--sync"  for  sync strategy
        	"--maxAdvance n"  for  psc strategy
        	"--drop n"   for  weed-out strategy
        	"--movingRate v"  for  easgd  strategy

      EXAMPLES:

      run master actor (in default Async model with 3 workers):
        docker run -d --name=speedo-master --net=host obdg/speedo
      run master actor in Easgd model with 3 workers:
        docker run -d --name=speedo-master --net=host obdg/speedo master localhost 3 --test 500 --maxIter 1000 --movingRate 0.5
      
      run worker actors:
        docker run -d --name=speedo-worker --net=host obdg/speedo worker master_ip worker_ip
EOF
exit 1
}

if [[ $1 == "master" ]]; then
  redis-server --daemonize yes
  NUM_WORKERS=${3:-3}
  shift
  shift
  shift
  java -cp SpeeDO-akka-1.0.jar -Xmx2G com.htc.speedo.akka.AkkaUtil --solver ${SOLVER} --worker ${NUM_WORKERS} --redis ${MASTER_IP} --host ${HOST_IP} --port 56126 $@ 2> /dev/null
elif [[ $1 == "worker" ]]; then
  WORKER_IP=$3
  java -cp SpeeDO-akka-1.0.jar -Xmx2G com.htc.speedo.akka.AkkaUtil --host ${WORKER_IP} --master akka.tcp://SpeeDO@${MASTER_IP}:56126/user/host 2> /dev/null
else
	print_usage
fi
