# SpeeDo - Parallelizing Stochastic Gradient Descent for Deep Convolutional Neural Network

## Introduction

Convolutional Neural Networks (CNNs) have achieved breakthrough results on many machine learning tasks. However, training CNNs is computationally intensive. When the size of training data is large and the depth of CNNs is high, as typically required for attaining high classification accuracy, training a model can take days and even weeks. So we propose [SpeeDO](http://learningsys.org/papers/LearningSys_2015_paper_13.pdf) (for Open DEEP learning System in backward order), a deep learning system designed for off-the-shelf hardwares. SpeeDO can be easily deployed, scaled and maintained in a cloud environment, such as AWS EC2 cloud, Google GCE, and Microsoft Azure.

In our implement, we support 5 distributed SGD models to speed up the training:

* Synchronous SGD
* Asynchronous SGD
* Partially Synchronous SGD
* Weed-Out SGD
* Elastic Averaging SGD

Please cite [SpeeDO](http://learningsys.org/papers/LearningSys_2015_paper_13.pdf) in your publications if it helps your research:

    @article{zhengspeedo,
      title={SpeeDO: Parallelizing Stochastic Gradient Descent for Deep Convolutional Neural Network},
      author={Zheng, Zhongyang and Jiang, Wenrui and Wu, Gang and Chang, Edward Y}
    }

## Architecture

SpeeDO takes advantage of many existing solutions in the open-source community, data flow of SpeeDO:

![Architecture and data flow of SpeeDO](https://raw.githubusercontent.com/obdg/speedo/master/data/figures/speedo_architecture.png "Architecture and data flow of SpeeDO")

SpeeDO mainly contains these components:

* [Caffe](http://caffe.berkeleyvision.org/) (required)
* [Redis](http://redis.io) (required)
* [Akka](http://akka.io)  (required)
* [Yarn](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)  [optional]
* [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)  [optional]

These components denote what we need to deploy before the distributed training.

# Deploy and Run

## Pre-requisite

* JDK 1.7+
* [Redis Server](http://redis.io/download)
* Ubuntu 12.04+ (not test on other linux os family)
* Network connection: You need to connect to maven and ivy repositories when compiling the demo

SpeeDO is running in **Master-Slaves(Worker)** archiecture. To avoid manual process in running, and distribute input data in Master and worker nodes, we can use YARN and HDFS. In below, we provides the instruction to run SpeeDO for both scenarios.

| Configuration | YARN present | HDFS present |
| ------------- | ------------- | ------------- |
| A | N | N |
| B | Y | Y |

**NOTE**

i. YARN is used for nodes resource scheduling. If YARN is not present, we can run our Master , and Worker process manually.

ii. HDFS is used for storing training data and network definition of caffe. If HDFS is not present, we can use shared-files system (like NFS ) or manually copying these files to each nodes.

We provide the steps to run configuration **A** and **B** here:
* For configuration **A**, we manually deploy and run on all nodes.
* For configuration **B**, we use [cloudera](http://www.cloudera.com/documentation/manager/5-1-x/Cloudera-Manager-Installation-Guide/Cloudera-Manager-Installation-Guide.html) (offering us both YARN and HDFS) to deploy and run SpeeDO.

## A. Deploy and run SpeeDO without YARN and HDFS

We provides TWO methods here: 1) Docker , 2) Manual ( step by step)

##1. Quick Start ( via Docker )

### Step.0 Pull image
Pull the speedo image ( bundled with caffe and all its dependencies libraries):
```bash
docker pull obdg/speedo:latest
```

### Step.1 Run containers on cluster
The following example will run 1000 iterations asynchronously using 1 Master with 3 workers ( 4 cluster nodes )

#### Master
Launch master container on your master node (in default Async model with 3 workers):
```bash
docker run -d --name=speedo-master --net=host obdg/speedo
```

**Or** run master actor in Easgd model with 3 workers
```bash
docker run -d --name=speedo-master --net=host obdg/speedo master <master-address> 3 --test 0 --maxIter 1000 --movingRate 0.5
```

Please replaces `master-address` with master node's ip

**NOTE**
Redis service will be started automatically when launching master container

#### Worker
Launch 3 worker containers on different worker nodes:
```bash
docker run -d --name=speedo-worker --net=host obdg/speedo worker <master-address> <worker-address>
```

Please replaces `master-address` with master node's ip, and `worker-address` with the current worker node's ip

##2. Manually ( Step by Step )

### Step.0 Pre-requistie
Install at each nodes ( Master and Worker)
1. JDK 1.7+
2. Redis Server
3. Clone SpeeDO and Caffe source from our github repo

Please use
```
git clone --recursive git@github.com/obdg/speedo.git # SpeeDO and caffe
```

### Step.1 Install caffe and its dependencies
Install [speedo/caffe](https://github.com/obdg/caffe) and all its dependencies on each nodes , please refer to section **A. Manually install on all cluster nodes** from [speedo/caffe install guide](https://github.com/obdg/caffe).

### Step.2 Prepare Input Data to run under Caffe

**NOTE**: We prefer to use **datumfile** format for SpeeDO ( see [caffe-pullrequest-2193](https://github.com/BVLC/caffe/pull/2193) ) instead of the default leveldb/lmdb format during training in Caffe to solve the memory usage problem ( refer to [caffe-issues-1377](https://github.com/BVLC/caffe/issues/1377)).

The input data required by Caffe, including:
* solver definition
* network definition
* training datasets
* testing datasets
* mean values

In this example, let's train cifar10 dataset and generate `training datasets` and `testing datasets` in dataumfile format:
```bash
cd caffe
./data/cifar10/get_cifar10.sh # download cifar dataset
./examples/speedo/create_cifar10.sh  # create protobuf file - in datumfile instead of leveldb/lmdb format
```

`Solver definition`, `network definition` and `means values` written in datumfile format for cifar10 is provided at examples/speedo.

>  If you want to manually produce these files, please follow the steps below. (Modify all paths in network definitions if needed ) :
```bash
sed -i "s/examples\/cifar10\/mean.binaryproto/mean.binaryproto/g" cifar10_full_train_test.prototxt
sed -i "s/examples\/cifar10\/cifar10_train_lmdb/cifar10_train_datumfile/g" cifar10_full_train_test.prototxt
sed -i "s/examples\/cifar10\/cifar10_test_lmdb/cifar10_test_datumfile/g" cifar10_full_train_test.prototxt
sed -i "s/backend: LMDB/backend: DATUMFILE/g" cifar10_full_train_test.prototxt
sed -i "17i\    rand_skip: 50000" cifar10_full_train_test.prototxt
sed -i "s/examples\/cifar10\/cifar10_full_train_test.prototxt/cifar10_full_train_test.prototxt/g" cifar10_full_solver.prototxt
```

At last, put the data in the same location(like /tmp/caffe/cifar10) on **all Master and Workers node**. You can do that by [Ansible](https://www.ansible.com/) or just scp to the right location.

### Step.3  Training under SpeeDO

SpeeDO use Master + Worker archiecture for the distributed training (Please refer to our paper for the detail information). We need to start Master node and Worker node as below.

#### Compile bundle jar
On each master and worker nodes, run
```bash
git clone git@github.com/obdg/speedo.git # if not done yet
cd speedo
./sbt akka:assembly
```

#### Run Master and Worker process

The following example will run 1000 iterations asynchronously using 1 Master with 3 workers ( 4 cluster nodes ).

##### Master
Launch master process on your master node:
```bash
JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/usr/lib java -cp target/scala-2.11/SpeeDO-akka-1.0.jar -Xmx2G com.htc.speedo.akka.AkkaUtil --solver /absolute_path/to/cifar10_full_solver.prototxt --worker 3 --redis <redis-address> --test 500 --maxIter 1000 --host <master-address> 2> /dev/null
```

Please replaces `redis-address` with the redis server location, and `master-address` with master node's ip/hostname.

This should output some thing like:

    [INFO] [03/03/2016 15:07:41.626] [main] [Remoting] Starting remoting
    [INFO] [03/03/2016 15:07:41.761] [main] [Remoting] Remoting started; listening on addresses :[akka.tcp://SpeeDO@cloud-master:56126]
    [INFO] [03/03/2016 15:07:41.763] [main] [Remoting] Remoting now listens on addresses: [akka.tcp://SpeeDO@cloud-master:56126]
    [INFO] [03/03/2016 15:07:41.777] [SpeeDO-akka.actor.default-dispatcher-3] [akka.tcp://SpeeDO@cloud-master:56126/user/host] Waiting for 3 workers to join.

##### Worker
Launch 3 workers process on worker nodes:
```bash
JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/usr/lib java -cp target/scala-2.11/SpeeDO-akka-1.0.jar -Xmx2G com.htc.speedo.akka.AkkaUtil --host <worker-address> --master <masteractor-addr> 2> /dev/null
```

Please replaces `worker-address` with worker's ip/hostname,  and `masteractor-addr` with master actor address.

The format of master actor address is **`akka.tcp://SpeeDO@cloud-master:56126/user/host`**, where cloud-master is the hostname of master node, and 56126 is the TCP port listen by akka's actor. Since the port is random by default, the address can vary in different runs. You can also use fixed port by passing a `--port <port>` command line argument when start Master.



## B. Deploy and run SpeeDO by cloudera
To try a cloudera solution for SpeeDO. Please refer [Run SpeeDO on Yarn & HDFS Cluster](https://github.com/obdg/speedo/blob/master/README_YARN.md)

## Experiments on AWS

The Cifar10 dataset is used to validate all parallel implementations on a CPU cluster with four 8-core instances

![SGD parallel schemes on CPU cluster](https://raw.githubusercontent.com/obdg/speedo/master/data/figures/speedo_psgd_cpu.png "SGD parallel schemes on CPU cluster")

Training [GoogleNet](http://www.cs.unc.edu/~wliu/papers/GoogLeNet.pdf) on a GPU cluster for different parallel implementations

![SGD parallel schemes on GPU cluster](https://raw.githubusercontent.com/obdg/speedo/master/data/figures/speedo_psgd_gpu.png "SGD parallel schemes on GPU cluster")

EASGD achieves the best speedup in our parallel implementations. And parameters of it have great impact for the speedup.

![Parameter Analysis of EASGD on GPU Cluster](https://raw.githubusercontent.com/obdg/speedo/master/data/figures/speedo_easgd_gpu.png "Parameter Analysis of EASGD on GPU Cluster")

## Authors

* [Zhongyang Zheng](https://github.com/zyzheng)
* [Wenrui Jiang](https://github.com/wenruij)
* [Gang Wu](https://github.com/simonandluna)

## Supervisor
* [Edward Y. Chang](http://infolab.stanford.edu/~echang/)

## License

Copyright 2016 HTC Corporation

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
