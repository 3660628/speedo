# Run SpeeDO on Yarn and HDFS with Cloudera

## Step.1 Install Cloudera on cluster
Install Cloudera Manager
```bash
wget https://archive.cloudera.com/cm5/installer/latest/cloudera-manager-installer.bin
chmod u+x cloudera-manager-installer.bin && sudo ./cloudera-manager-installer.bin
```

Any issues when installation, please check [Cloudera Manager Installation Guide](http://www.cloudera.com/documentation/manager/5-1-x/Cloudera-Manager-Installation-Guide/Cloudera-Manager-Installation-Guide.html)

## Step.2 Install caffe and its dependencies
Install [speedo/caffe](https://github.com/obdg/caffe) and all its dependencies, see section **B. Automatic deployment by cloudera parcels** from [speedo/caffe install guide](https://github.com/obdg/caffe).

## Step.3 Upload training datasets and network definitions to HDFS

Follow the same step in section **Step.2 Prepare Input Data for each nodes** of [Deploy and run SpeeDO manually without cloudera](https://github.com/obdg/speedo), the only difference is we do not need to preapre datasets on all hosts, we only upload them onto hdfs:
```bash
sudo su hdfs
# <username> denotes the user who will run SpeeDO
hdfs dfs -mkdir -p /user/<username>/cifar10/
hdfs dfs -chown -R <username>:supergroup /user/<username>/cifar10/
# switch back to <username>
exit
hdfs dfs -put <datasets> /user/<username>/cifar10/
```

At last, you hdfs should look like the following:
```bash
hdfs dfs -ls
-rw-r--r--   3 <username> supergroup   ***  cifar10/cifar10_full_solver.prototxt
-rw-r--r--   3 <username> supergroup   ***  cifar10/cifar10_full_train_test.prototxt
-rw-r--r--   3 <username> supergroup   ***  cifar10/cifar10_test_datumfile
-rw-r--r--   3 <username> supergroup   ***  cifar10/cifar10_train_datumfile
-rw-r--r--   3 <username> supergroup   ***  cifar10/mean.binaryproto
```

## Step.4 Run directly with Yarn

In this mode, Yarn is responsible for allocating containers for master and workers. You can manage applications and view logs in the usual Yarn ways.

For example, to run 1000 iterations asynchronously using 3 workers:
```bash
./sbt assembly
hadoop jar target/scala-2.11/SpeeDO-yarn-1.0.jar --appClass com.htc.speedo.SpeeDOApp --solver cifar10/cifar10_full_solver.prototxt --worker 3 --redis <redis-address> --test 500 --maxIter 1000
```

**NOTE** The training data will distribute to each node from hdfs. So, we may encounter an IO bottleneck when training data get massive for some applications. It's better to put datasets at the same location on all the machines in advanced for this situation(don not need to upload onto HDFS anymore). You also need to change the path in the Caffe network definition to absolute path of the data on each machine.
