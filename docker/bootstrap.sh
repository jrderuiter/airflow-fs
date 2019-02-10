#!/usr/bin/env bash

$HADOOP_HOME/etc/hadoop/hadoop-env.sh

/etc/init.d/ssh start
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

/etc/init.d/proftpd start
