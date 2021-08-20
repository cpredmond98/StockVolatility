#!/bin/sh
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64
export PATH=$PATH:JAVA_HOME/bin
export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/lib/tools.jar

mkdir stock
mkdir stock/data
hdfs dfs -mkdir stock
hdfs dfs -mkdir stock/data
