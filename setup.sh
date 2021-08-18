#!/bin/sh
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64
export PATH=$PATH:JAVA_HOME/bin
export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/lib/tools.jar
cd data
hdfs dfs -mkdir data
hdfs dfs -put sap.csv data
cd ..
hadoop com.sun.tools.javac.Main StockVariance.java
jar cf sv.jar StockVariance*.class
hadoop jar sv.jar StockVariance data/ output

