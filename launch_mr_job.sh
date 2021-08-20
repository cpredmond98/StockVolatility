#!/bin/bash
  
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64
export PATH=$PATH:JAVA_HOME/bin
export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/lib/tools.jar

echo "compiling program"
hadoop com.sun.tools.javac.Main StockVolatility.java
jar cf sv.jar StockVolatility*.class

echo "running map reduce"
hdfs dfs -rm -r stock/mr_output
hadoop jar sv.jar StockVolatility stock/pig_output stock/mr_output

echo "retrieving map reduce results"
rm -r ~/stock/output
hdfs dfs -get stock/mr_output ~/stock/output
hdfs dfs -rm -r stock/output
cat ~/stock/output/* > ~/stock/volatilities.txt
cat ~/stock/volatilities.txt
