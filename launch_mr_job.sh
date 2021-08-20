#!/bin/bash

echo "compiling program"
hadoop com.sun.tools.javac.Main StockVolatility.java
jar cf sv.jar StockVolatility*.class

echo "running map reduce"
hadoop jar sv.jar StockVolatility stock/pig_output stock/mr_output

echo "retrieving map reduce results"
rm -r ~/stock/output
hdfs dfs -get stock/mr_output ~/stock/output
hdfs dfs -rm -r stock/output
cat ~/stock/output/*
