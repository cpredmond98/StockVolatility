#!/bin/bash

hdfs dfs -put ~/stock/data/sap.csv ~/stock/data

echo "compiling program"
hadoop com.sun.tools.javac.Main StockVolatility.java
jar cf sv.jar StockVolatility*.class

echo "running"
hadoop jar sv.jar StockVolatility ~/stock/data/ ~/stock/output

echo "retrieving results"
rm -r ~/stock/output
hdfs dfs -get ~/stock/output ~/stock/output
hdfs dfs -rm -r ~/stock/output
cat ~/stock/output/*
