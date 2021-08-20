
echo "Starting script."

echo "Running Hive queries, will print the 10 days with the highest volume of trades."

hive -f hive_queries.sql

echo "Running Hive query to print the 5 year max and 5 year min into a text file named '5_yearminmax.txt'"
hive -e 'select stockName, FY_Max, FY_Min from stock.processed' > 5_yearminmix.txt

echo "Preparing to run pig"
hdfs dfs -rm -r stock/pig_output
echo "Running pig"
pig -x -useHCatalog mapreduce pig_queries.pig

rm -r pig_output
hdfs dfs -copyToLocal stock/pig_output pig_output

echo "displaying first 5 rows of the table generated in Hive/Pig"
head pig_output/*

echo ""
echo "Calculating anualized volatility"
chmod 777 launch_mr_job.sh
./launch_mr_job.sh
