
echo "Starting script."

echo "Running Hive queries, will print the 10 days with the highest volume of trades and the 5 year min and max for each stock."

hive -f hive_queries.sql

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
