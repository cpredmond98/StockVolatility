database = LOAD 'stocks.processed' using org.apache.hive.hcatalog.pig.HCatLoader();

set mapreduce.output.basename 'dataTable-';
STORE database into 'stock/data' USING PigStorage (',');
