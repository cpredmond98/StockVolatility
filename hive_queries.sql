create database if not exists stocks;
use stocks;

drop table if exists stockinfo;
create table rawInput(
    id int,
    day string,
    openCost double,
    high double,
    low double,
    closeCost double,
    volume int,
    stockName string
)
ROW format delimited 
fields terminated by ','
lines terminated by '\n';

load data local inpath 'path to data' OVERWRITE INTO TABLE stockinfo;

drop table if exists maxmin;
create table maxmin as 
    select stockName, MAX(high) as FY_Max , MIN(low) as FY_Min
    from stockinfo
    group by stockName;

drop table if exists processed
create table processed as  
    select r.*, m.FY_Max, m.FY_Min
    from rawInput r 
    left outer join maxmin m
    on(r.stockName = m.stockName);

select * from processed limit 10;

