set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.paritions.pernode=1000;

CREATE DATABASE IF NOT EXISTS project;
USE project;

DROP TABLE IF EXISTS project.wettestpop_ext;

CREATE EXTERNAL TABLE IF NOT EXISTS project.wettestpop_ext (

		msa STRING,
		population STRING,
		year STRING,
		month STRING,
		totalrainfall double,
		wettestpopulation double
		)
		
		ROW FORMAT DELIMITED
		FIELDS TERMINATED BY '\t'
		STORED AS TEXTFILE
		
LOCATION '/user/cloudera/FP/output/wettestpopulation/'; 

CREATE TABLE IF NOT EXISTS project.wettestpop (
		
		msa STRING,
		population STRING,
		totalrainfall double,
		wettestpopulation double
		)		
		
		PARTITIONED BY (year STRING,month STRING)
		STORED AS PARQUET;

INSERT OVERWRITE TABLE project.wettestpop PARTITION (year, month)
		SELECT 	msa,
			population,
			totalrainfall,
			wettestpopulation,
			year,
			month
		FROM wettestpop_ext;

 

