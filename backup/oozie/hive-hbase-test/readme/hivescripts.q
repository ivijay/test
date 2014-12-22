CREATE TABLE IF NOT EXISTS v_hive_hbase_load(
key int,
val1 int,
val2 int,
val3 int
)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath '/user/463683/input/Hive_Hbase_test' OVERWRITE into table v_hive_hbase_load;

select * from v_hive_hbase_load;

CREATE TABLE IF NOT EXISTS v_hive_hbase2(key int, value1 string, value2 int, value3 int) 
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,a:b,a:c,d:e"
    );

INSERT OVERWRITE TABLE  v_hive_hbase2 SELECT key, val1, val2, val3 from v_hive_hbase_load;

select * from v_hive_hbase2;
