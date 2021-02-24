CREATE TABLE IF NOT EXISTS openaq
(
  LOCATION                STRING,
  city                    STRING,
  country                 STRING,
  UTC                     TIMESTAMP,
  local_time              STRING,
  parameter               STRING,
  value                   DOUBLE,
  unit                    STRING,
  latitude                DOUBLE,
  longitude               DOUBLE,
  attribution             STRING
)
PARTITIONED BY(
  ds STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
"separatorChar" = ",",
  "quoteChar" = "\""
)
TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE openaq SET SERDEPROPERTIES ("timestamp.formats" = "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
-- LOAD DATA LOCAL INPATH '${hiveconf:FILE}' OVERWRITE INTO TABLE openaq PARTITION(ds = '${hiveconf:DS}');