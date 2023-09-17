CREATE EXTERNAL TABLE IF NOT EXISTS `glue-database`.`machine_leaning_trusted` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int,
  `customername` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `registrationdate` bigint,
  `sharewithresearchasofdate` bigint,
  `sharewithpublicasofdate` bigint,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spark-s3-bucket/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');