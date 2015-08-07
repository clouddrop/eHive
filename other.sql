 show explain path
 explain select count(*) from wc  where words=='one'  limit 5;
 
 create database db;
 show databases;
 
 
 load data from a path 
 
  LOAD DATA LOCAL INPATH './examples/files/kv2.txt' OVERWRITE INTO TABLE invites PARTITION (ds='2008-08-15');
  

  
  
  --1w2w3v1a1v1wloc
  
   --friends = 1,2,3
  properties = 1 , 1
  struc  adid=1 loc=loc
  
  --1w2w3w5v8a9v1wloc
  
  friends = 1w2w3w5 = { 1 , 2 3, 5}
  properties = 8a9 = { 8:9a10}
  struc = 1wloc = {adid = 1 , loc = loc}
  
 CREATE EXTERNAL TABLE all_complex(
 friends ARRAY<BIGINT>, 
 properties MAP<STRING, STRING> , 
 struc STRUCT<adid:INT,loc:STRING>) 
 
 ROW FORMAT 
 DELIMITED FIELDS TERMINATED BY 'v' 
 COLLECTION ITEMS TERMINATED BY "w" 
 MAP KEYS TERMINATED BY "a" 
 location '/hive-class/comp-type' ;
 
 friends{1,2,3}
 properties{1 ,1}
 struc adid:1 loc:loc
 
 
 
 
 
 
 
 
 --- complex type
 
 
 
 
CREATE EXTERNAL TABLE all_complex(friends ARRAY<BIGINT>, 
 properties MAP<STRING, STRING> , 
 struc STRUCT<adid:INT,loc:STRING>) 
 
 ROW FORMAT 
 DELIMITED FIELDS TERMINATED BY '|' 
 COLLECTION ITEMS TERMINATED BY "," 
 MAP KEYS TERMINATED BY ":" 
 location '/tmp/complextype/' ;
 
 
 
 select friends[1] , properties['1'] , struc.loc from all_complex;
 
 
 
 1,2,3|1:1|1,loc|
 
  1,2,3,4|1:1|1,loc|
 
 [1,2,3]	{"1":"1"}	{"adid":1,"loc":"loc"}
 
 
  1,2,3,4|1:1|1,loc|
  1,2,3,4|1:1|2,singapore|
  
   [1,2,3,4]	{"1":"1"}	{"adid":1,"loc":"loc"}
   [1,2,3,4]	{"1":"1"}	{"adid":2,"loc":"singapore"}


 