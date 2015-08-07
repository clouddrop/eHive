CREATE EXTERNAL TABLE RAW_ADHAR(
Registrar string,
Agency string,
State string,
District string,
SubDistrict string ,
Pin BIGINT,
Gender string,
Age int,
Aadhaar int,
Rejected int,
email int ,
mobile int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION '/hive/adhar/input'


set hive.enforce.bucketing = true
else mapred.reduce.tasks=5


CREATE TABLE comp_adhar(
Registrar string,
Agency string,
District string,
SubDistrict string ,
Pin BIGINT,
Gender string,
Age int,
Aadhaar int,
Rejected int,
email int ,
mobile int
)
 COMMENT 'This is complicated table'
 PARTITIONED BY(State string)
 CLUSTERED BY(Age) SORTED BY(Gender) INTO 5 BUCKETS ;
 
 
 
 
 
 Registrar, Agency, State,District,SubDistrict ,Pin,Gender,Age,Aadhaar,Rejected,email,mobile
 
 
 INSERT OVERWRITE TABLE  comp_adhar partition (State="West Bengal") 
 select Registrar, Agency, District,SubDistrict ,
 Pin,Gender,Age,Aadhaar,Rejected,email,mobile from RAW_ADHAR where State='West Bengal';
 
 
 FROM raw_adhar
 INSERT OVERWRITE TABLE  comp_adhar partition (State="Andhra Pradesh") 
 select Registrar, Agency, District,SubDistrict ,Pin,Gender,Age,Aadhaar,Rejected,email,mobile where State='Andhra Pradesh'
 INSERT OVERWRITE TABLE  comp_adhar partition (State="Gujarat") 
 select Registrar, Agency, District,SubDistrict ,Pin,Gender,Age,Aadhaar,Rejected,email,mobile where State='Gujarat'
 INSERT OVERWRITE TABLE  comp_adhar partition (State="Karnataka") 
 select Registrar, Agency, District,SubDistrict ,Pin,Gender,Age,Aadhaar,Rejected,email,mobile where State='Karnataka';
 
  
from raw_adhar 
INSERT OVERWRITE TABLE comp_adhar PARTITION(State) 
select Registrar, Agency, District,SubDistrict ,Pin,Gender,Age,Aadhaar,Rejected,email,mobile,State
DISTRIBUTE BY State;

 
 load from file
 load  data  local inpath '/Users/samar.kumar/edureka/hive/data/maharashtra' 
 overwrite into table comp_adhar partition(state='Maharashtra'); 
 --small table
 
 CREATE EXTERNAL TABLE FULL_GENDER(short_name string, full_name string) 
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ","  location '/hive/fullgender/input';

 
 
 select sub.state , max(sub.av) from (select district,state,avg(age) av 
 from comp_adhar group by district,state) sub group by sub.state;
 

  select c.subdistrict , c.age , f.full_name  from comp_adhar c   
  join  FULL_GENDER  f on c.gender = f.short_name where  State='Karnataka';
  
  select disticnt 

 
 --map join
select /*+ MAPJOIN(full_gender) */ * from raw_adhar join full_gender on (raw_adhar.gender=full_gender.short_name) where  State='Andhra Pradesh' ;

select /*+ MAPJOIN(full_gender) */ comp_adhar.subdistrict  , comp_adhar.age , full_gender.full_name from comp_adhar join full_gender on (comp_adhar.gender=full_gender.short_name) where  State='Andhra Pradesh' ;


select *   from comp_adhar join full_gender on (comp_adhar.gender=full_gender.short_name)
where  State='Andhra Pradesh' ;


select * from comp_adhar tablesample(bucket 1 out of 5)


create temporary function fullgender as 'samar.hive.udf.FullGender';

create temporary function funct_two as 'samar.hive.udf.ADChar';

 select fullgender(short_name), full_name from full_gender;
 
 
 
 insert overwrite local directory '/tmp/hiveoutput' select count(*) from wc  where words=='one'  limit 5; 
 