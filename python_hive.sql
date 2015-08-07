CREATE TABLE u_data ( userid INT, movieid INT, rating INT, unixtime STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS TEXTFILE;


LOAD DATA LOCAL INPATH "/home/edureka/hive/u.data" OVERWRITE INTO TABLE u_data;


CREATE TABLE u_data_new (userid INT,movieid INT, rating INT, weekday STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

add FILE /home/edureka/hive/script.py;


INSERT OVERWRITE TABLE u_data_new SELECT TRANSFORM (userid, movieid, rating, unixtime) USING 'python /home/edureka/hive/script.py' AS (userid, movieid, rating, weekday) FROM u_data;




1,2,3,1104796800
1,2,3,1325635200
1,2,3,1154796800
1,2,3,1104796800
1,2,3,1104796800





import sys
import datetime
for line in sys.stdin:
	line = line.strip()
	userid, movieid, rating, unixtime = line.split('\t')
	weekday = datetime.datetime.fromtimestamp(float(unixtime)).isoweekday()
	print '\t'.join([userid, movieid, rating, str(weekday)])
