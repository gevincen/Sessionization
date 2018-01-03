from pyspark.sql.types import *
from pyspark.sql import Row
import pandas as pd  
from datetime import timedelta
from pyspark.sql.functions import concat, col, lit

# Step 1: Get the data into spark context table view

sc = spark.sparkContext
lines = sc.textFile("site_events.csv")
parts = lines.map(lambda l: l.split("\t"))
visitors = parts.map(lambda p: (int(p[0].strip()), p[1].strip(), p[2].strip(), p[3].strip()))
schemaString = "vid ed et tid"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaVisitors = spark.createDataFrame(visitors, schema)
schemaVisitors.createOrReplaceTempView("visitors")

# Step 2: Elimiate visitors not included in the tests, eliminate duplicates, extract test ids 

rs1 = spark.sql("SELECT vid, ed, et,  explode(split(tid, ',')) as tid1 FROM visitors where tid <> '' order by tid1, vid, ed, et")
rs1.count()
rs1 = rs1.dropDuplicates()
rs2 = rs1.selectExpr("substring(tid1,1,6) as tid" , "vid" , "concat(ed,' ',et) as ts")

rs2a = rs1.selectExpr("substring(tid1,1,6) as tid" , "vid" , "ed", "et")
rs2a = rs2a.dropDuplicates()
rs2a.count()

# Answer 1: How many unique visitors were included in each test
rs2b = rs2a.selectExpr("tid" , "vid")
rs2b.dropDuplicates()
rs2b.groupBy("tid").count().show()

#Order by <testid, visitorid, eventdate, eventime>
rs3a = rs2a.orderBy(["tid", "vid", "ed" , "et"], ascending=[1, 1, 1, 1])
rs3b = rs3a.selectExpr("concat(tid, '-', vid) as tidvid" , "concat(ed,' ',et) as ts")

#Get the dataset into Pandas for Sessioniation logic implementation
rs4 = rs3b.toPandas()
rs4a = pd.DataFrame([rs4.tidvid, pd.to_datetime(rs4.ts)])
rs4b = rs4a.T
rs4b =  pd.concat([rs4b, rs4b.groupby('tidvid').transform(lambda x:x.shift(1))] ,axis=1)
rs4b.columns= ['tidvid', 'ts', 'prevts']

rs4b['new_session'] = ((rs4b['ts'] - rs4b['prevts'])>=T).astype(int)
rs4b['increment'] = rs4b.groupby("tidvid")['new_session'].cumsum()
rs4b['session_id'] = rs4b['tidvid'].astype(str) + '_' + rs4b['increment'].astype(str)

# Final result dataset is brought back for easy Spark operations to get the results printed
sesions_df = rs4b['session_id'].apply(lambda x: pd.Series(x.split('-')))


SessionschemaString = "tid vid sid"
Sessionfields = [StructField(field_name, StringType(), True) for field_name in SessionschemaString.split()]
Sessionschema = StructType(Sessionfields)
schemaSess = spark.createDataFrame(sesions_df, Sessionschema)
schemaSess.createOrReplaceTempView("sesions_df")

sess_res = spark.sql("select tid, vid, max(sid) as sid from sesions_df group by tid, vid ")
sess_res_a = spark.sql(" select tid, sum(sid1) as uvid from (select tid, vid, max(sid) as sid1 from sesions_df group by tid, vid ) a group by tid order by uvid desc")




