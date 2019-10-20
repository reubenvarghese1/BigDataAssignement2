#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]
    supp = sys.argv[2]
    conf = sys.argv[3]
    prot = sys.argv[4]

    print ("Executing HW2SQL with input from " + inFile + ", support=" +supp + ", confidence=" + conf + ", protection=" + prot)

    pp_schema = StructType([
            StructField("uid", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("val", IntegerType(), True)])

    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    Pro_Publica.createOrReplaceTempView("Pro_Publica")
    sqlContext.cacheTable("Pro_Publica")
    spark.sql("select count(*) from Pro_Publica").show()

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select attr, val, count(*) as supp \
               from Pro_Publica \
              group by attr, val \
             having count(*) >= "  + str(supp);
    F1 = spark.sql(query);
    F1.createOrReplaceTempView("F1")

    # YOUR SparkSQL CODE GOES HERE
    # You may use any valid SQL query, and store the output in intermediate temporary views
    # Output must go into R2, R3 and PD_R3 as stated below.  Do not change the format of the output
    # on the last three lines.

    # Compute R2, as described in the homework specification
    # R2(attr1, val1, attr2, val2, supp, conf)

    queryo = "SELECT * FROM (select iter.attr as attr1, iter.val as val1, vdectable.attr as attr2, vdectable.val as val2, count(*) as supportspecific FROM (select uid, attr, val from Pro_Publica where attr<>'vdecile') iter LEFT JOIN (SELECT * FROM Pro_Publica WHERE attr='vdecile') vdectable where iter.uid=vdectable.uid GROUP BY iter.attr, iter.val, vdectable.attr, vdectable.val) inti WHERE inti.supportspecific>500"
    F2 = spark.sql(queryo);
    F2.createOrReplaceTempView("F2")


    query ="select V.attr1 , V.val1 ,V.attr2, V.val2, V.SUPPORTSPECIFIC as support,((V.SUPPORTSPECIFIC * 1.0)/E.supp) as  conf from F1 E inner join F2 V ON (E.attr=V.attr1 and E.val=V.val1) WHERE (V.SUPPORTSPECIFIC >" + str(supp) + ") and ((V.SUPPORTSPECIFIC * 1.0)/E.supp) > " + str(conf) + " ORDER BY V.attr1, V.val1, V.attr2 ,V.val2";
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")

    # MORE OF YOUR SparkSQL CODE GOES HERE

    # Compute R3, as described in the homework specification
    # R3(attr1, val1, attr2, val2, attr3, val3, supp, conf)

    totalsum = "SELECT * FROM (SELECT itable.attr1 , itable.val1 ,itable.attr2, itable.val2, count(*) AS SUPPORTPARTIAL FROM (SELECT A.uid AS uniqueid, A.attr AS attr1, A.val AS val1, B.attr AS attr2, B.val AS val2 FROM Pro_Publica A, Pro_Publica B WHERE A.attr <> B.attr AND A.uid = B.uid AND A.attr <> 'vdecile' AND B.attr <> 'vdecile') itable WHERE itable.attr1 < itable.attr2 GROUP BY itable.attr1 , itable.val1 ,itable.attr2, itable.val2) ink WHERE ink.SUPPORTPARTIAL>500"

    UI = spark.sql(totalsum);
    UI.createOrReplaceTempView("UI")

    specificsup = "SELECT * FROM (SELECT itable.attr1 , itable.val1 ,itable.attr2, itable.val2, vdectable.attr, vdectable.val, count(*) AS SUPPORTSPECIFIC FROM (SELECT A.uid AS uniqueid, A.attr AS attr1, A.val AS val1, B.attr AS attr2, B.val AS val2 FROM Pro_Publica A, Pro_Publica B WHERE A.attr <> B.attr AND A.uid = B.uid AND A.attr <> 'vdecile' AND B.attr <> 'vdecile') itable LEFT JOIN (SELECT * FROM Pro_Publica WHERE attr='vdecile') vdectable ON (itable.uniqueid = vdectable.uid) WHERE itable.attr1 < itable.attr2 GROUP BY itable.attr1 , itable.val1 ,itable.attr2, itable.val2, vdectable.attr, vdectable.val) inka WHERE inka.SUPPORTSPECIFIC>500"
    UIS = spark.sql(specificsup);
    UIS.createOrReplaceTempView("UIS")




    query = "select E.attr1 , E.val1 ,E.attr2, E.val2, V.attr,V.val, V.SUPPORTSPECIFIC as support,((V.SUPPORTSPECIFIC * 1.0)/E.SUPPORTPARTIAL) as  conf from UI E inner join UIS V ON (E.attr1=V.attr1 and E.val1=V.val1 and E.attr2=V.attr2 and E.val2=V.val2) WHERE (V.SUPPORTSPECIFIC >" + str(supp) + ") and ((V.SUPPORTSPECIFIC * 1.0)/E.SUPPORTPARTIAL) > " + str(conf) + " ORDER BY E.attr1, E.val1, E.attr2 ,E.val2, V.attr, V.val";
    
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")

    # MORE OF YOUR SparkSQL CODE GOES HERE

    # Compute PD_R3, as described in the homework specification
    # PD_R3(attr1, val1, attr2, val2, attr3, val3, supp, conf, prot)
    query = "SELECT Distinct E.attr1 , E.val1 ,E.attr2, E.val2, E.attr,E.val, E.support, E.conf, round(((E.conf * 1.0)/U.conf),2) as prot from R3 E,R2 U,R2 V WHERE (V.attr1=E.attr2 OR V.attr1=E.attr1) AND V.attr1='race' AND ( CASE WHEN E.attr2='race' THEN (E.attr1=U.attr1 AND E.val1=U.val1) WHEN E.attr1='race' THEN (E.attr2=U.attr2 AND E.val2=U.val2) END ) AND ((E.conf * 1.0)/U.conf)>"+ str(prot) + " ORDER BY E.attr1, E.val1, E.attr2 ,E.val2, E.attr, E.val";

    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")

    R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")

    sc.stop()

