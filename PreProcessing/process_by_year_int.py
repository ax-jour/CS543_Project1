import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# top_left corner has both greater latitude and greater longitude
# Northeast,Southeast,Mid-Northeast,Mid-Southeast,Mid-Northwest,Mid-Southwest,Northwest,Southwest
# [Top_left(lat,lon), Bottom_right(lat,lon)]
SE,NE = [(37,80),(24,65)],[(50,80),(37,65)]
MSE,MNE = [(37,95),(24,80)],[(50,95),(37,80)]
MSW,MNW = [(37,110),(24,95)],[(50,110),(37,95)]
SW,NW = [(37,125),(24,110)],[(50,125),(37,110)]

spark = SparkSession \
        .builder \
        .master("local[11]") \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.memory.offHeap.enabled",True) \
        .config("spark.memory.offHeap.size","16g") \
        .appName("Airline") \
        .getOrCreate()

        
for YEAR in range(2009,2019):
    print('Processing dataset from {}'.format(YEAR))
    start_time=time.time()

    airport_station_df = spark.read.option("header",True).csv('./data/airports_stations.csv')
    ghcnd_all = spark.read.option("header",True).csv('./data/filtered_weather.csv').drop('M_FLAG','Q_FLAG','S_FLAG','OBS_TIME')
    carriers = spark.read.option("header",True).csv('./data/carriers_type_int.csv')
    full_stations_info = spark.read.option("header",False).csv('./data/ghcnd-stations.csv')

    ghcnd_all = ghcnd_all.join(airport_station_df, \
                                ghcnd_all.STATION_ID == airport_station_df.STATION, \
                                "LeftOuter")


    geo = airport_station_df \
        .join(full_stations_info,airport_station_df.STATION == full_stations_info._c0,'leftouter') \
        .select(col('ORIGIN').alias("AIRPORT_CODE"), \
                col('STATE'), \
                col('CITY'), \
                col('NAME'), \
                col('STATION'), \
                col('_c1').alias('LAITITUDE').cast('Double'), \
                col('_c2').alias('LONGITUDE').cast('Double') \
            ).withColumn('LONGITUDE', col('LONGITUDE')*-1)

    areas = geo.withColumn("AREA",when((geo.LAITITUDE < SE[0][0]) & (geo.LONGITUDE < SE[0][1]),5) \
                                                    .when((geo.LAITITUDE < NE[0][0]) & (geo.LONGITUDE < NE[0][1]),1) \
                                                    .when((geo.LAITITUDE < MSE[0][0]) & (geo.LONGITUDE < MSE[0][1]),6) \
                                                    .when((geo.LAITITUDE < MNE[0][0]) & (geo.LONGITUDE < MNE[0][1]),2) \
                                                    .when((geo.LAITITUDE < MSW[0][0]) & (geo.LONGITUDE < MSW[0][1]),7) \
                                                    .when((geo.LAITITUDE < MNW[0][0]) & (geo.LONGITUDE < MNW[0][1]),3) \
                                                    .when((geo.LAITITUDE < SW[0][0]) & (geo.LONGITUDE < SW[0][1]),8) \
                                                    .when((geo.LAITITUDE < NW[0][0]) & (geo.LONGITUDE < NW[0][1]),4) \
                                                    .when((geo.LAITITUDE > NW[0][0]) & (geo.LONGITUDE > NW[0][1]),4) \
                                                    .when((geo.LAITITUDE < SW[0][0]) & (geo.LONGITUDE > SW[0][1]),8) \
                                                    .when((geo.LAITITUDE < SE[0][0]) & (geo.LONGITUDE < SE[0][1]),5) \
                                                    .when((geo.LAITITUDE > NE[0][0]) & (geo.LONGITUDE < NE[0][1]),1) \
                                                    .otherwise(geo.STATION)) \
                                .drop('LAITITUDE','LONGITUDE')

    fl = spark.read.option("header",True).csv('./data/flights/{}.csv'.format(YEAR))

    df = fl.select(['FL_DATE','OP_CARRIER','OP_CARRIER_FL_NUM','ORIGIN','DEST','CRS_DEP_TIME','DEP_DELAY','CRS_ARR_TIME','ARR_DELAY'])

    df = df.join(areas, fl.ORIGIN == areas.AIRPORT_CODE,'LeftOuter') \
                    .drop('AIRPORT_CODE','STATE','CITY','NAME') \
                    .withColumnRenamed('AREA', 'ORIG_AREA').drop('ORIGIN') \
                    .withColumnRenamed('STATION', 'ORIG_STATION') \
            .join(areas, fl.DEST == areas.AIRPORT_CODE,'LeftOuter') \
                    .drop('AIRPORT_CODE','STATE','CITY','NAME') \
                    .withColumnRenamed('AREA', 'DEST_AREA').drop('DEST') \
                    .withColumnRenamed('STATION', 'DEST_STATION')


    df = df.join(ghcnd_all,(df.ORIG_STATION == ghcnd_all.STATION_ID) & (ghcnd_all.DATE == date_format(df.FL_DATE,"yyyyMMdd")),'Inner') \
                    .drop('STATION','AIRPORT_CODE','DATE','STATION_ID') \
                    .withColumnRenamed('ELEMENT', 'ORIG_WEATHER') \
                    .withColumnRenamed('DATA_VALUE', 'ORIG_WEATHER_DATA') \
            .join(ghcnd_all, (df.DEST_STATION == ghcnd_all.STATION_ID) & (ghcnd_all.DATE == date_format(df.FL_DATE,"yyyyMMdd")),'Inner') \
                    .drop('STATION','AIRPORT_CODE','DATE','STATION_ID') \
                    .withColumnRenamed('ELEMENT', 'DEST_WEATHER') \
                    .withColumnRenamed('DATA_VALUE', 'DEST_WEATHER_DATA') \
            .filter(((col('ORIG_WEATHER_DATA').cast('Double') != 0) & (col('ORIG_WEATHER').rlike('(PRCP|SNOW|SNWD|^WD+|^WS+)'))) | \
                    ((col('DEST_WEATHER_DATA').cast('Double') != 0) & (col('DEST_WEATHER').rlike('(PRCP|SNOW|SNWD|^WD+|^WS+)')))) \
            .drop('ORIG_WEATHER_DATA','DEST_WEATHER_DATA')


    df = df.withColumn('ORIG_WEATHER', when(~df.ORIG_WEATHER.rlike('(PRCP|SNOW|SNWD|^WD+|^WS+)'),"SUNNY") \
                                        .otherwise(df.ORIG_WEATHER)) \
            .withColumn('DEST_WEATHER', when(~df.DEST_WEATHER.rlike('(PRCP|SNOW|SNWD|^WD+|^WS+)'),"SUNNY") \
                                        .otherwise(df.DEST_WEATHER))


    df = df.groupBy('FL_DATE', \
                    'OP_CARRIER', \
                    'OP_CARRIER_FL_NUM', \
                    'CRS_DEP_TIME', \
                    'DEP_DELAY', \
                    'CRS_ARR_TIME', \
                    'ARR_DELAY', \
                    'ORIG_STATION', \
                    'ORIG_AREA', \
                    'DEST_STATION', \
                    'DEST_AREA') \
            .agg(collect_list('ORIG_WEATHER').alias('ORIG_WEATHERS'), collect_list('DEST_WEATHER').alias('DEST_WEATHERS'))


    df = df.withColumn('ORIG_WEATHER', when((array_contains(df.ORIG_WEATHERS,'SNWD') | array_contains(df.ORIG_WEATHERS,'SNOW') | array_contains(df.ORIG_WEATHERS,'WT18')),2) \
                                        .when((array_contains(df.ORIG_WEATHERS,'PRCP') | array_contains(df.ORIG_WEATHERS,'WT16')),1) \
                                        .when((array_contains(df.ORIG_WEATHERS,'WDF2') | array_contains(df.ORIG_WEATHERS,'WDF5') | array_contains(df.ORIG_WEATHERS,'WT11')),3) \
                                        .otherwise(4)) \
            .withColumn('DEST_WEATHER', when((array_contains(df.DEST_WEATHERS,'SNWD') | array_contains(df.DEST_WEATHERS,'SNOW') | array_contains(df.DEST_WEATHERS,'WT18')),2) \
                                        .when((array_contains(df.DEST_WEATHERS,'PRCP') | array_contains(df.DEST_WEATHERS,'WT16')),1) \
                                        .when((array_contains(df.DEST_WEATHERS,'WDF2') | array_contains(df.DEST_WEATHERS,'WDF5') | array_contains(df.DEST_WEATHERS,'WT11')),3) \
                                        .otherwise(4))

    df = df.withColumn('ORIG_SCHEDULE', \
                        when((df.CRS_DEP_TIME.cast('Integer') >= 800) & (df.CRS_DEP_TIME < 1800),3) \
                        .when((df.CRS_DEP_TIME.cast('Integer') >= 600) & (df.CRS_DEP_TIME < 800),2) \
                        .otherwise(1)) \
            .withColumn('DEST_SCHEDULE', \
                        when((df.CRS_ARR_TIME.cast('Integer') >= 800) & (df.CRS_ARR_TIME < 1800),3) \
                        .when((df.CRS_ARR_TIME.cast('Integer') >= 600) & (df.CRS_ARR_TIME < 800),2) \
                        .otherwise(1)) \
            .drop('CRS_DEP_TIME','CRS_ARR_TIME')

    df = df.join(carriers, df.OP_CARRIER == carriers.OP_CARRIER, 'LeftOuter') \
            .drop('OP_CARRIER') \
            .withColumnRenamed('CATEGORY','CARRIER_TYPE')

    df = df.withColumn('DEP_DELAY_CAT', 
                    when((df.DEP_DELAY.cast('Double') < -60),1) \
                    .when((df.DEP_DELAY.cast('Double') >= -60) & (df.DEP_DELAY < -30),2) \
                    .when((df.DEP_DELAY.cast('Double') >= -30) & (df.DEP_DELAY < -15),3) \
                    .when((df.DEP_DELAY.cast('Double') >= -15) & (df.DEP_DELAY < -5),4) \
                    .when((df.DEP_DELAY.cast('Double') >= -5) & (df.DEP_DELAY <= 5),5) \
                    .when((df.DEP_DELAY.cast('Double') > 5) & (df.DEP_DELAY <= 15),6) \
                    .when((df.DEP_DELAY.cast('Double') > 15) & (df.DEP_DELAY <= 30),7) \
                    .when((df.DEP_DELAY.cast('Double') > 30) & (df.DEP_DELAY <= 60),8) \
                    .when((df.DEP_DELAY.cast('Double') > -60),9) \
                    .otherwise(5)) \
        .withColumn('ARR_DELAY_CAT', 
                    when((df.ARR_DELAY.cast('Double') < -60),1) \
                    .when((df.ARR_DELAY.cast('Double') >= -60) & (df.ARR_DELAY < -30),2) \
                    .when((df.ARR_DELAY.cast('Double') >= -30) & (df.ARR_DELAY < -15),3) \
                    .when((df.ARR_DELAY.cast('Double') >= -15) & (df.ARR_DELAY < -5),4) \
                    .when((df.ARR_DELAY.cast('Double') >= -5) & (df.ARR_DELAY <= 5),5) \
                    .when((df.ARR_DELAY.cast('Double') > 5) & (df.ARR_DELAY <= 15),6) \
                    .when((df.ARR_DELAY.cast('Double') > 15) & (df.ARR_DELAY <= 30),7) \
                    .when((df.ARR_DELAY.cast('Double') > 30) & (df.ARR_DELAY <= 60),8) \
                    .when((df.ARR_DELAY.cast('Double') > -60),9) \
                    .otherwise(5)) \
        .drop('DEP_DELAY','ARR_DELAY')

    df = df.select('CARRIER_TYPE','ORIG_AREA','DEST_AREA','DEP_DELAY_CAT','ORIG_SCHEDULE','DEST_SCHEDULE','ORIG_WEATHER','DEST_WEATHER','ARR_DELAY_CAT')


    df.coalesce(1).write.option('header',True).csv('./output/{}_int'.format(YEAR))
    print('Processing dataset from {} used {}'.format(YEAR,time.time()-start_time))


processed_int = spark.read.option("header",True).csv('./data/processed_int/')
processed_int.coalesce(1).write.option('header',True).csv('./output/processed_int')