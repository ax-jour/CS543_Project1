from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
        .builder \
        .master("local[11]") \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.memory.offHeap.enabled",True) \
        .config("spark.memory.offHeap.size","16g") \
        .appName("Airline") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SE = [(37,80),(24,65)]
NE = [(50,80),(37,65)]
MSE = [(37,95),(24,80)]
MNE = [(50,95),(37,80)]
MSW = [(37,110),(24,95)]
MNW = [(50,110),(37,95)]
SW = [(37,125),(24,110)]
NW = [(50,125),(37,110)]

for YEAR in range(2009,2019):
        airport_station_df = spark.read.option("header",True).csv('./data/airports_stations.csv')
        ghcnd_all = spark.read.option("header",True).csv('./data/filtered_weather.csv').drop('M_FLAG','Q_FLAG','S_FLAG','OBS_TIME')

        selected_airport_station = airport_station_df.select(col('ORIGIN'),col('STATION'))
        ghcnd_all = ghcnd_all.join(selected_airport_station, \
                                                ghcnd_all.STATION_ID == selected_airport_station.STATION, \
                                                "LeftOuter")

        full_stations_info = spark.read.option("header",False).csv('./data/ghcnd-stations.csv')

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

        areas = geo.withColumn("AREA",when((geo.LAITITUDE < SE[0][0]) & (geo.LONGITUDE < SE[0][1]),"SE") \
                                        .when((geo.LAITITUDE < NE[0][0]) & (geo.LONGITUDE < NE[0][1]),"NE") \
                                        .when((geo.LAITITUDE < MSE[0][0]) & (geo.LONGITUDE < MSE[0][1]),"MSE") \
                                        .when((geo.LAITITUDE < MNE[0][0]) & (geo.LONGITUDE < MNE[0][1]),"MNE") \
                                        .when((geo.LAITITUDE < MSW[0][0]) & (geo.LONGITUDE < MSW[0][1]),"MSW") \
                                        .when((geo.LAITITUDE < MNW[0][0]) & (geo.LONGITUDE < MNW[0][1]),"MNW") \
                                        .when((geo.LAITITUDE < SW[0][0]) & (geo.LONGITUDE < SW[0][1]),"SW") \
                                        .when((geo.LAITITUDE < NW[0][0]) & (geo.LONGITUDE < NW[0][1]),"NW") \
                                        .when((geo.LAITITUDE > NW[0][0]) & (geo.LONGITUDE > NW[0][1]),"NW") \
                                        .when((geo.LAITITUDE < SW[0][0]) & (geo.LONGITUDE > SW[0][1]),"SW") \
                                        .when((geo.LAITITUDE < SE[0][0]) & (geo.LONGITUDE < SE[0][1]),"SE") \
                                        .when((geo.LAITITUDE > NE[0][0]) & (geo.LONGITUDE < NE[0][1]),"NE") \
                                        .otherwise(geo.STATION)) \
                            .drop('LAITITUDE','LONGITUDE')

        airports_stations =  spark.read.option("header",True).csv('./data/airports_stations.csv')
        fl = spark.read.option("header",True).csv('./data/flights/{}.csv'.format(YEAR))

        df = fl.select([
                'FL_DATE', \
                'OP_CARRIER', \
                'OP_CARRIER_FL_NUM', \
                'ORIGIN', \
                'DEST', \
                'CRS_DEP_TIME', \
                'DEP_DELAY', \
                'CRS_ARR_TIME', \
                'DIVERTED', \
                'CANCELLED', \
                'ARR_DELAY' \
                ])

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

        df = df.withColumn('ORIG_WEATHER', when((array_contains(df.ORIG_WEATHERS,'SNWD') | array_contains(df.ORIG_WEATHERS,'SNOW') | array_contains(df.ORIG_WEATHERS,'WT18')),'Snow') \
                                    .when((array_contains(df.ORIG_WEATHERS,'PRCP') | array_contains(df.ORIG_WEATHERS,'WT16')),'Rain') \
                                    .when((array_contains(df.ORIG_WEATHERS,'WDF2') | array_contains(df.ORIG_WEATHERS,'WDF5') | array_contains(df.ORIG_WEATHERS,'WT11')),'Wind') \
                                    .otherwise('Sunny')) \
        .withColumn('DEST_WEATHER', when((array_contains(df.DEST_WEATHERS,'SNWD') | array_contains(df.DEST_WEATHERS,'SNOW') | array_contains(df.DEST_WEATHERS,'WT18')),'Snow') \
                                    .when((array_contains(df.DEST_WEATHERS,'PRCP') | array_contains(df.DEST_WEATHERS,'WT16')),'Rain') \
                                    .when((array_contains(df.DEST_WEATHERS,'WDF2') | array_contains(df.DEST_WEATHERS,'WDF5') | array_contains(df.DEST_WEATHERS,'WT11')),'Wind') \
                                    .otherwise('Sunny'))

        df = df.drop('ORIG_WEATHERS','DEST_WEATHERS','ORIG_STATION','DEST_STATION')

        df.coalesce(1).write.option('header',True).csv('./output/{}'.format(YEAR))