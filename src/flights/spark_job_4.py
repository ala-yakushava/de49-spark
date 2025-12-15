import os
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

flights_schema = StructType([
    StructField('YEAR', IntegerType(), True),
    StructField('MONTH', IntegerType(), True),
    StructField('DAY', IntegerType(), True),
    StructField('DAY_OF_WEEK', IntegerType(), True),
    StructField('AIRLINE', StringType(), True),
    StructField('FLIGHT_NUMBER', IntegerType(), True),
    StructField('TAIL_NUMBER', StringType(), True),
    StructField('ORIGIN_AIRPORT', StringType(), True),
    StructField('DESTINATION_AIRPORT', StringType(), True),
    StructField('SCHEDULED_DEPARTURE', IntegerType(), True),
    StructField('DEPARTURE_TIME', DoubleType(), True),
    StructField('DEPARTURE_DELAY', DoubleType(), True),
    StructField('TAXI_OUT', DoubleType(), True),
    StructField('WHEELS_OFF', DoubleType(), True),
    StructField('SCHEDULED_TIME', DoubleType(), True),
    StructField('ELAPSED_TIME', DoubleType(), True),
    StructField('AIR_TIME', DoubleType(), True),
    StructField('DISTANCE', IntegerType()),
    StructField('WHEELS_ON', DoubleType(), True),
    StructField('TAXI_IN', DoubleType(), True),
    StructField('SCHEDULED_ARRIVAL', IntegerType(), True),
    StructField('ARRIVAL_TIME', DoubleType(), True),
    StructField('ARRIVAL_DELAY', DoubleType(), True),
    StructField('DIVERTED', IntegerType(), True),
    StructField('CANCELLED', IntegerType(), True),
    StructField('CANCELLATION_REASON', StringType(), True),
    StructField('AIR_SYSTEM_DELAY', DoubleType(), True),
    StructField('SECURITY_DELAY', DoubleType(), True),
    StructField('AIRLINE_DELAY', DoubleType(), True),
    StructField('LATE_AIRCRAFT_DELAY', DoubleType(), True),
    StructField('WEATHER_DELAY', DoubleType(), True),
])

airlines_schema = StructType([
    StructField('IATA_CODE', StringType(), True),
    StructField('AIRLINE',  StringType(), True),
])

airports_schema = StructType([
    StructField('IATA_CODE', StringType(), True),
    StructField('AIRPORT',  StringType(), True),
    StructField('CITY',  StringType(), True),
    StructField('STATE',  StringType(), True),
    StructField('COUNTRY',  StringType(), True),
    StructField('LATITUDE',  DoubleType(), True),
    StructField('LONGITUDE',  DoubleType(), True),
])

def process(spark: SparkSession, flights_path: str, airlines_path: str, airports_path: str, result_path: str):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """

    flights_data_path = os.path.join(Path(__name__).parent, flights_path)
    airlines_data_path = os.path.join(Path(__name__).parent, airlines_path)
    airports_data_path = os.path.join(Path(__name__).parent, airports_path)

    flights_fact = spark.read \
        .option("header", "false") \
        .schema(flights_schema) \
        .parquet(flights_data_path)
    
    airlines_fact = spark.read \
        .option("header", "false") \
        .schema(airlines_schema) \
        .parquet(airlines_data_path)
    
    airports_fact_1 = spark.read \
        .option("header", "false") \
        .schema(airports_schema) \
        .parquet(airports_data_path)
    
    airports_fact_2 = spark.read \
        .option("header", "false") \
        .schema(airports_schema) \
        .parquet(airports_data_path)

    datamart = flights_fact \
        .join(other=airlines_fact, on=airlines_fact['IATA_CODE'] == flights_fact['AIRLINE'], how='inner') \
        .join(other=airports_fact_1, on=airports_fact_1['IATA_CODE'] == flights_fact['ORIGIN_AIRPORT'], how='inner') \
        .join(other=airports_fact_2, on=airports_fact_2['IATA_CODE'] == flights_fact['DESTINATION_AIRPORT'], how='inner') \
        .select(airlines_fact['AIRLINE'].alias('AIRLINE_NAME'),
            F.col('TAIL_NUMBER'),
            airports_fact_1['COUNTRY'].alias('ORIGIN_COUNTRY'),
            airports_fact_1['AIRPORT'].alias('ORIGIN_AIRPORT_NAME'),
            airports_fact_1['LATITUDE'].alias('ORIGIN_LATITUDE'),
            airports_fact_1['LONGITUDE'].alias('ORIGIN_LONGITUDE'),
            airports_fact_2['COUNTRY'].alias('DESTINATION_COUNTRY'),
            airports_fact_2['AIRPORT'].alias('DESTINATION_AIRPORT_NAME'),
            airports_fact_2['LATITUDE'].alias('DESTINATION_LATITUDE'),
            airports_fact_2['LONGITUDE'].alias('DESTINATION_LONGITUDE'))

    datamart.show(truncate=False, n=100)
    datamart.write.mode('overwrite').parquet(result_path)

def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='src/flights/data/flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='src/flights/data/airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='src/flights/data/airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='output', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
