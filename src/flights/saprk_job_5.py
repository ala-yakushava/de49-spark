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

def process(spark: SparkSession, flights_path: str, airlines_path: str, result_path: str):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights_data_path = os.path.join(Path(__name__).parent, flights_path)
    airlines_data_path = os.path.join(Path(__name__).parent, airlines_path)

    flights_fact = spark.read \
        .option("header", "false") \
        .schema(flights_schema) \
        .parquet(flights_data_path)
    
    airlines_fact = spark.read \
        .option("header", "false") \
        .schema(airlines_schema) \
        .parquet(airlines_data_path)

    datamart = flights_fact \
        .join(other=airlines_fact, on=airlines_fact['IATA_CODE'] == flights_fact['AIRLINE'], how='inner') \
        .groupBy(airlines_fact['AIRLINE']) \
        .agg(
            F.count(F.when((F.col('ARRIVAL_DELAY') <= 0) & (F.col('CANCELLED') == 0), 1)).alias('correct_count'),
            F.count(F.when((F.col('ARRIVAL_DELAY') > 0) & (F.col('CANCELLED') == 0), 1)).alias('diverted_count'),
            F.count(F.when(F.col('CANCELLED') == 1, 1)).alias('cancelled_count'),
            F.avg(flights_fact['DISTANCE']).alias('avg_distance'),
            F.avg(flights_fact['AIR_TIME']).alias('avg_air_time'),
            F.count(F.when(F.col('CANCELLATION_REASON') == 'A', 1)).alias('airline_issue_count'),
            F.count(F.when(F.col('CANCELLATION_REASON') == 'B', 1)).alias('weather_issue_count'),
            F.count(F.when(F.col('CANCELLATION_REASON') == 'C', 1)).alias('nas_issue_count'),
            F.count(F.when(F.col('CANCELLATION_REASON') == 'D', 1)).alias('security_issue_count'),
        ) \
        .select(airlines_fact['AIRLINE'].alias('AIRLINE_NAME'),
            F.col('correct_count'),
            F.col('diverted_count'),
            F.col('cancelled_count'),
            F.col('avg_distance'),
            F.col('avg_air_time'),
            F.col('airline_issue_count'),
            F.col('weather_issue_count'),
            F.col('nas_issue_count'),
            F.col('security_issue_count'))

    datamart.show(truncate=False, n=100)
    datamart.write.mode('overwrite').parquet(result_path)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='src/flights/data/flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='src/flights/data/airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='output', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
