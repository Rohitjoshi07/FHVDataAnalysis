import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when


month_dict= {1:"January",2:"Febuary",3:"March",4:"April",5:"May",6:"June",7:"July",8:"August",9:"September",10:"October",11:"November",12:"December"}
year = 2022 
month_mapper= {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"june",7:"july",8:"aug",9:"sept",10:"oct",11:"nov",12:"dec"}

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-fhvhv') \
        .getOrCreate()
    return spark


def transform_df(df, month):
    
    df = df.repartition(18)
    updated_df = df.withColumn('pickup_date',F.to_date(df['pickup_datetime'])) \
            .withColumn('dropoff_date', F.to_date(df['dropoff_datetime']))\
            .withColumn('service_name', when(df['hvfhs_license_num']=='HV0003', "Uber").when(df['hvfhs_license_num']=='HV0005',"Lyft").otherwise("Others"))\
            .withColumn("trip_month", month)\
            .drop('pickup_datetime')\
            .drop('dropoff_datetime')\
            .na.fill(value=0,subset=["airport_fee","tips","trip_miles","trip_time","tolls","base_passenger_fare","driver_pay","bcf"])\
            .na.fill(value='N',subset=["access_a_ride_flag","wav_match_flag","wav_request_flag","shared_match_flag","shared_request_flag"])
    
    print(f"dataframe updated! --{month}")
    
    return updated_df


def read_data(spark, fpath):
    df = spark.read.parquet(fpath)
    return df


def process_trips_data(spark, input_path, output_path):
    for i in range(1,13):
        fmon = month_mapper[i]
        df = read_data(spark, input_path.format(month=fmon))
        updated_df = transform_df(df, F.lit(month_dict[i]))
        updated_df = updated_df.coalesce(20)

        updated_df.coalesce(10).write \
            .mode('overwrite').format('parquet').save(f'{output_path}/trips_data/{fmon}')
        print(f"Data submitted for {month_dict[i]}")
    
    if i>=13:
        print(f"Data processed successfully!")
    else:
        print(f"Data processed successfully till {month_dict[i]}!")
    return True

def save_to_bigquery(df_result, output):
    df_result.write.format('bigquery') \
    .option('table', output) \
    .save()
    print(f"Data submitted to bigquery: {output}")



def main():
    print("Spark-processing started!")
    output_path = "gs://fhvhv-data-lake/output_fhv_data"
    input_path = "gs://fhvhv-data-lake/2022/raw/{month}/*"
    bq_path = "fhvhv_trips/report-2022"

    spark = create_spark_session()

    trip_data_status = process_trips_data(spark, input_path, output_path)
    if trip_data_status:
        print("Spark-processing is completed!")


    print("Job execution successful!")
    

if __name__=="__main__":
    main()

    
    
