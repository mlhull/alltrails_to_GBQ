from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, count, when, col
import os

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('alltrails_pipeline') \
        .getOrCreate()
    return spark

def new_column_names(df):
    df = df.withColumnRenamed('state', 'state_name') \
        .withColumnRenamed('AverageTemperatureF', 'avg_temp_f') \
        .withColumnRenamed('AverageTemperatureC', 'avg_temp_c') \
        .withColumnRenamed('AverageTemperatureAvgAnnualPrecipitationIn', 'avg_temp_avg_ann_prec_in') \
        .withColumnRenamed('AverageTemperatureAvgHighF', 'avg_temp_avg_high_f') \
        .withColumnRenamed('AverageTemperatureAvgLowF', 'avg_temp_avg_low_f')
    return df

if __name__ == '__main__':
    spark = create_spark_session()

#Get the environment variables ## UPDATE ##
    ALLTRAILS_DATA_PATH = os.environ.get('ALLTRAILS_DATA_PATH', 'gs://#PLACEHOLDER/alltrails-data.csv') #PLACEHOLDER
    CLIMATE_DATA_PATH = os.environ.get('CLIMATE_DATA_PATH', 'gs://#PLACEHOLDER/average-temperatures-by-state-2024.csv') #PLACEHOLDER
    BUCKET_NAME = os.environ.get('BUCKET_NAME', '#PLACEHOLDER') #PLACEHOLDER
    BQ_DATASET = os.environ.get('BQ_DATASET', 'alltrails_dataset') 
    BQ_TABLE = os.environ.get('BQ_TABLE', 'alltrails_table') 

    # Extract data - Alltrails
    alltrails_df = spark.read.option("header", True). \
        option("inferSchema", True). \
            csv(ALLTRAILS_DATA_PATH) 
    new_value = 'Hawaii'
    alltrails_df = alltrails_df.withColumn('state_name', when(alltrails_df['state_name'] == 'Maui', new_value).otherwise(alltrails_df['state_name'])). \
        drop('visitor_usage')

    # Extract data - Climate
    climate_df = spark.read.option("header", True). \
    option("inferSchema", True). \
        csv(CLIMATE_DATA_PATH) 
    climate_df = new_column_names(climate_df)

    # Transform data into dataframe for GBQ 
    alltrails_df.createOrReplaceTempView("alltrails_df")
    climate_df.createOrReplaceTempView("climate_df")

    final_df = spark.sql('''
        select
            a.*,
            b.avg_temp_f,
            b.avg_temp_c,
            b.avg_temp_avg_ann_prec_in,
            b.avg_temp_avg_high_f,
            b.avg_temp_avg_low_f
        from alltrails_df as a 
            left join climate_df as b
                on a.state_name = b.state_name
    '''
                        )    

    # Load data into GBQ
    spark.conf.set('temporaryGcsBucket', BUCKET_NAME)

    final_df.write.format('bigquery') \
    .option('table', f'{BQ_DATASET}.{BQ_TABLE}') \
    .mode('append') \
    .save()