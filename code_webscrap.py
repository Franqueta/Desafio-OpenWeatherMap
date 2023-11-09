import os
from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import col, explode

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pyspark-shell'

spark = SparkSession.builder \
    .appName("Current_Data") \
    .config("spark.hadoop.fs.s3a.access.key", "ueVk36Brq2ZX8gCGGj1w") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.29.8.228:9000") \
    .getOrCreate()

minio_bucket_name = "bucketd2d"
minio_path = "transient/historico"
format = "delta"
mode = "append"
lang = "pt_br"
units = "metric"
api_key = "c478634695f1e63e882ce6444d5198e4"
cidades = ["Aracaju", "Itabaiana", "Lagarto"]

df_current = None

for cidade in cidades:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={api_key}&units={units}&lang={lang}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        rdd = spark.sparkContext.parallelize([data], numSlices=1)
        # Converte os dados em um DataFrame
        df_city = spark.read.json(rdd, multiLine=True)
        # Seleciona apenas as colunas desejadas
        df_city = df_city.select(
            col('name').alias('city'),
            col('main.temp').alias('tempo'),
            col('main.temp_max').alias('tempe_max'),
            col('main.temp_min').alias('tempe_min'),
            col('clouds.all').alias('nuvens'),
            col('cod').alias('codígo'),
            col('dt').alias('dt'),
            col('timezone').alias('timezone'),
            col('main.feels_like').alias('main_feels_like'),
            explode('weather').alias('weather_explode'),
            col('id').alias('id'),
            col('weather_explode.main').alias('main'),
            col('weather_explode.icon').alias('icon'),
            col('weather_explode.id').alias('weather_id')
        )
        # Se df_current for nulo, atribui df_city a ele; caso contrário, faz um unionAll
        if df_current is None:
            df_current = df_city
        else:
            df_current = df_current.union(df_city)   

df_current.write.format("delta") \
    .mode("append") \
    .option("path", f"s3a://{minio_bucket_name}/{minio_path}") \
    .save()
spark.stop()






