import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# minhas importações
import boto3
import logging
from pyspark.sql.functions import (
  avg,
  col,
  to_date,
  year,
  month,
  regexp_replace,
  udf
)
from pyspark.sql.types import IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# configura o Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# início
database_name = "fiap-etl-tech-challenge"
table_name = "tb_b3_raw"

dyf = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)
df = dyf.toDF()

df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

logger.info("Schema do DataFrame após leitura da tabela:")
df.printSchema()

# Remover pontos e converter 'theoretical_quantity' para long
df = df.withColumn("theoretical_quantity",  regexp_replace(col("theoretical_quantity"), "[^0-9]", "").cast("int"))
df = df.na.drop(subset=["theoretical_quantity"])

# Substituir vírgula por ponto e converter 'participation_percentage' para double
df = df.withColumn("participation_percentage", regexp_replace(col("participation_percentage"), ",", ".").cast("double"))

def calc_week_of_month(date):
    if date is None:
        return None

    current_day = date.day
    return (current_day - 1) // 7 + 1

week_of_month_udf = udf(calc_week_of_month, IntegerType())

# Adicionar colunas 'month', 'year' e 'week_of_month'
df = df.withColumn("month", month(col("date"))) \
       .withColumn("year", year(col("date"))) \
       .withColumn("week_of_month", week_of_month_udf(col("date")))

logger.info("Schema do DataFrame depois de transformar tipo da tabela:")
df.printSchema()

# Agrupar por 'code', 'year', 'month' e 'week_of_month' e calcular média de 'theoretical_quantity' e 'participation_percentage'
df_monthly_avg = df.groupBy("code", "year", "month", "week_of_month").agg(
    avg("theoretical_quantity").alias("avg_monthly_theoretical_quantity"),
    avg("participation_percentage").alias("avg_monthly_participation_percentage")
)

bucket_name = "s3://fiap-etl-tech-challenge-2-540692057042"
transformed_data_path = f"{bucket_name}/refined"

# Salva df_monthly_avg como Parquet particionado
df_monthly_avg.write.mode("overwrite").partitionBy("code", "year", "month", "week_of_month").parquet(transformed_data_path)

glue_client = boto3.client("glue")
database_name = "fiap-etl-tech-challenge"
table_name = "tb_b3_refined"
table_location = transformed_data_path

table_input = {
    'Name': table_name,
    'StorageDescriptor': {
        'Columns': [
            {"Name": "avg_monthly_theoretical_quantity", "Type": "double"},
            {"Name": "avg_monthly_participation_percentage", "Type": "double"}
        ],
        'Location': table_location,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'Compressed': False,
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1'}
        }
    },
    'PartitionKeys': [
        {"Name": "code", "Type": "string"},
        {"Name": "year", "Type": "int"},
        {"Name": "month", "Type": "int"},
        {"Name": "week_of_month", "Type": "int"}
    ],
    'TableType': 'EXTERNAL_TABLE'
}

try:
    glue_client.get_table(DatabaseName=database_name, Name=table_name)
    logger.info(f"Tabela '{table_name}' já existe no Glue Catalog. Atualizando tabela...")
    glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    logger.info(f"Tabela '{table_name}' atualizada no Glue Catalog.")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    logger.info(f"Tabela '{table_name}' criada no Glue Catalog.")

repair_query = f"MSCK REPAIR TABLE `{database_name}`.`{table_name}`"
spark.sql(repair_query)
logger.info(f"REPAIR TABLE executado na tabela '{table_name}'.")

# fim

job.commit()
