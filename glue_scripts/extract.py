import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# minhas importações
import boto3
import logging
from datetime import datetime

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

bucket_name = "s3://fiap-etl-tech-challenge-2-540692057042"
today_date = datetime.now().strftime("%Y-%m-%d")
parquet_file_path = f"{bucket_name}/scraped-data/{today_date}.parquet"

df = spark.read.parquet(parquet_file_path)

df = df.withColumnRenamed("Código", "code") \
       .withColumnRenamed("Ação", "stock") \
       .withColumnRenamed("Tipo", "stock_type") \
       .withColumnRenamed("Qtde. Teórica", "theoretical_quantity") \
       .withColumnRenamed("Part. (%)", "participation_percentage")

logger.info("Schema do DataFrame:")
df.printSchema()

# root
#  |-- code: string (nullable = true)
#  |-- stock: string (nullable = true)
#  |-- stock_type: string (nullable = true)
#  |-- theoretical_quantity: string (nullable = true)
#  |-- participation_percentage: string (nullable = true)
#  |-- date: string (nullable = true)

# salvando dados no bucket
raw_data_path = f"{bucket_name}/raw/"
df.write.mode("append").partitionBy("date").parquet(raw_data_path)

glue_client = boto3.client("glue")

database_name = "fiap-etl-tech-challenge"
table_name = "tb_b3_raw"
table_location = raw_data_path

# verifica se o banco de dados existe e crie se necessário
try:
    glue_client.get_database(Name=database_name)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(
        DatabaseInput={'Name': database_name}
    )
    logger.info(f"Banco de dados '{database_name}' criado no Glue Catalog.")

# definição da tabela
table_input = {
    'Name': table_name,
    'StorageDescriptor': {
        'Columns': [
            {"Name": "code", "Type": "string"},
            {"Name": "stock", "Type": "string"},
            {"Name": "stock_type", "Type": "string"},
            {"Name": "theoretical_quantity", "Type": "string"},
            {"Name": "participation_percentage", "Type": "string"}
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
    'PartitionKeys': [{"Name": "date", "Type": "string"}],
    'TableType': 'EXTERNAL_TABLE'
}

# cria ou atualiza a tabela no Glue Catalog
try:
    glue_client.get_table(DatabaseName=database_name, Name=table_name)
    logger.info(f"Tabela '{table_name}' já existe no Glue Catalog. Atualizando tabela...")
    glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    logger.info(f"Tabela '{table_name}' atualizada no Glue Catalog.")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    logger.info(f"Tabela '{table_name}' criada no Glue Catalog.")

logger.info(f"Tabela '{table_name}' disponível no Athena.")

# executa MSCK REPAIR TABLE para descobrir partições
repair_table_query = f"MSCK REPAIR TABLE `{database_name}`.`{table_name}`"
logger.info(f"Executando comando: {repair_table_query}")
spark.sql(repair_table_query)
logger.info(f"Comando MSCK REPAIR TABLE executado com sucesso para a tabela '{table_name}'.")

# fim

job.commit()
