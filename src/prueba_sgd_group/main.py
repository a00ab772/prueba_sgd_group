import json
import logging
import os
import sys

bucket_name = os.getenv("S3_BUCKET_CONFIG", "bucket-de-configuracion")

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
import pyspark.sql.functions as F

log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(log_level)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter(os.getenv("FORMATO_LOG", "%(message)s"))
    handler.setFormatter(formatter)
    logger.addHandler(handler)

carpeta_last = f"{os.path.abspath(os.path.dirname(__file__))}/resources"


class SparkHandler:
    def __init__(self, host, app_name):
        secret_name = os.getenv("SPARK_SECRET", "secreto-configuracion-de-spark")
        region_name = os.getenv("AWS_DEFAULT_REGION", "eu-central-1")

        session_aws = boto3.session.Session()
        client = session_aws.client(service_name="secretsmanager", region_name=region_name)

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response["SecretString"])

        spark_access_key = secret.get("SPARK_ACCESS_KEY_ID")
        spark_secret_key = secret.get("SPARK_SECRET_ACCESS_KEY")
        spark_driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")
        spark_executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

        self.spark_session = (
            SparkSession.builder
            .appName(app_name)
            .master(host)
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
            .config("spark.sql.warehouse.dir", "spark-warehouse")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("spark.files.fetchTimeout", "60")
            .config("spark.driver.memory", spark_driver_memory)
            .config("spark.executor.memory", spark_executor_memory)
            .config("spark.hadoop.fs.s3a.access.key", spark_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", spark_secret_key)
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @staticmethod
    def interpretador_de_dataflow(dataflow, year):
        spark = SparkSession.getActiveSession()
        dfs = {}

        logger.debug("""
        
        PROCESANDO LAS SECCIÓN DE 'inputs' ...
        ======================================
        """)
        for input_config in dataflow.get("inputs", []):
            name = input_config["name"]
            fmt = input_config["config"]["format"]
            path = input_config["config"]["path"]
            options = input_config.get("options", {})

            archivo_de_entrada = path.replace("{{ year }}", str(year))

            clean_path = archivo_de_entrada.lstrip("/")
            full_path = f"s3a://{bucket_name}/{clean_path}"

            reader = spark.read.format(fmt)
            for key, value in options.items():
                reader = reader.option(key, value)

            try:
                dfs[name] = reader.load(full_path)
            except Exception as e:
                logger.error(f"Fallo al cargar en Spark desde {full_path}: {e}")
                if hasattr(e, 'java_exception'):
                    logger.error(f"Excepción de Java: {e.java_exception}")
                raise e


        logger.debug(""" 
        
        [+] Atendiendo al enunciado de la prueba, únicamente vamos a procesar los datos provinientes de la clave 'input_data'. He asumido que la clave 'parquet_data' puede ignorarse, ya que entiendo que no aporta valor para los requisitos que queremos cumplir en esta prueba.  
        
        """)
        df_transformado = dfs["demo_data"]


        logger.debug("""
        
        PROCESANDO LAS SECCIÓN DE 'transformations' ...
        ===============================================
        """)
        transformations = dataflow["transformations"]

        for transformation in transformations:

            t_type = transformation["type"]
            t_config = transformation["config"]

            if t_type == "add_fields":
                for field in t_config["fields"]:
                    field_name = field["name"]
                    expr = field["expression"]
                    if expr.startswith("'") and expr.endswith("'"):
                        df_transformado = df_transformado.withColumn(field_name, lit(expr.strip("'")))
                    elif expr == "current_date()":
                        df_transformado = df_transformado.withColumn(field_name, current_date())

            elif t_type == "filter":
                df_transformado = df_transformado.filter(t_config["filter"])

            elif t_type == "group":
                group_fields = t_config["group_fields"]
                aggregations = t_config["aggregations"]

                # Nota: incluyo en el group el campo load_date para evitar que falle la escritura de la sección de 'output' que particiona por este campo
                columnas_de_particionado = [col for col in ["load_date"] if col in df_transformado.columns]
                columnas_de_agregacion = list(set(group_fields + columnas_de_particionado))

                condicion_de_agregacion = [F.expr(agg_str) for agg_str in aggregations]
                df_transformado = df_transformado.groupBy(*columnas_de_agregacion).agg(*condicion_de_agregacion)

            logger.debug(f" [+] Aplicamos al dataframe el siguiente elemento de la lista de 'transformations': '{t_type}' ==> {t_config}.\n\n")
            if logger.isEnabledFor(logging.DEBUG):
                df_transformado.show(truncate=False)

        logger.debug(f" [+] La variable 'df_transformado' contiene el resultado de todas las transformaciones aplicadas.\n")
        if logger.isEnabledFor(logging.DEBUG):
            df_transformado.show(truncate=False)
            df_transformado.explain()

        logger.debug("""
        
        ***************************************
        PROCESANDO LAS SECCIÓN DE 'outputs' ...
        ***************************************
        """)

        outputs = dataflow["outputs"]

        for output in outputs:
            out_name = output["name"]
            out_type = output["type"]
            out_input = output["input"]
            out_config = output["config"]

            if out_type == "file":
                ruta_salida = f"{carpeta_last}{out_config['path']}"
                file_format = out_config["format"]
                save_mode = out_config["save_mode"]

                writer = df_transformado.write.mode(save_mode)#.option("header", "true").option("inferSchema", "true")#.option("compression", "snappy")

                if "partition" in out_config:
                    writer = writer.partitionBy(out_config["partition"])

                if file_format == "parquet":
                    writer.parquet(ruta_salida)

                logger.debug(f" [+] Acción de escritura ===> '{out_type}'    |   '{save_mode}' | destino: '{ruta_salida}' {file_format} files.\n\n")

                logger.debug(f" [+] Adicionalmente escribo en {ruta_salida}/{out_name}.csv el csv resultante, para que podamos comprobar la salida con un editor de texto.\n\n")
                writer.csv(f"{ruta_salida}/{out_name}.csv")

            elif out_type == "table":
                out_table_name = out_config["table"]
                out_save_mode = out_config["save_mode"]
                region_name = os.getenv("AWS_DEFAULT_REGION", "eu-central-1")

                dynamodb_client = boto3.client("dynamodb", region_name=region_name)
                rows = df_transformado.collect()

                logger.debug(f""" 
                    [+] Escribiendo {len(rows)} registros en la tabla de DynamoDB: {out_table_name} (Modo: {out_save_mode}).
                    
                        He de resaltar que al haber elegido DynamoDB como base de datos de destino para persistir la salida
                        de tipo tabla, no es necesario distinguir entre 'append' y 'merge', ya que ambas serán resueltas
                        mediante dynamodb_client.put_item(...).
                        
                        Esto se debe a que en DynamoDB, un put_item funciona de manera nativa como un upsert,
                        creando el elemento si no existe o lo sobrescribe por completo si la clave primaria coincide. 
                    """)

                for row in rows:
                    item = {}
                    for field in df_transformado.columns:
                        val = row[field]
                        if val is not None:
                            if isinstance(val, (int, float)):
                                item[field] = {"N": str(val)}
                            else:
                                item[field] = {"S": str(val)}

                    dynamodb_client.put_item(
                        TableName=out_table_name,
                        Item=item
                    )

                logger.debug(
                    f" [+] Acción de escritura ===> '{out_type}' (DynamoDB) : '{out_table_name}' | Modo de escritura : '{out_save_mode}'.\n\n")

        return df_transformado
