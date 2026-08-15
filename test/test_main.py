import atexit
import json
import logging
import os
# Descomentad la linea para mostrar la salida logger.debug(...)
# os.environ["LOG_LEVEL"] = "DEBUG"
import sys
from unittest.mock import patch

import boto3
import pyspark
from moto import mock_aws
import pyspark.sql.functions as F
import unittest

from datetime import datetime
from src.prueba_sgd_group.main import SparkHandler, resources_path

log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(log_level)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logging.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark.SparkEnv").setLevel(logging.ERROR)

directorio_base = os.path.dirname(os.path.abspath(__file__))

# Uso variables globales para reutilizarlas en todas las clases de test
SPARK_HANDLER_GLOBAL = None
MOCK_AWS_GLOBAL = None
UNITTEST_LOAD_PATCHER_GLOBAL = None
UNITTEST_SAVE_PATCHER_GLOBAL = None

def setUpModule():
    global SPARK_HANDLER_GLOBAL, MOCK_AWS_GLOBAL, UNITTEST_LOAD_PATCHER_GLOBAL, UNITTEST_SAVE_PATCHER_GLOBAL

    # Mock del servidor de Airflow
    if "airflow" not in sys.modules:
        import types

        airflow_module = types.ModuleType("airflow")
        models_module = types.ModuleType("airflow.models")
        operators_module = types.ModuleType("airflow.operators")
        providers_module = types.ModuleType("airflow.providers")
        standard_module = types.ModuleType("airflow.providers.standard")
        std_operators_module = types.ModuleType("airflow.providers.standard.operators")
        python_module = types.ModuleType("airflow.providers.standard.operators.python")

        class MockDAG:
            def __init__(self, dag_id, *args, **kwargs):
                self.dag_id = dag_id
                self.tasks = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        class MockPythonOperator:
            def __init__(self, task_id, python_callable, *args, **kwargs):
                self.task_id = task_id
                self.python_callable = python_callable

            def execute(self, context):
                return self.python_callable()

        models_module.DAG = MockDAG
        python_module.PythonOperator = MockPythonOperator

        sys.modules["airflow"] = airflow_module
        sys.modules["airflow.models"] = models_module
        sys.modules["airflow.operators"] = operators_module
        sys.modules["airflow.providers"] = providers_module
        sys.modules["airflow.providers.standard"] = standard_module
        sys.modules["airflow.providers.standard.operators"] = std_operators_module
        sys.modules["airflow.providers.standard.operators.python"] = python_module

    # Mock de la conexión con AWS
    MOCK_AWS_GLOBAL = mock_aws()
    MOCK_AWS_GLOBAL.start()

    secrets_client = boto3.client("secretsmanager", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
    secrets_client.create_secret(
        Name=os.getenv("SPARK_SECRET", "secreto-configuracion-de-spark"),
        SecretString=json.dumps({
            "SPARK_ACCESS_KEY_ID": "AKIAIOSFODNN7SUP3RS3CR3T",
            "SPARK_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYSUP3RS3CR3TKEY"
        })
    )

    s3_client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
    bucket_name = os.getenv("S3_BUCKET_CONFIG", "bucket-de-configuracion")
    file_key = "metadata/metadata.json"

    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": os.getenv("AWS_DEFAULT_REGION", "eu-central-1")}
    )

    archivo_metadata_json_local = f"{resources_path}/metadata.json"
    assert os.path.exists(archivo_metadata_json_local), "El fichero metadata.json no existe en resources"

    with open(archivo_metadata_json_local, "r", encoding="utf-8") as f:
        contenido_archivo_metadata_json_local = f.read()

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=contenido_archivo_metadata_json_local.encode("utf-8")
    )

    input_files = [
        "data2024.csv",
        "data2025.csv",
        "poblacion2024.csv",
        "poblacion2025.csv"
    ]

    for file_name in input_files:
        local_file_path = f"{resources_path}/data/demo/input/{file_name}"
        assert os.path.exists(local_file_path), f"El fichero {file_name} no existe en resources/data/demo/input"

        with open(local_file_path, "rb") as f:
            file_content = f.read()

        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"data/demo/input/{file_name}",
            Body=file_content
        )

    dynamodb_client = boto3.client("dynamodb", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
    for table_name in ["demo", "raw_demo"]:
        try:
            dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "municipio", "KeyType": "HASH"},
                    {"AttributeName": "provincia", "KeyType": "RANGE"}
                ],
                AttributeDefinitions=[
                    {"AttributeName": "municipio", "AttributeType": "S"},
                    {"AttributeName": "provincia", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST"
            )
        except Exception as e:
            logger.info(f"La tabla de DynamoDB mock '{table_name}' ya existe o se omitió: {e}")

    # Configuración local de Spark y Hadoop
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"
    os.environ["S3_BUCKET_CONFIG"] = "bucket-de-configuracion"
    os.environ["SPARK_SECRET"] = "secreto-configuracion-de-spark"
    os.environ["SPARK_DRIVER_MEMORY"] = "2g"
    os.environ["SPARK_EXECUTOR_MEMORY"] = "2g"

    SPARK_HANDLER_GLOBAL = SparkHandler(host="local[*]", app_name="Orquestador")
    hadoop_conf = SPARK_HANDLER_GLOBAL.spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("hadoop.native.lib", "false")

    original_load = pyspark.sql.readwriter.DataFrameReader.load

    def mocked_load(self, path=None):
        if path and "s3a://" in path:
            parts = path.replace("s3a://", "").split("/", 1)
            relative_path = parts[1] if len(parts) > 1 else parts[0]
            local_file = os.path.join(resources_path, relative_path)

            if os.path.exists(local_file) and (not os.path.isdir(local_file) or os.listdir(local_file)):
                return original_load(self, local_file)

            spark = SPARK_HANDLER_GLOBAL.spark_session
            return spark.createDataFrame([], schema="id STRING")

        return original_load(self, path)

    original_save = pyspark.sql.readwriter.DataFrameWriter.save

    def mocked_save(self, path=None, format=None, mode=None, **options):
        if path and "s3a://" in path:
            parts = path.replace("s3a://", "").split("/", 1)
            relative_path = parts[1] if len(parts) > 1 else parts[0]
            local_file = os.path.join(resources_path, relative_path)

            os.makedirs(os.path.dirname(local_file), exist_ok=True)

            return original_save(self, path=local_file, format="parquet", mode=mode, **options)

        return original_save(self, path=path, format=format, mode=mode, **options)

    UNITTEST_LOAD_PATCHER_GLOBAL = patch("pyspark.sql.readwriter.DataFrameReader.load", mocked_load)
    UNITTEST_LOAD_PATCHER_GLOBAL.start()

    UNITTEST_SAVE_PATCHER_GLOBAL = patch("pyspark.sql.readwriter.DataFrameWriter.save", mocked_save)
    UNITTEST_SAVE_PATCHER_GLOBAL.start()


def tearDownModule():
    global SPARK_HANDLER_GLOBAL, MOCK_AWS_GLOBAL, UNITTEST_LOAD_PATCHER_GLOBAL, UNITTEST_SAVE_PATCHER_GLOBAL
    if UNITTEST_SAVE_PATCHER_GLOBAL:
        UNITTEST_SAVE_PATCHER_GLOBAL.stop()
    if UNITTEST_LOAD_PATCHER_GLOBAL:
        UNITTEST_LOAD_PATCHER_GLOBAL.stop()

    if SPARK_HANDLER_GLOBAL and SPARK_HANDLER_GLOBAL.spark_session:
        try:
            SPARK_HANDLER_GLOBAL.spark_session.stop()
        except Exception:
            pass

    if MOCK_AWS_GLOBAL:
        try:
            MOCK_AWS_GLOBAL.stop()
        except Exception:
            pass


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark_handler = SPARK_HANDLER_GLOBAL
        cls.spark_session = cls.spark_handler.spark_session
        cls.dynamodb_client = boto3.client("dynamodb", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
        cls.s3_client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
        cls.bucket_name = os.getenv("S3_BUCKET_CONFIG", "bucket-de-configuracion")
        cls.file_key = "metadata/metadata.json"


class Test_de_los_apartados_1_al_4(TestBase):

    def test_interpreta_json(self):
        with open(f"{resources_path}/metadata.json", "r", encoding="utf-8") as f:
            data_dict = json.load(f)

        for year in ["2024", "2025"]:
            for dataflow in data_dict["dataflows"]:
                df_transformado = self.spark_handler.interpretador_de_dataflow(dataflow=dataflow, year=year)

                count_na = df_transformado.filter("municipio == 'N/A'").count()
                self.assertEqual(count_na, 0)
                count_ambos_sexos = df_transformado.filter("sexo == 'Ambos sexos'").count()
                self.assertEqual(count_ambos_sexos, 0)
                self.assertEqual(df_transformado.count(), 15410)

                out_config = dataflow.get("outputs", [{}])[0].get("config", {})
                table_name = out_config.get("table", "demo")

                response = self.dynamodb_client.scan(TableName=table_name)
                items_guardados = response.get("Items", [])
                self.assertGreater(len(items_guardados), 0)

                # TODO: Es necesario demostrar el comportamiento solicitado en las salidas (overwrite, append y merge).
                #       De momento cuento los registros insertados en dynamodb, pero cuando tenga un rato libre haré un
                #       reporte que permita más claramente ver lo que pasa con esos tres tipos de salida, imprimiendo
                #       algunos valores en cada caso a modo ilustrativo.

class TestAirflow(TestBase):

    def test_creacion_y_ejecucion_dag_airflow_mockeado(self):
        from airflow.models import DAG
        from airflow.providers.standard.operators.python import PythonOperator
        with DAG(
                dag_id="planificacion_tarea_airflow",
                start_date=datetime(2026, 8, 7),
                schedule="*/5 * * * *",
        ) as dag:

            def lanza_interpretador_de_dataflow():
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
                data_dict = json.loads(response["Body"].read().decode("utf-8"))

                for year in ["2024", "2025"]:
                    for dataflow in data_dict.get("dataflows", []):
                        df_result = self.spark_handler.interpretador_de_dataflow(dataflow=dataflow, year=year)

                        assert df_result is not None
                        assert df_result.count() > 0

                        count_na = df_result.filter("municipio == 'N/A'").count()
                        assert count_na == 0

                        count_ambos = df_result.filter("sexo == 'Ambos sexos'").count()
                        assert count_ambos == 0

            task = PythonOperator(
                task_id="ejecutar_interpretador_spark",
                python_callable=lanza_interpretador_de_dataflow
            )

        self.assertIsNotNone(dag)
        task.execute(context={})


class Test_del_apartado_5(TestBase):

    def test_filas_distintas_entre_csv_2024_y_csv_2025(self):
        data2024 = (
            self.spark_handler.spark_session.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(f"{resources_path}/data/demo/input/data2024.csv")
        )

        data2025 = (
            self.spark_handler.spark_session.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(f"{resources_path}/data/demo/input/data2025.csv")
        )

        filas_solo_en_data2024 = data2024.exceptAll(data2025)
        filas_solo_en_data2025 = data2025.exceptAll(data2024)

        self.assertEqual(filas_solo_en_data2024.count(), 8)
        self.assertEqual(filas_solo_en_data2025.count(), 8)

    def test_discrepancias_en_datos_agregados_en_los_csv_de_entrada(self):
        for year in ["2024", "2025"]:
            df = (
                self.spark_handler.spark_session.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv(f"{resources_path}/data/demo/input/data{year}.csv")
            )

            df_agrupado_por_sexo = df.groupBy("provincia", "municipio").agg(
                F.sum(F.when(F.col("sexo") == "Ambos sexos", F.col("total")).otherwise(0)).alias("ambos_sexos"),
                F.sum(F.when(F.col("sexo") == "Hombres", F.col("total")).otherwise(0)).alias("hombres"),
                F.sum(F.when(F.col("sexo") == "Mujeres", F.col("total")).otherwise(0)).alias("mujeres")
            )
            num_municipios = df_agrupado_por_sexo.count()

            df_discrepancias = df_agrupado_por_sexo.filter(
                F.abs(F.col("ambos_sexos") - (F.col("hombres") + F.col("mujeres"))) > 0.01
            ).withColumn(
                "diferencia",
                F.abs(F.col("ambos_sexos") - (F.col("hombres") + F.col("mujeres"))).cast("long")
            ).orderBy(F.col("diferencia").asc())
            num_discrepancias = df_discrepancias.count()

            df_concordancias = df_agrupado_por_sexo.exceptAll(
                df_discrepancias.drop("diferencia")
            ).withColumn(
                "diferencia",
                F.lit(0).cast("long")
            )
            num_concordancias = df_concordancias.count()

            if year == "2024":
                self.assertEqual(num_discrepancias, 235)
                self.assertEqual(num_concordancias, 7521)
                self.assertEqual(num_municipios, num_discrepancias + num_concordancias)
            else:
                self.assertEqual(num_discrepancias, 236)
                self.assertEqual(num_concordancias, 7520)
                self.assertEqual(num_municipios, num_discrepancias + num_concordancias)

def limpiar_hilos_al_finalizar_el_test_de_pycharm():
    try:
        active_spark = pyspark.sql.SparkSession.getActiveSession()
        if active_spark:
            active_spark.sparkContext.stop()
    except Exception:
        pass
    os._exit(0)

atexit.register(limpiar_hilos_al_finalizar_el_test_de_pycharm)

if __name__ == "__main__":
    unittest.main()
