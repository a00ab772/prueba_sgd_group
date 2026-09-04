import atexit
import json
import logging
import os
import csv
import shutil

# Descomentad la linea para mostrar la salida logger.debug(...)
# os.environ["LOG_LEVEL"] = "DEBUG"
# Descomentad la linea que os interese mostrar en el logging
# os.environ["FORMATO_LOG"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# os.environ["FORMATO_LOG"] = "%(levelname)s - %(message)s"
os.environ["FORMATO_LOG"] = "%(message)s"

import sys
from unittest.mock import patch

import boto3
import pyspark
from moto import mock_aws
import pyspark.sql.functions as F
import unittest

from datetime import datetime
from src.prueba_sgd_group.main import SparkHandler, carpeta_resources

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

        # Mapeo completo en sys.modules para que coincida con la ruta de importación
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

    archivo_metadata_json_local = f"{carpeta_resources}/metadata.json"
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
        local_file_path = f"{carpeta_resources}/data/demo/input/{file_name}"
        assert os.path.exists(local_file_path), f"El fichero {file_name} no existe en resources/data/demo/input"

        with open(local_file_path, "rb") as f:
            file_content = f.read()

        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"data/demo/input/{file_name}",
            Body=file_content
        )

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
            local_file = os.path.join(carpeta_resources, relative_path)

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
            local_file = os.path.join(carpeta_resources, relative_path)

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
        cls.s3_client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
        cls.bucket_name = os.getenv("S3_BUCKET_CONFIG", "bucket-de-configuracion")
        cls.file_key = "metadata/metadata.json"

    def setUp(self):
        for table in ["demo", "raw_demo"]:
            try:
                self.spark_session.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass


class Test_de_los_apartados_1_al_4(TestBase):

    def test_interpreta_json(self):
        with open(f"{carpeta_resources}/metadata.json", "r", encoding="utf-8") as f:
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

                if self.spark_session.catalog.tableExists(table_name):
                    df_tabla = self.spark_session.table(table_name)
                    self.assertGreater(df_tabla.count(), 0)


class TestOverwriteAppendMerge(TestBase):

    def setUp(self):
        super().setUp()
        rutas_a_limpiar = [
            f"{carpeta_resources}/data_reducido/demo/output",
            f"{carpeta_resources}/data_1_registro/demo/output",
            "spark-warehouse"
        ]
        for ruta in rutas_a_limpiar:
            if os.path.exists(ruta):
                try:
                    shutil.rmtree(ruta)
                except Exception:
                    pass

    def _imprimir_contenido_csv(self, path_csv):
        logger.info("-" * 100)
        logger.info(f"\n[+] Leyendo archivo de entrada {path_csv}")

        if os.path.exists(path_csv):
            with open(path_csv, mode="r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=';')
                filas_csv = list(reader)

            if filas_csv:
                cabecera = filas_csv[0]
                datos = filas_csv[1:]

                logger.info(f"\n    Contenido del archivo {path_csv} ({len(datos)} registros):\n")

                ancho_columnas = {i: len(col) for i, col in enumerate(cabecera)}
                for fila in datos:
                    for i, val in enumerate(fila):
                        if i < len(ancho_columnas) and len(str(val)) > ancho_columnas[i]:
                            ancho_columnas[i] = len(str(val))

                cabecera_str = " | ".join(col.ljust(ancho_columnas[i]) for i, col in enumerate(cabecera))
                logger.info(f"    {cabecera_str}")

                linea_separadora = "-+-".join("-" * ancho_columnas[i] for i in range(len(cabecera)))
                logger.info(f"    {linea_separadora}")

                for fila in datos:
                    row_str = " | ".join(
                        str(val).ljust(ancho_columnas[i]) if i < len(ancho_columnas) else str(val) for i, val in
                        enumerate(fila))
                    logger.info(f"    {row_str}")
            else:
                logger.info(f"    El fichero {path_csv} está vacío.")
        else:
            logger.info(f"    No se encuentra el fichero CSV en la ruta: {path_csv}")

        logger.info("-" * 100)

    def _imprimir_contenido_parquet_historic_en_formato_csv(self, carpeta_historic: str):

        if os.path.exists(carpeta_historic):
            csv_encontrados = []
            for root, dirs, files in os.walk(carpeta_historic):
                for file in files:
                    if file.endswith(".csv"):
                        csv_encontrados.append(os.path.join(root, file))

            if csv_encontrados:
                todas_filas = []
                cabecera = None

                for path_csv in csv_encontrados:
                    with open(path_csv, mode="r", encoding="utf-8") as f:
                        reader = csv.reader(f, delimiter=',')
                        filas_csv = list(reader)

                    if filas_csv:
                        if cabecera is None:
                            cabecera = filas_csv[0]
                            todas_filas.extend(filas_csv[1:])
                        else:
                            if filas_csv[0] == cabecera:
                                todas_filas.extend(filas_csv[1:])
                            else:
                                todas_filas.extend(filas_csv)

                if cabecera and todas_filas:
                    logger.info(
                        f"\n    Contenido unificado de todos los CSVs almacenados en la carpeta parquet 'historic' ({len(todas_filas)} registros):\n")

                    ancho_columnas = {i: len(col) for i, col in enumerate(cabecera)}
                    for fila in todas_filas:
                        for i, val in enumerate(fila):
                            if i < len(ancho_columnas) and len(str(val)) > ancho_columnas[i]:
                                ancho_columnas[i] = len(str(val))

                    cabecera_str = " | ".join(col.ljust(ancho_columnas[i]) for i, col in enumerate(cabecera))
                    logger.info(f"    {cabecera_str}")

                    linea_separadora = "-+-".join("-" * ancho_columnas[i] for i in range(len(cabecera)))
                    logger.info(f"    {linea_separadora}")

                    for fila in todas_filas:
                        row_str = " | ".join(
                            str(val).ljust(ancho_columnas[i]) if i < len(ancho_columnas) else str(val) for i, val in
                            enumerate(fila))
                        logger.info(f"    {row_str}")
                else:
                    logger.info(f"    Los archivos CSV encontrados están vacíos.")
            else:
                logger.info(f"    No se encontraron archivos CSV dentro de {carpeta_historic}")
        else:
            logger.info(f"    No se encuentra el directorio historic en la ruta: {carpeta_historic}")

        logger.info("-" * 100)

    def _imprimir_contenido_parquet(self, path_parquet: str, df_referencia):
        df_parquet = df_referencia.interpretador_de_dataflow.__globals__[
            'SparkSession'].getActiveSession().read.parquet(path_parquet) if hasattr(self.spark_handler,
                                                                                     'spark') else df_referencia.sql_ctx.read.parquet(
            path_parquet)
        registros_parquet = df_parquet.collect()
        logger.info(f"\n   Contenido actual de las filas en el Parquet ({len(registros_parquet)} registros):")
        if registros_parquet:
            cols = df_parquet.columns
            anchos = {col: len(col) for col in cols}
            for fila in registros_parquet:
                for col in cols:
                    val_len = len(str(fila[col]))
                    if val_len > anchos[col]:
                        anchos[col] = val_len
            logger.info(f"    {' | '.join(col.ljust(anchos[col]) for col in cols)}")
            logger.info(f"    {'-+-'.join('-' * anchos[col] for col in cols)}")
            for fila in registros_parquet:
                logger.info(f"    {' | '.join(str(fila[col]).ljust(anchos[col]) for col in cols)}")

    def test_informe_overwrite_append_merge_CSV_1_registro(self):
        with open(f"{carpeta_resources}/metadata_csv_entrada_1_registro.json", "r", encoding="utf-8") as f:
            data_dict = json.load(f)

        dataflow = data_dict["dataflows"][0]

        logger.info("=" * 100)
        logger.info("INFORME OVERWRITE, APPEND Y MERGE CON CSV CON 1 REGISTRO")
        logger.info("=" * 100)
        logger.info("""

        A continuación vamos a leer los archivos csv de entrada de 2024 y 2025 en dos rondas, mostrando en cada una de ellas las
        salidas overwrite, append y merge que hemos obtenido.
        """)
        for ronda in [1, 2]:
            logger.info(f"\nRonda {ronda}\n")
            for year in ["2024", "2025"]:
                self._imprimir_contenido_csv(
                    f"{carpeta_resources}/data_1_registro/demo/input/archivo_1_registro_{year}.csv")

                df_transformado = self.spark_handler.interpretador_de_dataflow(dataflow=dataflow, year=year)

                logger.info("\n1. Salida tipo 'file' con save_mode overwrite (/data_1_registro/demo/output/last):")
                last_path = f"{carpeta_resources}/data_1_registro/demo/output/last"
                if os.path.exists(last_path):
                    self._imprimir_contenido_parquet(last_path, df_transformado)

                logger.info("\n2. Salida tipo 'file' con save_mode append (/data_1_registro/demo/output/historic):")
                carpeta_historic = f"{carpeta_resources}/data_1_registro/demo/output/historic"
                if os.path.exists(carpeta_historic):
                    self._imprimir_contenido_parquet_historic_en_formato_csv(carpeta_historic)

                logger.info("\n3. Salida tipo 'table' Delta (modo append):")
                nombre_tabla_demo = "demo"
                if self.spark_session.catalog.tableExists(nombre_tabla_demo):
                    df_demo = self.spark_session.table(nombre_tabla_demo)
                    logger.info(
                        f"\n   La tabla Delta '{nombre_tabla_demo}' tiene {df_demo.count()} registros (modo append):")
                    df_demo.show(truncate=False)
                    self.assertGreater(df_demo.count(), 0)

                logger.info("\n4. Salida tipo 'table' Delta (modo merge):")
                nombre_tabla_raw = "raw_demo"
                if self.spark_session.catalog.tableExists(nombre_tabla_raw):
                    df_raw = self.spark_session.table(nombre_tabla_raw)
                    logger.info(
                        f"\n   La tabla Delta '{nombre_tabla_raw}' tiene {df_raw.count()} registros (modo merge):")
                    df_raw.show(truncate=False)
                    self.assertGreater(df_raw.count(), 0)

                logger.info("\n" + "=" * 100)

    def test_informe_overwrite_append_merge_CSV_REDUCIDOS(self):
        with open(f"{carpeta_resources}/metadata_csv_entrada_reducidos.json", "r", encoding="utf-8") as f:
            data_dict = json.load(f)

        dataflow = data_dict["dataflows"][0]

        logger.info("=" * 100)
        logger.info("INFORME OVERWRITE, APPEND Y MERGE CON CSV REDUCIDOS")
        logger.info("=" * 100)
        logger.info("""

        A continuación vamos a leer los archivos csv de entrada de 2024 y 2025 en dos rondas, mostrando en cada una de ellas las
        salidas overwrite, append y merge que hemos obtenido.
        """)

        for ronda in [1, 2]:
            logger.info(f"\nRonda {ronda}\n")
            for year in ["2024", "2025"]:
                df_transformado = self.spark_handler.interpretador_de_dataflow(dataflow=dataflow, year=year)

                self._imprimir_contenido_csv(
                    f"{carpeta_resources}/data_reducido/demo/input/archivo_reducido_{year}.csv")

                logger.info("\n1. Salida tipo 'file' con save_mode overwrite (/data_reducido/demo/output/last):")
                last_path = f"{carpeta_resources}/data_reducido/demo/output/last"
                if os.path.exists(last_path):
                    self._imprimir_contenido_parquet(last_path, df_transformado)

                logger.info("\n2. Salida tipo 'file' con save_mode append (/data_reducido/demo/output/historic):")
                carpeta_historic = f"{carpeta_resources}/data_reducido/demo/output/historic"
                if os.path.exists(carpeta_historic):
                    self._imprimir_contenido_parquet_historic_en_formato_csv(carpeta_historic)

                logger.info("\n3. Salida tipo 'table' Delta (modo append):")
                nombre_tabla_demo = "demo"
                if self.spark_session.catalog.tableExists(nombre_tabla_demo):
                    df_demo = self.spark_session.table(nombre_tabla_demo)
                    logger.info(
                        f"\n   La tabla Delta '{nombre_tabla_demo}' tiene {df_demo.count()} registros (modo append):")
                    df_demo.show(truncate=False)
                    self.assertGreater(df_demo.count(), 0)

                logger.info("\n4. Salida tipo 'table' Delta (modo merge):")
                nombre_tabla_raw = "raw_demo"
                if self.spark_session.catalog.tableExists(nombre_tabla_raw):
                    df_raw = self.spark_session.table(nombre_tabla_raw)
                    logger.info(
                        f"\n   La tabla Delta '{nombre_tabla_raw}' tiene {df_raw.count()} registros (modo merge):")
                    df_raw.show(truncate=False)
                    self.assertGreater(df_raw.count(), 0)

                logger.info("\n" + "=" * 100)

    def test_informe_overwrite_append_merge(self):
        with open(f"{carpeta_resources}/metadata.json", "r", encoding="utf-8") as f:
            data_dict = json.load(f)

        dataflow = data_dict["dataflows"][0]

        logger.info("""

         =================================
         INFORME overwrite, append y merge
         =================================
        """)

        for year in ["2024", "2025"]:
            df_transformado = self.spark_handler.interpretador_de_dataflow(dataflow=dataflow, year=year)
            logger.info(f" [+] Año procesado: {year} | Total registros: {df_transformado.count()}")

            logger.info(
                "1. Salida tipo 'file' con save_mode overwrite (ubicación de salida: /data/demo/output/last) ...")
            last_path = f"{carpeta_resources}/data/demo/output/last"
            if os.path.exists(last_path):
                files_last = [os.path.join(dp, f) for dp, dn, filenames in os.walk(last_path) for f in filenames if
                              f.endswith(".parquet")]
                logger.info(
                    f" [+] [OVERWRITE] Directorio '{last_path}' existe. Ficheros parquet encontrados tras finalizar bucle: {len(files_last)}")

            logger.info(
                "2. Salida tipo file con save_mode append y particionado (ubicació: /data/demo/output/historic)")
            carpeta_historic = f"{carpeta_resources}/data/demo/output/historic"
            if os.path.exists(carpeta_historic):
                partitions = os.listdir(carpeta_historic)
                logger.info(
                    f" [+] [APPEND] Directorio '{carpeta_historic}' particionado. Particiones encontradas: {partitions}")

            logger.info("3. Salida tipo table Delta (modo append)")
            nombre_tabla = "demo"
            if self.spark_session.catalog.tableExists(nombre_tabla):
                count_table = self.spark_session.table(nombre_tabla).count()
                logger.info(f"La tabla Delta '{nombre_tabla}' tiene {count_table} elementos insertados en modo append.")
                self.assertGreater(count_table, 0)

            logger.info("4. Salida tipo table Delta (modo merge)")
            nombre_tabla_raw = "raw_demo"
            if self.spark_session.catalog.tableExists(nombre_tabla_raw):
                count_table_raw = self.spark_session.table(nombre_tabla_raw).count()
                logger.info(
                    f"La tabla Delta '{nombre_tabla_raw}' tiene {count_table_raw} elementos insertados en modo merge.")
                self.assertGreater(count_table_raw, 0)

            logger.info("==================================================\n")


class TestAirflow(TestBase):

    def test_creacion_y_ejecucion_dag_airflow_mockeado(self):
        from airflow.models import DAG
        from airflow.providers.standard.operators.python import PythonOperator

        # Limpieza previa del metastore y del directorio spark-warehouse
        for table in ["demo", "raw_demo"]:
            try:
                self.spark_session.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass

        rutas_a_limpiar = [
            f"{carpeta_resources}/data/demo/output",
            f"{carpeta_resources}/data_reducido/demo/output/spark-warehouse",
            "spark-warehouse"
        ]
        for ruta in rutas_a_limpiar:
            if os.path.exists(ruta):
                try:
                    shutil.rmtree(ruta)
                except Exception:
                    pass

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

                        out_config = dataflow.get("outputs", [{}])[0].get("config", {})
                        table_name = out_config.get("table", "demo")

                        if self.spark_session.catalog.tableExists(table_name):
                            assert self.spark_session.table(table_name).count() > 0

            task = PythonOperator(
                task_id="ejecutar_interpretador_spark",
                python_callable=lanza_interpretador_de_dataflow
            )

        self.assertIsNotNone(dag)
        task.execute(context={})


class Test_del_apartado_5(TestBase):

    def test_filas_distintas_entre_csv_2024_y_csv_2025(self):
        logger.info("""
        La función test_filas_distintas_entre_csv_2024_y_csv_2025 quiere responder a la pregunta... 

        ¿Qué filas han cambiado del fichero de 2024 al fichero de 2025?
        """)
        data2024 = (
            self.spark_handler.spark_session.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(f"{carpeta_resources}/data/demo/input/data2024.csv")
        )

        data2025 = (
            self.spark_handler.spark_session.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(f"{carpeta_resources}/data/demo/input/data2025.csv")
        )

        filas_solo_en_data2024 = data2024.exceptAll(data2025)
        filas_solo_en_data2025 = data2025.exceptAll(data2024)

        count_2024 = filas_solo_en_data2024.count()
        count_2025 = filas_solo_en_data2025.count()

        logger.info("""
            Filas de data2024.csv que no estan en data2025.csv...
            """)
        for fila in filas_solo_en_data2024.collect():
            logger.info(f" {fila.asDict()}")

        logger.info("""
            Filas de data2025.csv que no estan en data2024.csv...
            """)
        for fila in filas_solo_en_data2025.collect():
            logger.info(f" {fila.asDict()}")

        logger.info(
            f"\n\n► Hay {count_2024} filas en el archivo data2024.csv que no existen en el archivo data2025.csv")
        self.assertEqual(count_2024, 8, msg=f"Error en filas 2024: Se esperaban 8 pero se obtuvieron {count_2024}")

        logger.info(f"\n► Hay {count_2025} filas en el archivo data2025.csv que no existen en el archivo data2024.csv")
        self.assertEqual(count_2024, 8,
                         msg=f"Error en la verificación del archivo data2024.csv: Se esperaban 8 filas que no estuvieran en el archivo data2025.csv, pero en realidad hay {count_2024} filas.")
        self.assertEqual(count_2025, 8,
                         msg=f"Error en la verificación del archivo data2025.csv: Se esperaban 8 filas que no estuvieran en el archivo data2024.csv, pero en realidad hay {count_2025} filas.")

    def test_discrepancias_en_datos_agregados_en_los_csv_de_entrada(self):
        logger.info("""
        La función test_discrepancias_en_datos_agregados_en_los_csv_de_entrada quiere responder a la pregunta... 

        ¿Qué problemas de calidad del dato tiene el fichero de 2025 (Ambos sexos != Hombres + Mujeres)?

        Para resolverlo, agrego los datos del csv por provincia y municipio y comparo el resultado de sumar hombres y mujeres con el número consignado en la fila "Ambos sexos".

        Nota: He extendido la evaluación para que ambos csv. 
        """)
        for year in ["2024", "2025"]:
            df = (
                self.spark_handler.spark_session.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv(f"{carpeta_resources}/data/demo/input/data{year}.csv")
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

            exp_discrepancias = 235 if year == "2024" else 236
            exp_concordancias = 7521 if year == "2024" else 7520

            logger.info(f"""
            data{year}.csv
            ..............
             - Número de filas agregadas por provincia y municipio en las que la (Ambos sexos != Hombres + Mujeres) : {num_discrepancias}
             - Número de filas agregadas por provincia y municipio en las que la (Ambos sexos == Hombres + Mujeres) : {num_concordancias}
             - Número de municipios: {num_municipios}""")

            self.assertEqual(
                num_discrepancias, exp_discrepancias,
                msg=f"El archivo data{year}.csv tiene {num_discrepancias} filas en las que la (Ambos sexos != Hombres + Mujeres). Este número es incorrecto, esperabamos que hubiesen {exp_discrepancias}."
            )
            self.assertEqual(
                num_concordancias, exp_concordancias,
                msg=f"El archivo data{year}.csv tiene {num_concordancias} filas en las que la (Ambos sexos == Hombres + Mujeres). Este número es incorrecto, esperabamos que hubiesen {exp_concordancias}."
            )
            self.assertEqual(
                num_municipios, num_discrepancias + num_concordancias,
                msg=f"[{year}] La suma de municipios no coincide con el total de [(Ambos sexos == Hombres + Mujeres) + (Ambos sexos != Hombres + Mujeres)]"
            )


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