import pytest


def test_dag_import():
    from dags.etl_process_dag import dag

    assert dag is not None