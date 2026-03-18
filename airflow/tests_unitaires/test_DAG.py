import pytest
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from datetime import datetime, timedelta
from dags.ETL_pipeline import monitoring_pipeline

DAG_ID = "pipeline_ETL"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None
    assert len(dagbag.import_errors) == 0


def test_dag_id(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    assert dag.dag_id == DAG_ID


def test_default_args(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    default_args = dag.default_args
    assert default_args["owner"] == "ETL_TBD_ATUT_2026"
    assert default_args["depends_on_past"] is False
    assert default_args["email_on_failure"] is False
    assert default_args["email_on_retry"] is False
    assert default_args["retries"] == 2
    assert default_args["retry_delay"] == timedelta(minutes=2)


def test_schedule_and_catchup(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    assert dag.schedule_interval == "0 23 * * *"
    assert dag.catchup is False


def test_tasks_exist(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = {"extract_data", "lancer_spark", "transform_data", "load_curated", "report_monitoring"}
    assert set(task_ids) == expected_tasks


def test_task_types(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    task_types = {
        "extract_data": "BashOperator",
        "lancer_spark": "BashOperator",
        "transform_data": "BashOperator",
        "load_curated": "BashOperator",
        "report_monitoring": "PythonOperator"
    }
    for task_id, expected_type in task_types.items():
        assert dag.get_task(task_id).__class__.__name__ == expected_type


def test_dependencies(dagbag):
    dag = dagbag.get_dag(DAG_ID)

    assert "lancer_spark" in dag.get_task("extract_data").downstream_task_ids
    assert "transform_data" in dag.get_task("lancer_spark").downstream_task_ids
    assert "load_curated" in dag.get_task("transform_data").downstream_task_ids
    assert "report_monitoring" in dag.get_task("load_curated").downstream_task_ids


def test_monitoring_pipeline(monkeypatch, dagbag):
    """
    Teste la fonction monitoring_pipeline avec des TaskInstance simulées.
    """
    dag = dagbag.get_dag(DAG_ID)
    
    class DummyTI:
        def __init__(self, state="success", duration=None, start_date=None, end_date=None):
            self.state = state
            self.duration = duration
            self.start_date = start_date
            self.end_date = end_date
    
    # Créer un contexte factice
    dag_run = type("DagRun", (), {})()
    dag_run.get_task_instance = lambda task_id: DummyTI(
        state="success",
        duration=5.0,
        start_date=datetime(2026, 3, 17, 10, 0, 0),
        end_date=datetime(2026, 3, 17, 10, 5, 0)
    )
    context = {"dag_run": dag_run}
    
    # Appeler la fonction monitoring
    monitoring_pipeline(**context)
    
    # Tester également le cas où duration est None et start/end dates manquantes
    dag_run.get_task_instance = lambda task_id: DummyTI(
        state="failed",
        duration=None,
        start_date=None,
        end_date=None
    )
    monitoring_pipeline(**context)