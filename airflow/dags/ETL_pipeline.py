from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
import os


# Arguments par défaut du DAG
default_args = {
    "owner": "ETL_TBD_ATUT_2026",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# Fonction de monitoring du pipeline
def monitoring_pipeline(**context):
    log = LoggingMixin().log
    dag_run = context["dag_run"]
    extraction = dag_run.get_task_instance("extract_data")
    transformation = dag_run.get_task_instance("transform_data")
    chargement = dag_run.get_task_instance("load_curated")

    log.info("Monitoring global du pipeline ETL Livres")

    if extraction:
        log.info(
            f"📦 extract_data | état={extraction.state} | durée={extraction.duration:.2f}s"
            if extraction.duration else
            f"📦 extract_data | état={extraction.state}"
        )

    if transformation:
        log.info(
            f"⚙️ transform_data | état={transformation.state} | durée={transformation.duration:.2f}s"
            if transformation.duration else
            f"⚙️ transform_data | état={transformation.state}"
        )

    if chargement:
        log.info(
            f"📤 load_curated | état={chargement.state} | durée={chargement.duration:.2f}s"
            if chargement.duration else
            f"📤 load_curated | état={chargement.state}"
        )

    if extraction and chargement and extraction.start_date and chargement.end_date:
        total_duration = (chargement.end_date - extraction.start_date).total_seconds()
        log.info(f"⏱️ Durée totale du pipeline : {total_duration:.2f}s")
    else:
        log.warning("Impossible de calculer la durée totale")


# Définition du DAG
with DAG(
    dag_id="pipeline_ETL",
    default_args=default_args,
    description="Pipeline ETL Livres vers MinIO",
    schedule_interval="0 23 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "spark", "minio", "livres"],
) as dag:

    # Extraction
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="python3 /opt/spark/jobs/extraction.py",
        env=os.environ,
    )

    # Démarrage Spark
    lancer_spark = BashOperator(
        task_id="lancer_spark",
        bash_command="docker start spark_final || true",
    )

    # Transformation
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="""
        docker exec -i --user root spark_final bash -c "
        export SPARK_LOCAL_IP=127.0.0.1

        /opt/spark/bin/spark-submit \
        --conf spark.driver.host=127.0.0.1 \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.local.hostname=localhost \
        --conf spark.jars.ivy=/tmp/ivy \
        --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark/jobs/transformation.py
        "
        """,
        env=os.environ,
    )

    # Chargement
    load_curated = BashOperator(
        task_id="load_curated",
        bash_command="""
        docker exec -i --user root spark_final bash -c "
        export SPARK_LOCAL_IP=127.0.0.1

        /opt/spark/bin/spark-submit \
        --conf spark.driver.host=127.0.0.1 \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.local.hostname=localhost \
        --conf spark.jars.ivy=/tmp/ivy \
        --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark/jobs/chargement.py
        "
        """,
        env=os.environ,
    )

    # Monitoring
    report_monitoring = PythonOperator(
        task_id="report_monitoring",
        python_callable=monitoring_pipeline,
    )

    # Pipeline
    extract_data >> lancer_spark >> transform_data >> load_curated >> report_monitoring