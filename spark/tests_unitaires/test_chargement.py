import sys
import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

#Mock etl_monitoring
sys.modules["etl_monitoring"] = MagicMock()
sys.modules["etl_monitoring"].logger = MagicMock()
sys.modules["etl_monitoring"].monitor_performance = lambda x: (lambda f: f)

from jobs.chargement import run_chargement


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("TestChargement").getOrCreate()


def test_chargement_local(monkeypatch, spark):
    """
    Test simple du chargement TRANSFORMATION vers CHARGEMENT en local
    """

    with tempfile.TemporaryDirectory() as tmp_transformation, \
         tempfile.TemporaryDirectory() as tmp_chargement:

        #Variables d'environnement
        monkeypatch.setenv("TRANSFORMATION_BUCKET", tmp_transformation)
        monkeypatch.setenv("CHARGEMENT_BUCKET", tmp_chargement)
        monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test")
        monkeypatch.setenv("MINIO_SECRET_KEY", "test")

        #Données simulées
        data = [
            ("1", "Book1", "A1", 10.5, "Fiction", "yes", "scraping"),
            ("2", "Book2", "A2", 12.0, "Science", "no", "api"),
            ("3", "Book3", "A3", 8.0, "Fiction", "yes", "csv"),
        ]

        columns = ["ISBN", "title", "author", "price", "category", "availability", "source"]

        df = spark.createDataFrame(data, columns)

        #Écriture dans transformation (input)
        df.write.mode("overwrite").parquet(tmp_transformation)

        # Patch Spark pour éviter config S3
        with patch("jobs.chargement.SparkSession.builder.getOrCreate", return_value=spark):
            run_chargement()

        #Vérifier que des fichiers existent
        files = os.listdir(tmp_chargement)
        assert files, "Aucun fichier créé dans chargement"

        #recréer Spark car il a été stoppé
        spark_new = SparkSession.builder.master("local[*]").appName("TestChargement2").getOrCreate()

        #Lire le résultat
        df_result = spark_new.read.parquet(tmp_chargement)

        #Vérifications
        assert df_result.count() == 3
        assert "source_clean" in df_result.columns

        sources = set(row["source_clean"] for row in df_result.select("source_clean").collect())
        assert sources == {"scraping", "api", "csv"}