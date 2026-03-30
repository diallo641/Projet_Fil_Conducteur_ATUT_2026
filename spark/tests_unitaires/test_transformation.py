import sys
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

#Mock du module manquant pour que l'import de run_transformation fonctionne
sys.modules["etl_monitoring"] = MagicMock()
sys.modules["etl_monitoring"].logger = MagicMock()
sys.modules["etl_monitoring"].monitor_performance = lambda x: (lambda f: f)

#Importer la fonction à tester après avoir mocké et défini les modules
from jobs.transformation import run_transformation

@pytest.fixture(scope="session")
def spark():
    """Créer une session Spark locale pour les tests"""
    return SparkSession.builder.master("local[*]").appName("Test_ETL").getOrCreate()


def test_transformation_local_dir(monkeypatch, spark):
    """
    Test de la transformation  en utilisant des dossiers locaux.
    """
    with tempfile.TemporaryDirectory() as tmp_raw_dir, tempfile.TemporaryDirectory() as tmp_processed_dir:

        #Rediriger les buckets vers des dossiers temporaires 
        monkeypatch.setenv("EXTRACTION_BUCKET", tmp_raw_dir)
        monkeypatch.setenv("TRANSFORMATION_BUCKET", tmp_processed_dir)

        #Création de CSV fictifs
        csv_content = (
            "ISBN,title,author,price,category,availability\n"
            "1,Book1,Author1,10.5,Fiction,yes\n"
            "2,Book2,Author2,12.0,Science,no"
        )
        filenames = [
            "books_externe_clean.csv",
            "books_scraping_minio.csv",
            "books_api.csv"
        ]
        for file_name in filenames:
            with open(os.path.join(tmp_raw_dir, file_name), "w") as f:
                f.write(csv_content)

        #Patch SparkSession pour que run_transformation utilise notre session locale
        with patch("jobs.transformation.SparkSession.builder.getOrCreate", return_value=spark):
            run_transformation()

        #Vérification : des fichiers Parquet doivent être créés dans tmp_processed_dir
        parquet_files = [f for f in os.listdir(tmp_processed_dir) if f.endswith(".parquet")]
        assert parquet_files, "Aucun fichier Parquet trouvé dans le dossier transformation local"

        #Vérification du contenu via Spark
        df = spark.read.parquet(tmp_processed_dir)
        assert df.count() == 6  
        assert "ISBN" in df.columns
        assert "title" in df.columns