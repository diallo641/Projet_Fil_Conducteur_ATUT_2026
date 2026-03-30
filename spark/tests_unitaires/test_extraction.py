import sys
import os
import tempfile
import glob
import pytest
from pyspark.sql import SparkSession

#Mock du module manquant pour que l'import de run_extraction fonctionne
from unittest.mock import MagicMock
sys.modules["etl_monitoring"] = MagicMock()
sys.modules["etl_monitoring"].logger = MagicMock()
sys.modules["etl_monitoring"].monitor_performance = lambda x: (lambda f: f)

# Maintenant on peut importer ton extraction
from jobs.extraction import run_extraction

@pytest.fixture(scope="module")
def spark():
    """Fournit une session Spark pour les tests"""
    spark_session = (
        SparkSession.builder
        .appName("Test_extraction")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_extraction_local(spark):
    """
    Test de l'extraction CSV vers un dossier local simulant MinIO.
    Vérifie que chaque CSV est écrit dans un dossier séparé et que le contenu est correct.
    """
    with tempfile.TemporaryDirectory() as tmp_data_dir:
        with tempfile.TemporaryDirectory() as tmp_output_dir:
            # Création de CSV fictifs
            csv_content = "id,title\n1,book1\n2,book2"
            filenames = [
                "livres_externe_clean.csv",
                "livres_scraping.csv",
                "api_livres.csv"
            ]

            for file_name in filenames:
                with open(os.path.join(tmp_data_dir, file_name), "w") as f:
                    f.write(csv_content)

            # Exécution de l'extraction vers un dossier local
            run_extraction(
                local_data_dir=tmp_data_dir,
                minio_bucket=tmp_output_dir  # simulateur de MinIO
            )

            # Vérifie que Spark a créé un dossier par CSV
            written_folders = glob.glob(os.path.join(tmp_output_dir, "*"))
            assert len(written_folders) == 3, f"Attendu 3 dossiers, trouvé {len(written_folders)}"

            # Vérifie le contenu de chaque dossier
            for folder in written_folders:
                csv_files = glob.glob(os.path.join(folder, "*.csv"))
                assert len(csv_files) == 1, f"Aucun CSV trouvé dans {folder}"

                df = spark.read.csv(csv_files[0], header=True, inferSchema=True)
                assert df.count() == 2, f"Nombre de lignes incorrect dans {csv_files[0]}"
                assert list(df.columns) == ["id", "title"], f"Colonnes incorrectes dans {csv_files[0]}"