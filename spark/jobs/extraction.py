from pyspark.sql import SparkSession
import os
from etl_monitoring import logger, monitor_performance
import time

@monitor_performance("run_extraction")
def run_extraction(local_data_dir="/opt/spark/data", minio_bucket="s3a://extraction/"):
    """
    Fonction pour charger les fichiers CSV locaux vers MinIO
    avec logging structuré et suivi du temps d'exécution
    """
    spark = SparkSession.builder \
        .appName("LoadBooksToMinIO") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("🎉 Début de l'extraction des fichiers CSV vers MinIO")

    csv_files = [
        "livres_externe_clean.csv",
        "livres_scraping.csv",
        "api_livres.csv"
    ]

    for file_name in csv_files:
        local_path = os.path.join(local_data_dir, file_name)

        if not os.path.exists(local_path):
            logger.warning(f"⚠️ Fichier non trouvé : {local_path}")
            continue

        df = spark.read.csv(local_path, header=True, inferSchema=True)
        logger.info(f"✅ {file_name} lu : {df.count()} lignes")
        df.show(3, truncate=False)

        # Mesurer le temps du chargement
        start_time = time.time()
        minio_path = minio_bucket + file_name
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(minio_path)
        duration = time.time() - start_time
        logger.info(f"📦 {file_name} chargé dans MinIO : {minio_path} (durée {duration:.2f}s)")

    spark.stop()
    logger.info("🎉 Tous les fichiers CSV ont été chargés dans le bucket MinIO.")


# Utilisation  de la fonction pour exécuter dans le  fichier test
if __name__ == "__main__":
    run_extraction()
