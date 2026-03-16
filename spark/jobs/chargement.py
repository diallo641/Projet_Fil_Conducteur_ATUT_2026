
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from etl_monitoring import logger, monitor_performance  


# Fonction d'uniformisation
def uniformiser(df):
    """
    Fonction de curation pour la couche CURATED
    """
    df_clean = df.withColumn(
        "source_clean",
        when(col("source") == "scraping", "scraping")
        .when(col("source") == "api", "api")
        .otherwise("csv")
    )
    return df_clean


# Main avec monitoring
@monitor_performance("run_chargement")
def run_chargement():
    logger.info("🚀 Démarrage de la couche")

    # Variables d'environnement
    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]

    TRANSFORMATION_BUCKET = os.environ["TRANSFORMATION_BUCKET"]
    CHARGEMENT_BUCKET = os.environ["CHARGEMENT_BUCKET"]

    # Initialisation Spark
    spark = (
        SparkSession.builder
        .appName(os.environ.get("SPARK_APP_NAME", "CuratedBooks"))
        .config(
            "spark.jars",
            "/opt/spark/libs/hadoop-aws-3.3.6.jar,"
            "/opt/spark/libs/aws-java-sdk-bundle-1.12.500.jar"
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            os.environ.get("MINIO_SSL", "false")
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Lecture
        logger.info(f"📥 Lecture de la couche TRANSFORMATION depuis {TRANSFORMATION_BUCKET}")
        df = spark.read.parquet(TRANSFORMATION_BUCKET)
        logger.info(f"📊 Nombre de lignes lues : {df.count()}")

        if df.rdd.isEmpty():
            logger.error("❌ Aucun fichier trouvé dans le bucket transformation")
            sys.exit(1)

        # Curation
        logger.info("🧹 Nettoyage des données")
        df_clean = uniformiser(df)

        # Écriture CURATED
        logger.info(f"📤 Écriture vers {CHARGEMENT_BUCKET}")
        df_clean.coalesce(1) \
            .write \
            .mode("overwrite") \
            .partitionBy("source_clean") \
            .parquet(CHARGEMENT_BUCKET)

        logger.info("✅ Chargement terminé avec succès !")

    except Exception as e:
        logger.error(f"❌ Erreur lors du traitement : {e}")
        sys.exit(1)

    finally:
        spark.stop()


# Execution
if __name__ == "__main__":
    run_chargement()