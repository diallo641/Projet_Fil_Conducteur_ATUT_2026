import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, current_date, regexp_replace, input_file_name, when, desc, row_number
from pyspark.sql.window import Window
from etl_monitoring import logger, monitor_performance


# Buckets source et destination
EXTRACTION_BUCKET = os.environ.get("EXTRACTION_BUCKET")  
TRANSFORMATION_BUCKET = os.environ.get("TRANSFORMATION_BUCKET")  

# Colonnes finales
colonnes_finales = [
    "ISBN",
    "title",
    "author",
    "price",
    "category",
    "availability",
    "source"
]

@monitor_performance("run_transformation")
def run_transformation():
    
    # Initialisation Spark
    spark = (
        SparkSession.builder
        .appName(os.environ.get("SPARK_APP_NAME", "TransformBooksToProcessed"))
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", os.environ.get("MINIO_SSL", "false"))
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.500"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("🚀 Démarrage de la transformation ")

   
    # Lecture de tous les CSV
    logger.info("📥 Lecture des fichiers CSV depuis le bucket extraction")
    df = spark.read.option("header", "true").csv(EXTRACTION_BUCKET + "/*.csv")
    logger.info(f"📊 Nombre de lignes lues : {df.count()}")

    if df.rdd.isEmpty():
        logger.error("❌ Aucun fichier CSV trouvé dans le bucket extraction")
        raise Exception("Aucun fichier CSV trouvé dans le bucket extraction")

    
    #source via nom de fichier
    df = df.withColumn("filename", input_file_name())
    df = df.withColumn(
        "source",
        when(col("filename").contains("scraping"), "scraping")
        .when(col("filename").contains("api"), "api")
        .otherwise("csv")
    )

   
    # Colonnes manquantes
    for c in colonnes_finales:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
    df = df.select(colonnes_finales)


    # Nettoyage du prix
    df = df.withColumn(
        "price",
        regexp_replace(col("price"), "[^0-9.]", "").cast("double")
    )
    logger.info("💎 Nettoyage des prix effectué")

    
    # Suppression des doublons 
    # Garde la ligne avec le prix le plus élevé pour chaque ISBN/source
    window = Window.partitionBy("ISBN", "source").orderBy(desc("price"))
    df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    logger.info(f"✅ Suppression des doublons par ISBN/source, nombre de lignes après : {df.count()}")

   
    # Ajout date d'ajout
    df = df.withColumn("date_ajout", current_date())

   
    # Filtrer uniquement les 3 sources
    df = df.filter(col("source").isin(["csv", "scraping", "api"]))
    logger.info("✅ Filtrage des 3 sources terminé")

   
    # Écriture dans le bucket transformation
    df.write.mode("overwrite").partitionBy("source", "date_ajout").parquet(TRANSFORMATION_BUCKET)
    logger.info("📦 Données écrites dans MinIO (bucket processed) avec partition source/date_ajout")

    spark.stop()
    logger.info("✅ Transformation terminée avec succès sous Minio")


if __name__ == "__main__":
    run_transformation()