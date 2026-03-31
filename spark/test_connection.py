# Quick test — verifies Spark can connect to Postgres via JDBC

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ConnectionTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jdbc_url  = "jdbc:postgresql://postgres:5432/employee_db"
jdbc_props = {
    "user":     "spark_user",
    "password": "spark_pass123",
    "driver":   "org.postgresql.Driver"
}

try:
    df = spark.read.jdbc(url=jdbc_url, table="employees_clean", properties=jdbc_props)
    print("\n Connection successful!")
    print("   Schema:")
    df.printSchema()
    print(f"   Row count: {df.count()}")
except Exception as e:
    print(f"\n Connection failed: {e}")
finally:
    spark.stop()