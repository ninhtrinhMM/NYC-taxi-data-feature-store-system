from pyspark.sql import SparkSession

# Tạo SparkSession có Delta
spark = (
    SparkSession.builder.appName("DeltaTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Tạo DataFrame nhỏ để test
data = [("A", 30), ("B", 25), ("C", 40)]
df = spark.createDataFrame(data, ["name", "age"])

print("Row count:", df.count())

# Đường dẫn output Delta table (anh đổi path theo máy anh)
output_path = "/home/ninhtrinhmm/Project-Feature-Store/DeltaTest"

# Ghi ra Delta
df.write.format("delta").mode("overwrite").save(output_path)

print("✅ Delta table saved at:", output_path)
