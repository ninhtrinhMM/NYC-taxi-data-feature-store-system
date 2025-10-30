from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def main():
    # Spark Config 
    print("-----khởi tạo Spark Session...------")
    
    builder = (
        SparkSession.builder
        .master("local[16]")
        .appName("MinIO to PostgreSQL")
        
        # Delta Lake configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # MinIO/S3 configs
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")  
        .config("spark.hadoop.fs.s3a.access.key", "duongninh")  
        .config("spark.hadoop.fs.s3a.secret.key", "anninhhuyen098")  
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # Nếu MinIO không dùng SSL

        .config("spark.sql.shuffle.partitions", "16")  # bằng luồng CPU
        .config("spark.default.parallelism", "16")  # bằng luồng CPU

        .config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "14g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.memory.fraction", "0.8") # Tăng bộ nhớ cho các thao tác tính toán

        # OFF-HEAP MEMORY
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "5g")  

        # Bổ sung JARs cho Hadoop AWS và AWS SDK + postgreSQL để độc và ghi trên S3 và PostgreSQL
        .config("spark.jars", "/home/ninhtrinhmm/spark-jars/hadoop-aws-3.3.4.jar,/home/ninhtrinhmm/spark-jars/aws-java-sdk-bundle-1.12.262.jar,/home/ninhtrinhmm/spark-jars/postgresql-42.7.1.jar")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("---✅ Spark Session đã khởi tạo thành công!---")
    
    # ĐỌC DELTA TABLE TỪ MINIO
    
    print(f"-----📥 Đọc Delta Table từ MinIO -----")

    try:
        df = spark.read.format("delta").load("s3a://mlop2/nycdata")
        
        print(f"-----✅ Đọc thành công! Tổng số dòng: {df.count():,}-----")

    except Exception as e:
        print(f"---❌ Lỗi khi đọc từ MinIO: {e}---")
        spark.stop()
        return
    
    # CẤU HÌNH POSTGRESQL CONNECTION

    postgres_config = {
        "url": "jdbc:postgresql://localhost:5434/k6",  # k6 là tên database
        "dbtable": "NYC",  # Khởi tạo tên table
        "user": "k6",  
        "password": "k6",  
        "driver": "org.postgresql.Driver"
    }

    # ========GHI DỮ LIỆU VÀO POSTGRESQL================

    print(f"-----💾 Đang ghi dữ liệu vào PostgreSQL table '{postgres_config['dbtable']}'......")
    
    try:
        df.repartition(100).write \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", postgres_config["dbtable"]) \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .option("batchsize", "1000") \
            .mode("overwrite") \
            .save()
        
        print(f"✅ Đã ghi thành công vào PostgreSQL!")
        
    except Exception as e:
        print(f"❌ Lỗi khi ghi vào PostgreSQL: {e}")
        spark.stop()
        return

    spark.stop()
    print("\n🎉 Hoàn tất! Spark Session đã đóng.")

if __name__ == "__main__":
    main()