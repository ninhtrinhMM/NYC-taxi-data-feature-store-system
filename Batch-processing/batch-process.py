from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import json

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
        .config("spark.sql.debug.maxToStringFields", "1000")

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
        .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/postgresql-42.7.1.jar")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("--- Spark Session đã khởi tạo thành công!---")
    
    # ======= Đọc file ======
    version_tracker_file = "/opt/airflow/Project-Feature-Store/delta_version_tracker.json"
    
    if not os.path.exists(version_tracker_file):
        print("------ Không có thông tin version tracker!------")
        spark.stop()
        return
    
    with open(version_tracker_file, 'r') as f:
        version_info = json.load(f)
    
    if not version_info.get("has_new_data", False):
        print("------ Không có dữ liệu mới!------")
        spark.stop()
        return
    
    last_version = version_info["last_processed_version"]
    current_version = version_info["current_version"]
    
    print(f"--- Có {version_info['new_files_count']} file mới (version {last_version} → {current_version})----")
    
    # ======= ĐỌC DỮ LIỆU MỚI TỪ DELTA TABLE =======
    delta_path = "/opt/airflow/Project-Feature-Store/Deltatable"
    
    if last_version == -1:
        # Lần đầu tiên, đọc toàn bộ
        print("--- Đọc toàn bộ Delta Table lần đầu ---")
        df = spark.read.format("delta").load(delta_path)
    else:
        print(f"--- Đọc incremental từ version {last_version + 1} đến {current_version} ---")
        df_cdf = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", last_version + 1) \
            .option("endingVersion", current_version) \
            .load(delta_path)
    # Lọc chỉ lấy các bản ghi mới (inserts)
        df = df_cdf.filter("_change_type IN ('insert', 'update_postimage')") \
                   .drop("_change_type", "_commit_version", "_commit_timestamp")
    
    row_count = df.count()
    print(f"--- Tổng số dòng cần upload: {row_count}")
    
    # =========CẤU HÌNH POSTGRESQL CONNECTION=========
    postgres_config = {
        "url": "jdbc:postgresql://172.17.0.1:5434/k6",
        "dbtable": "NYC",
        "user": "<....>",
        "password": "<......>",
        "driver": "org.postgresql.Driver"}
    
    # Kiểm tra bảng PostgreSQL có tồn tại không, nếu không thì tạo mới
    print(f"-----Kiểm tra bảng PostgreSQL '{postgres_config['dbtable']}' tồn tại...------")
    try:
        df_check = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", postgres_config["dbtable"]) \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .load()
        table_exists = True
        print(f"-----Bảng '{postgres_config['dbtable']}' đã tồn tại.------")
    except Exception as e:
        table_exists = False
        print(f"-----Bảng '{postgres_config['dbtable']}' không tồn tại. Sẽ tạo mới khi ghi dữ liệu.------")
    
    # ========GHI DỮ LIỆU VÀO POSTGRESQL==========
    print(f"-------Bắt đầu Upload dữ liệu lên PostgreSQL----------")
    
    try:
        write_mode = "append"  # Luôn append vì chỉ ghi dữ liệu mới
        
        print(f" --- Uploading {row_count} dòng to PostgreSQL...")
        
        df.write \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", postgres_config["dbtable"]) \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .mode(write_mode) \
            .save()
        
        print(f"----- Upload thành công {row_count} dòng-----")
        
        # ===== CẬP NHẬT VERSION TRACKER =====
        with open(version_tracker_file, 'w') as f:
            json.dump({}, f, indent=2)
        
    except Exception as e:
        print(f"   Lỗi khi upload: {e}")
        spark.stop()
        return
    
    spark.stop()
    print("\n Hoàn tất!")

if __name__ == "__main__":
    main()
