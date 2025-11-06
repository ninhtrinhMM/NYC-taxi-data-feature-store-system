from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from glob import glob
import os
import json
from delta.tables import DeltaTable

def main():
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("Incremental Delta Table Load")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.debug.maxToStringFields", "1000")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Danh sách các path quan trọng
    delta_path = "/opt/airflow/Project-Feature-Store/Deltatable"  # Delta table

    checkpoint_file = "/opt/airflow/Project-Feature-Store/processed_files.json"  # Checkpoint file để so sánh
    new_files_output= "/opt/airflow/Project-Feature-Store/new_files_output.json"  # lưu trữ danh sách file mới phát hiện

    version_tracker_file = "/opt/airflow/Project-Feature-Store/delta_version_tracker.json"  # File theo dõi version
    
    # ========== ĐỌC CHECKPOINT FILE ==========
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            processed_files = set(json.load(f))
        print(f"📋 Đã có {len(processed_files)} file được xử lý trước đó")
    else:
        processed_files = set()
        with open(checkpoint_file, 'w') as f:
            json.dump([], f)
        print("📋 Tạo mới file processed_files.json và chưa có file nào được xử lý")
    
    # ========== ĐỌC new_files_output ==========

    if not os.path.exists(new_files_output): # Nếu chưa có file thì tạo mới
        with open(new_files_output, 'w') as f:
            json.dump([], f)
        print("📋 Tạo mới file new_files_output.json")
    else:    
        print("📋 Đã có sẵn new_files_output.json")

    # ========= Đọc version tracker file ==========
    
    if os.path.exists(version_tracker_file):
        print("---- Đã có sẵn file version tracker-----")
    else:
        with open(version_tracker_file, 'w') as f:
            json.dump({}, f)
    
    # ========== LẤY DANH SÁCH FILE MỚI ==========
    parquet_files = glob("/opt/airflow/NYC-data/*.parquet")
    parquet_files.sort()
    
    new_files = [f for f in parquet_files if f not in processed_files]
    
    if not new_files: #new_files rỗng
        print("=== Không có file mới nào cần xử lý! ===")
        spark.stop()
        return
    
    #### Nếu tìm thấy, bắt đầu xử lý
    print(f"----Tìm thấy {len(new_files)} file mới cần xử lý----")
    
    # ======== HÀM ÉP KIỂU VỀ STRING ======
    def to_string(df):
        for field in df.schema.fields:
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
        return df
    
    # Hàm ép tên các cột thành lower case

    def lowercase_columns(df):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        return df
    
    # ========= LƯU VERSION TRƯỚC KHI GHI =========
    
    if os.path.exists(delta_path) and os.listdir(delta_path): 
        delta_table = DeltaTable.forPath(spark, delta_path) #lập đelta table từ path
        version_before = delta_table.history(1).select("version").collect()[0][0] # lấy version hiện tại
        print(f"==== Delta Table hiện tại ở version {version_before} ====")
    else:
        version_before = -1
        print("=== Vì Delta Table chưa tồn tại, nên version_before ép bằng -1 ===")
    
    # ========= XỬ LÝ TỪNG FILE MỚI VÀ GHI VÀO DELTA TABLE =========

    print("=== Bắt đầu xử lý các file mới và ghi vào Delta Table......")

    for idx, parquet in enumerate(new_files):
        name = os.path.splitext(os.path.basename(parquet))[0]
        print(f"-----{idx+1}/{len(new_files)}: Bắt đầu với file {name} vào Deltatable-----")
    
        df = spark.read.parquet(parquet)

        df= lowercase_columns(df)
        df_fix = to_string(df)
    
        if version_before == -1 :
            print(f"===  Bắt đầu tạo Delta Table với CDF enabled .... ===")
            df_fix.write.format("delta") \
                .mode("overwrite") \
                .option("delta.enableChangeDataFeed", "true") \
                .save(delta_path)
            version_before = 0  # Cập nhật version_before khác với -1 để các lần sau vào nhánh else
            print(f"=== ✅ Thành công tạo Delta Table ===")
        else:
           
            df_fix.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(delta_path)
            print(f"=== ✅ Append file {name} vào Delta Table ===")
        
        # CẬP NHẬT CHECKPOINT
        processed_files.add(parquet)
        with open(checkpoint_file, 'w') as f:
            json.dump(list(processed_files), f, indent=2)
        
        print(f"-----✅ File {name} đã được xử lý, còn lại {len(new_files) - (idx + 1)} file-----")
    
    print(f"==== Hoàn thành ghi {len(new_files)} file vào Delta Table====")

    # ====Lưu version sau khi ghi ======

    delta_table = DeltaTable.forPath(spark, delta_path)
    version_after = delta_table.history(1).select("version").collect()[0][0]

    print(f"==== Delta Table sau khi ghi có version là: {version_after} ====")
    
    with open(version_tracker_file, 'w') as f:
        json.dump({
            "last_processed_version": version_before,
            "current_version": version_after,
            "new_files_count": len(new_files),
            "has_new_data": True
        }, f, indent=2)

    # ====== Cập nhập các file mới vào new_files_output.json ======
    with open(new_files_output, 'w') as f:
        json.dump(new_files, f, indent=2)
    
    print("\n" + "="*60 + "TỔNG KẾT")
    
    df_final = spark.read.format("delta").load(delta_path)
    total_rows = df_final.count()
    print(f"✅ Tổng số dòng trong Delta Table: {total_rows}")
    print(f"✅ Số file mới vừa thêm: {len(new_files)}")
    print(f"✅ Delta Table version: {version_before} → {version_after}")
    
    print("\n📋 Sample data (10 dòng):")
    df_final.show(10, truncate=False)
    
    spark.stop()
    print("----- Hoàn tất!")

if __name__ == "__main__":
    main()
