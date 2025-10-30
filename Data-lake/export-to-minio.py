import os
import json
import yaml
from utils.helpers import load_cfg
from minio.error import S3Error
from minio import Minio

# Tạo hàm đọc file config.yaml
def load_cfg(config_file):
    """
    Load configuration from a YAML config file
    """
    cfg = None
    with open(config_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    return cfg

def main():

    #Đọc file new_files_output.json để lấy đường dẫn thư mục cần upload
    with open("/opt/airflow/Project-Feature-Store/new_files_output.json", 'r') as f:
        new_files = json.load(f)
        
    # Nếu new_files rỗng
    if not new_files:
        print("------⚠️ Không có file mới để upload!------")
        return
    # Nếu tìm thấy 
    print(f"🆕 Tìm thấy {len(new_files)} file mới cần xử lý:")

    #KẾT NỐI ĐẾN MINIO 
    
    cfg = load_cfg("/opt/airflow/scripts/Data-lake/utils/config.yaml")
    datalake_cfg = cfg["datalake"]

    print(f"------📦 Chuẩn bị cấu hình của {datalake_cfg['endpoint']}------") 

    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,)

    print(f".... Xác thực bucket tại {datalake_cfg['endpoint']}...")

    found = minio_client.bucket_exists(bucket_name=datalake_cfg["bucket_name"])
    if not found:
        minio_client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
    else:
        print(f'Bucket {datalake_cfg["bucket_name"]} already exists, skip creating!')

    print(f".... ✅ Kết nối thành công đến MinIO: {datalake_cfg['endpoint']}...")

    print(f"------📂 Bắt đầu upload lên MinIO------")
    success_file = 0
    failed_files = [] 

    for file in new_files:
        if not os.path.exists(file):
            print(f"  ⚠️  File {os.path.basename(file)} không tồn tại")
            continue
        try:
            minio_client.fput_object(
                bucket_name= datalake_cfg["bucket_name"],
                object_name= f"{datalake_cfg['folder_name']}/{os.path.basename(file)}" ,
                file_path=file,)
            
        except S3Error as e:
            print(f"  ❌ Lỗi upload: {e}")
            failed_files.append(os.path.basename(file))
        except Exception as e:
            print(f"  ❌ Lỗi không xác định: {e}")
            failed_files.append(os.path.basename(file))

        success_file += 1
        print(f"-----Hoàn tất upload file {os.path.basename(file)} lên MinIO, còn lại {len(new_files) - success_file} file------")

    print(f"------✅ Hoàn tất upload {success_file}/{len(new_files)} lên MinIO!------")

    # ==== Nếu có file thất bại =======

    if not failed_files:
        print(" Tất cả file đã được upload thành công!") 

    print(f" Thất bại {len(failed_files)} file")
    print("---Danh sách file thất bại:")
    for f in failed_files:
        print(f"  - {f}")

    
if __name__ == "__main__":
    main()