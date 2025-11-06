# 1. Giới thiệu tổng quan: 

## a. Tổng quan về dữ liệu NYC taxi: 

Bộ Dataset là các bản record lịch trình di chuyển của taxi vàng ( Yellow Taxi ) và xanh lá ( Green Taxi ) bao gồm các trường dữ liệu ghi lại ngày/giờ đón và trả khách, địa điểm đón và trả khách, quãng đường di chuyển, chi tiết cước phí, loại hình giá cước, hình thức thanh toán và số lượng hành khách do tài xế báo cáo.

Dữ liệu được sử dụng trong các tập dữ liệu đính kèm đã được thu thập và cung cấp cho Ủy ban Taxi và Xe thuê (TLC) của NYC bởi các nhà cung cấp công nghệ được ủy quyền theo Chương trình Nâng cao Trải nghiệm Hành khách Taxi và Xe thuê (TPEP/LPEP).

Hồ sơ chuyến đi của Xe Cho Thuê Có Tài Xế (“FHV”) bao gồm các trường dữ liệu ghi lại số giấy phép của hãng điều phối (dispatching base license number) và ngày, giờ, cùng với ID vị trí khu vực taxi (taxi zone location ID) đón khách. Dữ liệu chuyến đi được công bố hàng tháng trên trang web này, thường là chậm hai tháng để có đủ thời gian cho các nhà cung cấp gửi đầy đủ dữ liệu. Do kích thước lớn của các tập dữ liệu, các tệp hồ sơ chuyến đi đã được lưu trữ dưới định dạng PARQUET.

<img width="1024" height="640" alt="Image" src="https://github.com/user-attachments/assets/45490bb8-87e4-4cb3-996b-88264fec88c3" />

## b. Mục tiêu của dự án:

Với số lượng và dung lượng lớn các file parqute, đòi hỏi chúng ta phải thiết kế 1 hệ thống lữu trữ dữ liệu, phục vụ update dữ liệu mới được upload vào ổ local, tức xử lý dữ liệu theo Batch và dữ liệu được sinh ra ( generate ) liên tục theo Real-time, được gọi là Streaming. Đây được gọi là kiến trúc Lambda Architecture. 

## c. Sơ đồ dự án: 
------------

# 2. Chuẩn bị: 

## a. Kéo repo từ Github về ( sử dụng hệ điều hành Ubuntu ):

Mở Terminal ( Ctrl + Alt +T ) và gõ lần lượt các lệnh sau:

* git init
* git clone https://github.com/ninhtrinhMM/NYC-taxi-data-feature-store-system
* Ngay sau đó toàn bộ Github Repo từ link trên sẽ được tải về và hiển thị trong Folder tên là NYC-taxi-data-feature-store-system ở máy local, được gọi là Repo local. Mở VS Code và open Folder trên.
  
Mở file docker-compose.yml lên và tiến hành chỉnh sửa USER và PASSWORD của container posrgreSQL-db và datalake-minio

<img width="743" height="250" alt="Image" src="https://github.com/user-attachments/assets/e7668f0f-0d68-4702-a6be-3d261b9078b5" />

Mở file config.yaml ở folder Data-lake/utils, chỉnh sửa tên bucket và access key và secret key, lưu ý không dược viết hoa và có ký tự "-"

<img width="584" height="234" alt="Image" src="https://github.com/user-attachments/assets/213a134a-00a0-4a0e-82c7-b0ba43db95dd" />

Mở file batch-process.py nằm trong folder Batch-processing và điền USER và PASS giống với container posrgreSQL-db ban nãy 

<img width="597" height="166" alt="Image" src="https://github.com/user-attachments/assets/0728e77a-da0f-4d71-9ac5-629751adc5d3" />

Ở file airflow-docker-compose-CeleryExe.yml, thay đổi phần USER và PASS

<img width="757" height="288" alt="Image" src="https://github.com/user-attachments/assets/b8779289-a6df-4bc0-b892-cac5d4157b6a" />

## b. Công cụ chuẩn bị: 

* Dbeaver
* Git
* Dowload các file JAR sau:
* Tạo sẵn 1 Folder tên "Project-Feature-Store" để chứa folder Delta Table và các file cần thiết

## c. Dowload dữ liệu NYC-taxi: 



# 3. Triển khai: 

## a. Khởi tạo môi trường: 

```conda create -n NYC python=3.10.18``` && ```conda activate NYC``` && ```pip install -r requirement.txt```

## b. Khởi tạo hạ tầng: 

Chạy: ```docker compose -f docker-compose.yml up -d```

NOTE: lưu ý kiểm tra và đảm bảo đủ các container đã được vận hành thành công

<img width="1077" height="188" alt="Image" src="https://github.com/user-attachments/assets/18897a88-8e97-41f1-aa5b-b7da30094dec" />

## c. Khởi tạo luồng dữ liệu Streaming và lưu trữ: 

Vì container stream-producer đã được vận hành trong file docker compose trên nên dữ liệu đã được tạo ra và gửi message liên tục vào Topic "device-1" trong Kafka Broker. Truy cập localhost:9021, ấn vào phần "Topic" để check tình trạng sức khỏe của Topic "device-1", nếu là Healthy như trong ảnh thì tức là vận hành thành công. 

<img width="1478" height="499" alt="Image" src="https://github.com/user-attachments/assets/c8b67cae-d624-47bf-b896-62627e46c749" />

Tiếp theo, để dữ liệu trong topic "device-1" được xử lý real-time bởi Pyflink ( trường hợp này không áp dụng Water-mark stradetegy ) rồi đưa vào Topic khác tên là "NYC" với vai trò là đầu ra, chạy lệnh sau: 

```python Streaming-processing/datastream_api.py```

Chạy xong nếu hiển thị ở terminal là : ---Đọc Folder JAR thành công !--- thì nghĩa là đã vận hành file thành công.

Note: trong trường hợp chạy file datastream_api.py gặp lỗi ```025-10-29 09:35:20,031 main ERROR FileManager (/opt/flink/log/flink-ninhtrinhmm-python-ninhtrinhmm-berserker-7540.log) java.io.FileNotFoundException: ``` thì khởi tạo lại folder log của flink nhưu sau:
```sudo mkdir -p /opt/flink/log``` && ```sudo chown -R $USER:$USER /opt/flink/log``` && ```sudo chmod -R 755 /opt/flink/log```

Lưu ý không được tắt Terminal này, nếu tắt thì luồng xử lý dữ liệu Real Time sẽ không còn nữa. Mọi command khác đều phải thực hiện ở Terminal khác. 

Trở lại localhost:9021 để check xem tình trạng của 2 Topic là "device-1" và "NYC", nếu đều healthy thì nghĩa là vận hành thành công. 

<img width="1444" height="501" alt="Image" src="https://github.com/user-attachments/assets/e1a09ef7-d6a0-4bf7-8620-b9ea56c435f6" />

Tiếp theo, chúng ta cần có 1 table ở PostgreSQL để chứa các dữ liệu mới được sinh ra ở Topic "NYC". Để làm được, tiến hành khởi tạo 1 Kafka Connector có nhiệm vụ đưa dữ liệu từ Topic "NYC" vào table tại PostgreSQL, bằng cách gửi cấu hình của Connector tới container flink-connect ( service Connect )

Cấu hình của Connector ( có chứa cấu hình của PostgreSQL luôn ) như sau: 

<img width="857" height="456" alt="Image" src="https://github.com/user-attachments/assets/18374075-5565-4142-852a-8fc86fadca45" />

* "name": tên của connector
* "topics": Tên của Topic mà Connector lấy dữ liệu, ở đây là topic "NYC"
* "connection.url": địa chỉ của PostgreSQL, bao gồm cả tên Database
* "connection.user" và "connection.pass": user và pass của PostgreSQL
* "auto.create": tự tạo table mới nếu chưa có trước đó
* "table.name.format": tên table được tạo ra để chứa dữ liệu từ Topic "NYC"

Tiến hành khởi tạo Connector bằng lệnh sau: ```cd Streaming-processing``` && ```bash run.sh register_connector ./kafka-connect/connect-sink.json```

Để đảm bảo Connector "connector-db" đã được vận hành tốt, chạy lệnh sau: ```curl -s http://localhost:8083/connectors/connector-db/status | python3 -m json.tool```

Nếu thấy status là RUNNING nhưu trong hình thì nghĩa là vận hành thành công

<img width="1533" height="297" alt="Image" src="https://github.com/user-attachments/assets/c96b16cd-b096-4a4b-9973-ee053402a352" />

Bật Dbeaver lên, nhập đúng tên Database, USER và PASS ( ở file docker-compose.yml ). Vào các trường k6 / schemas / public / Tables thì sẽ thấy table mới tên là "nyc_stream" nhưu trong hình 

<img width="1305" height="579" alt="Image" src="https://github.com/user-attachments/assets/deea51da-90c3-4b92-9878-dad80913e0ac" />

## d. Khởi tạo Airflow để xử lý dữ liệu theo Batch và lưu trữ:

Trước hết, để Airflow có quyền chỉnh sửa trên folder "Project-Feature-Store", chạy các lệnh sau ở Terminal Ubuntu ( ctrl + Alt + T ): 

```chmod -R 777 ./Project-Feature-Store```
```sudo chown -R 50000:0 < Path dẫn đến Folder "Project-Feature-Store" >```

Phần mount volume của airflow-worker có những lưu ý về các path local như sau: 

<img width="849" height="278" alt="Image" src="https://github.com/user-attachments/assets/76159904-eb55-4a79-bd37-0bc939ccf807" />

* ${AIRFLOW_PROJ_DIR:-.}/dags: Folder chứa file data-pipeline.py
* ./Data-lake: Folder chứa file khởi tạo Delta Table: spark-delta-table.py và Upload dữ liệu lên Minio: export-to-minio.py
* ./Batch-processing: Folder chứa file upload dữ liệu từ Delta Table lên PostgreSQL dựa theo tracking version: batch-process.py
* /home/ninhtrinhmm/Project-Feature-Store: Folder "Project-Feature-Store" đã tạo ở bước 2.b ( Chỉnh lại theo ý muốn )
* /home/ninhtrinhmm/spark-jars: Folder chứa các file JAR ở bước 2.b ( Chỉnh lại theo ý muốn )
* ./NYC-data/Data: Dữ liệu NYC-taxi mà bạn vừa download về

Tiến hành chạy file airflow-docker-compose-CeleryExe.yml: ```cd ..``` && ```docker compose -f airflow-docker-compose-CeleryExe.yml up -d``` 

NOTE: Lưu ý kiểm tra đủ các container đã vận hành thành công như trong hình

<img width="906" height="247" alt="Image" src="https://github.com/user-attachments/assets/cf10c1cf-355f-47dd-ae0f-d69352b6be4c" />

NOTE: Nếu bị lỗi ```Could not read served logs: Client error '403 FORBIDDEN' for url 'http://db7227de6db5:8793/log/dag_id=data_processing_pipeline098run_id=manual__2025-10-27T03:25:23.957681+00:00/task_id=spark_deltalake/attempt=1.log'For more information check: https://httpstatuses.com/403``` mà vẫn chạy bình thường, chỉ không hiển thị log thì chạy lệnh sau:
```python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"``` để điền Fernet Key và tạo 1 SECRET KEY mới. 

Truy cập vào localhost:8080 để vận hành Airflow. Sau khi điền USER và PASS, giao diện Airflow mở ra và bạn sẽ thấy tên của DAG như ở trong file data-pipeline.py

<img width="1821" height="546" alt="Image" src="https://github.com/user-attachments/assets/12aedb24-4c9b-4ee1-ba02-e7512970962d" />

Để vận hành DAG này, ấn vào nút Play ở phía bên tay phải. Để xem tiến độ chạy các task bên trong, click vào DAG và sẽ thấy tiến độ như trong hình 

<img width="1821" height="842" alt="Image" src="https://github.com/user-attachments/assets/356681c0-d27b-4772-9475-731f75b1b81e" />

3 Task lần lượt, tương ứng với 3 task được định nghĩa trong file data-pipeline.py như sau: 

* spark_deltalake: khởi tạo Delta Table nếu chưa có, đưa những file Parquet vào Delta Table và tiến hành tracking version
* export_to_minio: Kiểm tra những file Parquet mới được thêm vào ổ Local và upload lên Minio
* batch_processing: Dựa vào tracking version và đưa những file Parquet mới ở Delta Table vào table ở PostgreSQL

Trong quá trình chạy, nếu thấy ở folder "Project-Feature-Store" xuất hiện folder Deltatable và 3 file sau nghĩa là đang chạy thành công. 

<img width="1077" height="289" alt="Image" src="https://github.com/user-attachments/assets/4b5aa4a5-7d7e-44ea-863b-2c3624d45e6a" />

* Deltatable: Folder Deltatable
* delta_version_tracker.json: File Json chứa version của Delta Table trước và sau khi ghi dữ liệu mới vào
* new_files_output.json: File Json chứa những file mới được thêm vào ổ local
* processed_files.json: Fiel Json chứa danh sách những fiel đã được xử lý và đem vào Delta Table rồi

Ở Airflow localhost:8080, hiển thị như sau nghĩa là 3 Task đã được chạy thành công

<img width="1151" height="391" alt="Image" src="https://github.com/user-attachments/assets/72cffc5f-9603-41e6-a65c-7a0d9fbbf141" />

Tiến hành mở PostgreSQL để kiểm tra table vừa mới tạo ở task số 3, Ở file python batch-process.py đã định nghĩa tên table ở PostgreSQL là "NYC", nên khi mở Dbeaver để truy cập vào PostgreSQL, chúng ta sẽ thấy bảng "nyc" hiện lên như trong hình là thành công. 

<img width="744" height="167" alt="Image" src="https://github.com/user-attachments/assets/62b30744-a809-4c9d-ae21-25ba5ed88b91" />

<img width="1435" height="773" alt="Image" src="https://github.com/user-attachments/assets/7390f36c-0864-4bf3-8868-ac185c27c139" />

## e. Khởi tạo Serving Table kết hợp từ Batch và Stream processing:

Hiện giờ đã có 2 bảng ở Postgresql là nyc và nyc_streaming. Tiếp theo tiến hành tạo bảng table mới là kết hợp từ 2 bảng trên được gọi là Serving Table

Mở Dbeaver, vào mục Script và thực thi các Script sau: 

* Tạo bảng Serving:
```
DO $BODY$
DECLARE
    v_sql TEXT := '';
    v_col RECORD;
    v_first BOOLEAN := TRUE;
BEGIN
    RAISE NOTICE '';

    RAISE NOTICE 'BƯỚC 1: TẠO BẢNG NYC_SERVING';
    
    -- Bắt đầu tạo câu CREATE TABLE với cột ID đầu tiên
    v_sql := E'CREATE TABLE nyc_serving (\n    id SERIAL PRIMARY KEY';
    
    -- Lấy TẤT CẢ cột từ NYC và NYC_stream (CHUẨN HÓA CHỮ THƯỜNG)
    FOR v_col IN 
        SELECT DISTINCT LOWER(column_name) as column_name
        FROM (
            SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc'
            UNION
            SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc_stream'
        ) combined
        WHERE LOWER(column_name) NOT IN ('id')
        ORDER BY LOWER(column_name)
    LOOP
        v_sql := v_sql || E',\n    ' || v_col.column_name || ' TEXT';
    END LOOP;
    
    -- Thêm cột data_source
    v_sql := v_sql || E',\n    data_source TEXT CHECK (data_source IN (''batch'', ''stream''))';
    v_sql := v_sql || E'\n);';
    
    -- Tạo bảng
    EXECUTE v_sql;
    RAISE NOTICE '✓ Đã tạo bảng nyc_serving với cột ID (SERIAL PRIMARY KEY)';
    
    -- Tạo indexes
    CREATE INDEX IF NOT EXISTS idx_serving_source ON nyc_serving(data_source);
    RAISE NOTICE '✓ Đã tạo index cho data_source';
    
    RAISE NOTICE '';
END $BODY$;
```
* Tạo bảng Log để theo dõi tình trạng:
```
DO $$
BEGIN

    RAISE NOTICE 'BƯỚC 2: TẠO BẢNG LOG';
    
    DROP TABLE IF EXISTS nyc_merge_log CASCADE;
    
    CREATE TABLE nyc_merge_log (
        id SERIAL PRIMARY KEY,
        run_time TIMESTAMP DEFAULT NOW(),
        batch_count BIGINT,
        stream_count BIGINT,
        total_count BIGINT,
        execution_seconds DECIMAL(10,2),
        status TEXT
    );
    
    RAISE NOTICE '✓ Đã tạo bảng nyc_merge_log';
    RAISE NOTICE '';
END $$;
```

* Tạo Fuction-Merge:
```
DO $$
BEGIN
    
    RAISE NOTICE 'BƯỚC 3: TẠO MERGE FUNCTION';
END $$;

CREATE OR REPLACE FUNCTION merge_to_serving()
RETURNS TABLE(
    status TEXT,
    batch_rows BIGINT,
    stream_rows BIGINT,
    total_rows BIGINT,
    exec_time DECIMAL
) AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_batch_count BIGINT := 0;
    v_stream_count BIGINT := 0;
    v_total_count BIGINT := 0;
    v_exec_time DECIMAL;
    v_columns TEXT;
    v_sql TEXT;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Lấy danh sách cột CHUNG giữa 2 bảng (bỏ qua cột id gốc)
    SELECT string_agg(column_name, ', ' ORDER BY column_name)
    INTO v_columns
    FROM (
        SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc'
        INTERSECT
        SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc_stream'
    ) common
    WHERE column_name NOT IN ('id', 'ID', 'Id');
    
    -- Xóa dữ liệu cũ trong serving
    TRUNCATE nyc_serving RESTART IDENTITY;
    
    -- MERGE BATCH DATA: Insert từ NYC
    v_sql := format(
        'INSERT INTO nyc_serving (%s, data_source) SELECT %s, ''batch'' FROM nyc',
        v_columns, v_columns
    );
    EXECUTE v_sql;
    GET DIAGNOSTICS v_batch_count = ROW_COUNT;
    
    -- MERGE STREAMING DATA: Insert từ NYC_stream
    v_sql := format(
        'INSERT INTO nyc_serving (%s, data_source) SELECT %s, ''stream'' FROM nyc_stream',
        v_columns, v_columns
    );
    EXECUTE v_sql;
    GET DIAGNOSTICS v_stream_count = ROW_COUNT;
    
    -- Đếm tổng số records
    SELECT COUNT(*) INTO v_total_count FROM nyc_serving;
    
    -- Tính thời gian thực thi
    v_exec_time := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    
    -- Ghi log
    INSERT INTO nyc_merge_log (batch_count, stream_count, total_count, execution_seconds, status)
    VALUES (v_batch_count, v_stream_count, v_total_count, v_exec_time, 'SUCCESS');
    
    -- Return kết quả
    RETURN QUERY SELECT 
        'SUCCESS'::TEXT,
        v_batch_count,
        v_stream_count,
        v_total_count,
        v_exec_time;
        
EXCEPTION WHEN OTHERS THEN
    -- Ghi log lỗi
    INSERT INTO nyc_merge_log (status)
    VALUES ('FAILED: ' || SQLERRM);
    
    RETURN QUERY SELECT 
        ('FAILED: ' || SQLERRM)::TEXT,
        0::BIGINT,
        0::BIGINT,
        0::BIGINT,
        0::DECIMAL;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    RAISE NOTICE '✓ Đã tạo function: merge_to_serving()';
    RAISE NOTICE '';
END $$;
```
* Tạo Trigger tự động sync cho dữ liệu từ bảng nyc_stream:
```
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'BƯỚC 4: TẠO TRIGGER TỰ ĐỘNG';
    RAISE NOTICE '========================================';
END $$;

CREATE OR REPLACE FUNCTION auto_sync_stream()
RETURNS TRIGGER AS $$
DECLARE
    v_columns TEXT;
    v_values TEXT;
    v_sql TEXT;
BEGIN
    -- Lấy danh sách cột chung (bỏ cột id gốc)
    SELECT string_agg(column_name, ', ' ORDER BY column_name)
    INTO v_columns
    FROM (
        SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc'
        INTERSECT
        SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc_stream'
    ) common
    WHERE column_name NOT IN ('id', 'ID', 'Id');
    
    -- Tạo danh sách giá trị từ NEW
    SELECT string_agg('$1.' || column_name, ', ' ORDER BY column_name)
    INTO v_values
    FROM (
        SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc'
        INTERSECT
        SELECT column_name FROM information_schema.columns WHERE table_name = 'nyc_stream'
    ) common
    WHERE column_name NOT IN ('id', 'ID', 'Id');
    
    -- Tạo câu INSERT (ID sẽ tự động tăng do SERIAL)
    v_sql := format(
        'INSERT INTO nyc_serving (%s, data_source) SELECT %s, ''stream''',
        v_columns, v_values
    );
    
    EXECUTE v_sql USING NEW;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_auto_sync ON nyc_stream;

CREATE TRIGGER trg_auto_sync
    AFTER INSERT ON nyc_stream
    FOR EACH ROW
    EXECUTE FUNCTION auto_sync_stream();

DO $$
BEGIN
    RAISE NOTICE '✓ Đã tạo trigger: trg_auto_sync';
    RAISE NOTICE '✓ Mỗi khi INSERT vào nyc_stream → Tự động vào nyc_serving (ID tự tăng)';
    RAISE NOTICE '';
END $$;
```
* Chạy Merge lần đầu
```
DO $$
DECLARE
    v_result RECORD;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'BƯỚC 5: MERGE DỮ LIỆU LẦN ĐẦU';
    RAISE NOTICE '========================================';
    
    SELECT * INTO v_result FROM merge_to_serving();
    
    RAISE NOTICE 'Status: %', v_result.status;
    RAISE NOTICE 'Batch: % dòng', v_result.batch_rows;
    RAISE NOTICE 'Stream: % dòng', v_result.stream_rows;
    RAISE NOTICE 'Tổng: % dòng', v_result.total_rows;
    RAISE NOTICE 'Thời gian: % giây', v_result.exec_time;
    RAISE NOTICE '';
END $$;
```
Sau khi chạy xong, chúng ta sẽ thấy dữ liệu xuất hiện ở bảng nyc_serving, đây chính là dữ liệu từ 2 bảng nyc và nyc_stream. 

Vì đã có Trigger tự động sync nên dữ liệu từ bảng nyc_stream luôn chảy về bảng nyc_serving. Còn để dữ liệu từ nyc được chảy về nyc_serving, chúng ta chỉ cần thực thi script sau: ```SELECT * INTO v_result FROM merge_to_serving();```

<img width="1693" height="927" alt="Image" src="https://github.com/user-attachments/assets/48d94c3b-60e2-47eb-993d-5e849965afd5" />

Hoặc có thể tích hợp việc chạy Script trên thành 1 task ở Air Flow chạy ở sau cùng. 

# 4. Kích hoạt Debezium để theo dõi CDC từ bảng Serving Table: 

Để nắm bắt các thay đổi, cập nhập ở bảng nyc_serving, thay vì dùng SQL query có thể gây mất thời gian, chúng ta sử dụng công cụ Debezium đã được kích hoạt sẵn trong fiel docker-compose.yml được chạy ở ban đầu. 

Tiến hành khởi tạo Kafka Connector có nhiệm vụ kết nối giữa bảng nyc_serving và Kafka Broker, bằng cách gửi cấu hình của Connector đến container cdc-debezium ( service Debezium ) như sau: 

```cd Streaming-processing``` && ```bash run-cdc.sh register_connector ./kafka-connect/connect-debezium.json```

Cấu hình của Connector nyc-serving-cdc như sau:  

<img width="835" height="324" alt="Image" src="https://github.com/user-attachments/assets/be43b14d-f3c8-44b0-8f79-25deb363a6f7" />  

Trong đó, tên Topic mới chứa các mesage CDC sẽ là "database.server.name" + "table.include.list", trong trường hợp này, tên Topic mới sẽ là "test.public.nyc_serving"

Lên localhost:9021, vào Topic để kiểm tra tình trạng của Topic mới có Healthy không

<img width="1336" height="653" alt="Image" src="https://github.com/user-attachments/assets/85762e44-4290-4cb0-85e4-982e342f6446" /> 

Click vào Topic "test.public.nyc_serving" và vào phần Message để xem các thay đổi vừa được cập nhập từ bảng nyc_serving

<img width="1693" height="860" alt="Image" src="https://github.com/user-attachments/assets/9b39ad91-cc74-48b1-a643-60c5a613a811" /> 







