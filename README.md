========= khởi tạo môi trường Python 3.9 =========

''shel''    ### Nếu lỗi thì mở Vs code bằng code . trong Anaconda powershell

conda create -n MLOP2 python=3.10.18
conda activate MLOP2
pip install -r requirement.txt

========= Batch Processing ======

* Thiết lập Delta Table và cập nhập xem Folder gốc có file mới nào không
```cd Data-lake```
```python delta-test.py``` ## Test thử tạo 1 Delta Tabel đơn giản
```python spark-delta-table.py```

* Thiết lập các hạ tầng như MINIO, TRINO, PostgreSQL trong file docker-compose: ```docker compose -f docker-compose.yml up -d```

* Upload toàn bộ  file parquet lên MINIO: ```cd Data-lake``` && ```python export-to-minio.py```

* Dowload 

* Đưa dữ liệu từ MINIO vào PostgreSQL bởi Spark: ```cd ..``` && ```cd Batch-processing``` && ```python batch-process.py```

========= Streaming Data Processing ============

* Sử dụng Flink để xử lý dữ liệu từ topic "device_1" sang topic mới: ```python Streaming-processing/datastream_api.py```

* trong trường hợp chạy file datastream_api.py gặp lỗi ```025-10-29 09:35:20,031 main ERROR FileManager (/opt/flink/log/flink-ninhtrinhmm-python-ninhtrinhmm-berserker-7540.log) java.io.FileNotFoundException: ``` thì khởi tạo lại folder log của flink nhưu sau:
```sudo mkdir -p /opt/flink/log``` && ```sudo chown -R $USER:$USER /opt/flink/log``` && ```sudo chmod -R 755 /opt/flink/log```

* Truy cập vào Control-Center thông qua localhost:9021 để check xem đã có 2 Topic có tình trạng healthy chưa

* Mở Terminal khác, tiến hành gửi cấu hình của PostgreSQL đến service connect thông qua file run.sh:```cd Streaming-processing```&&```bash run.sh register_connector ./kafka-connect/connect-sink.json`

* Xem log của service connect có lỗi không: ```docker logs flink-connect --tail 100 | grep -i "error\|exception\|failed\|NYC"```

* Check tình trạng của connector mới tạo:```curl -s http://localhost:8083/connectors/<"tên connector">/status | python3 -m json.tool```

* Nếu không thấy lỗi thì truy cập vào table trong posrgreSQL-db:5432 bằng Dbeaver


============= Air Flow: Pipeline tự động ==============

* CHạy trên terminal của máy local để cấp quyền chỉnh sửa folder Project-Feature-Store cho UID 50000, tức Airflow user trong container, chạy rồi thì thôi
```chmod -R 777 ./Project-Feature-Store```
```sudo chown -R 50000:0 ./Project-Feature-Store```

* Chạy ```docker compose -f airflow-docker-compose-CeleryExe.yml up -d```

* Kiểm tra các service đã lên đủ chưa, ```docker ps -a```. Nếu thấy Airflow-scheduler bị exit thì thêm "AIRFLOW__LOGGING__ENABLE_TASK_CONTEXT_LOGGER: 'False'" vào environment:
&airflow-common-env và chạy lại, Airflow-scheduler sẽ được ổn định. 

* Truy cập vào localhost:8080 và chạy mà thấy lỗi ```Could not read served logs: Request URL is missing an 'http://' or 'https://' protocol``` trong phần logs thì sửa như sau: Bỏ mount /opt/airflow/logs đến log ở máy local

* Nếu bị lỗi ```Could not read served logs: Client error '403 FORBIDDEN' for url 'http://db7227de6db5:8793/log/dag_id=data_processing_pipeline098run_id=manual__2025-10-27T03:25:23.957681+00:00/task_id=spark_deltalake/attempt=1.log'For more information check: https://httpstatuses.com/403``` mà vẫn chạy bình thường, chỉ không hiển thị log thì chạy lệnh sau:
```python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"``` để điền Fernet Key và tạo 1 SECRET KEY mới. 

======= Tạo Serving Layer =======

Vào Dbeaver, bật script và chạy lệnh sau: 

1. Tạo bảng Serving: 

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

2. Tạo bảng Log để theo dõi tình trạng: 

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

3. Tạo Fuction-Merge: 

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

4. Tạo Trigger tự động sync cho dữ liệu từ bảng nyc_stream:

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

5. Chạy Merge lần đầu: 

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

* Hoàn thiện xong ấn refresh để thấy kết quả hiển thị ở table nyc_serving

