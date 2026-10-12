# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Trần Xuân Tùng` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602787` |
| GitHub | `xuantungtran64-ai` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-03904` |

## Kết quả cuối

- `dataset_version` của bạn: `9ba8b52b8fb3`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `train: 639 / val: 207 / test: 173`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng `ts_utc` dạng ISO-8601 UTC, tốc độ `speed_mps` (m/s), và các cột như `heading_deg`, `gas`, `brake`, `steer`; nguồn B dùng `timestamp_ms` dạng epoch mili-giây, `spd_kmh` (km/h), `hdg`, `throttle_pct`, `brake_pct`, `steering`. B còn ghi bàn đạp theo phần trăm, trong khi A ghi giá trị chuẩn hoá. Vì vậy hai nguồn khác cả tên cột, đơn vị, định dạng thời gian và cách biểu diễn tín hiệu. Bronze giữ một bảng cho mỗi nguồn và lưu dạng chữ để bảo toàn dữ liệu gốc, chịu được schema drift và cho phép kiểm tra/biến đổi lại; ép kiểu hoặc gộp sớm có thể làm mất thông tin hay âm thầm diễn giải sai dữ liệu.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging chuẩn hoá hai nguồn về cùng schema: tốc độ m/s, thời gian UTC, cùng các cột telemetry. A đã có tốc độ m/s và ISO UTC; B đổi km/h bằng cách chia 3,6, đổi epoch ms sang timestamp, đổi phần trăm ga/phanh về tỷ lệ 0–1, rồi trừ độ lệch đồng hồ 1.500 ms đã khai báo. Bằng chứng cho thấy mẫu B đầu tiên là `2025-10-08 14:30:00.009 UTC`, lệch video chỉ 9 ms. Mỗi drive nhận 3.021/3.234 dòng; quarantine như sau:

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng reject | Sạch |
|---|---:|---:|---:|---:|---:|
| A | 20 | 30 | 3 | 53 | 2.968 |
| B | 20 | 30 | 3 | 53 | 3.181 |

Tổng sạch + reject khớp số dòng nhận vào ở cả hai drive. Rejects giữ nguyên dòng cùng lý do và thông tin batch/dòng nguồn để truy vết, sửa quy tắc rồi xử lý lại; xoá im lặng sẽ làm mất khả năng kiểm toán và che khuất chất lượng dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` là `warn` vì khoảng trống cảm biến là dropout cần được báo cáo nhưng không nhất thiết làm mọi dữ liệu còn lại vô dụng. `in_range` là `error` vì tốc độ ngoài 0–70 m/s có thể là dữ liệu sai và không nên đi tiếp vào gold. Trong `break_gate.json`, khi nới giới hạn lên 1.000 m/s, gate vẫn bắt test `in_range_silver_telemetry_speed_mps__70__0` với 6 dòng lỗi; kết quả là 5 PASS, 1 WARN, 1 ERROR. Khôi phục giới hạn cho kết quả 6 PASS, 1 WARN, 0 ERROR. Cổng chất lượng ngăn dữ liệu vi phạm điều kiện quan trọng lan xuống các bước sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` ghi 1.246 dòng trước và sau hai lần chạy lại, fingerprint đều là `6c7480dfde7e`; partition thử nghiệm `trip-00003` có 60 dòng. Biến thể `append` cho 120 dòng sau hai lần, tức nhân đôi partition. Delta `overwrite` với predicate theo `trip_id` thay thế đúng partition trong một commit giao dịch: partition khác không bị đụng tới và người đọc không thấy trạng thái đã xoá nhưng chưa ghi lại nếu tiến trình lỗi giữa chừng. Chạy lại cùng partition vì thế không cộng dồn bản sao.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng gần như bằng không và bị loại; khung tối có thể vẫn chứa cảnh hữu ích (ví dụ đường hầm/bóng râm), nên được gắn cờ nhưng giữ lại. Theo funnel, A có 6 khung đen và B có 4; B có 61 khung tối được giữ. Tôi chọn ngưỡng 2 như pipeline đang dùng: đủ dung sai cho khác biệt nhỏ của dHash nhưng ít loại hơn ngưỡng 6. Ngưỡng cao hơn đánh dấu nhiều khung tương tự hơn. Cần xét tốc độ xe: xe chạy chậm khiến các khung liên tiếp tự nhiên giống nhau, còn xe chạy nhanh thường tạo khác biệt lớn hơn; một ngưỡng cố định có thể loại quá nhiều cảnh chậm hoặc bỏ sót ảnh lặp ở cảnh nhanh.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Với 1.019 khung được giữ và 26 trip, chia ngẫu nhiên theo khung làm cả 26/26 trip xuất hiện ở cả train lẫn test; cả 26 trip đều bị chia cắt. Chia theo trip cho 0 trip bị chia cắt. Các khung gần nhau trong cùng cảnh thường rất giống nhau; nếu một phần nằm ở train và phần kia ở test, mô hình đã thấy gần như cùng cảnh khi học. Điểm test khi đó đo khả năng nhớ cảnh hơn là khái quát sang chuyến/cảnh mới nên có thể đẹp giả tạo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là 12 ký tự đầu của SHA-256 trên danh sách `frame_id|split` đã sắp xếp. Nó nhận diện nội dung và cách gán split, không đổi chỉ vì chạy vào thời điểm khác hay vì số dòng tình cờ giống nhau. Với cùng dữ liệu và MSSV, hai lần export cho cùng version; kết quả cuối của tôi là `9ba8b52b8fb3`. MSSV được ghép vào thứ tự băm trip trước khi chia split, nên MSSV khác có thể gán trip/khung vào split khác và tạo version riêng dù đầu vào giống nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Trong manifest của `9ba8b52b8fb3`, `lineage` trỏ tới `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.frames` v25, `silver.telemetry` v0, `silver.frames_flagged` v0 và `silver.frame_telemetry` v0. Hai video đầu vào được nhận diện bằng SHA-256: drive A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, drive B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Các phiên bản bảng và hash video cho biết chính xác dữ liệu upstream; để tái tạo sau này còn cần mã pipeline/dbt, cấu hình và MSSV (ảnh hưởng split), cùng quy tắc lọc/chia trong manifest. `dataset_version` và `manifest_sha256` giúp kiểm tra đầu ra có đúng nội dung hay không.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Theo dependency của DAG, trip `trip-03904` là một trong 26 mapped task `silver_frames`: khi mô phỏng lỗi sẽ có 1 task đỏ; 7 task phía sau (hai dbt run/test bạc, hai dbt run/test gold, publish lake, export dataset và publish warehouse) bị `upstream_failed`. Sau khi clear đúng mapped task đỏ và chạy lại thành công, các bước phụ thuộc có thể tiếp tục. Việc ghi đè partition theo trip tránh nhân đôi; với cùng đầu vào/split, version dự kiến giữ nguyên `9ba8b52b8fb3`. Tuy nhiên repo hiện chưa có ảnh log task đỏ hoặc DAG xanh sau khi clear, nên số task và version trước/sau ở đây mới được suy ra từ cấu trúc DAG và nguyên tắc idempotent, chưa có ảnh để xác nhận lần chạy sự cố cụ thể.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba nút thắt đầu tiên sẽ là giải mã video và task `silver_frames` đang giới hạn đồng thời một task; lưu lượng video/khung hình trên một MinIO đơn lẻ; và xử lý dbt trên DuckDB một máy cùng warehouse Postgres. Với 500 xe/ngày, tôi sẽ đưa decode/extract thành worker pool phân tán theo trip sau một hàng đợi để tăng song song và retry độc lập; chuyển lake sang object storage S3 có khả năng mở rộng; và chạy biến đổi lớn trên engine/warehouse phân tán như Spark hoặc BigQuery/Snowflake thay cho một DuckDB đơn nút. Airflow vẫn điều phối, không nên gánh xử lý dữ liệu trực tiếp.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi chọn câu hỏi: **10 trip có tỷ lệ khung không được giữ cao nhất là trip nào?**

```sql
select drive_id, trip_id, frames, frames_kept,
	   round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as dropped_pct
from marts.mart_trip_summary
order by dropped_pct desc, frames desc
limit 10;
```

Tỷ lệ cao giúp ưu tiên kiểm tra trip có nhiều khung đen, mờ, trùng hoặc mất đồng bộ; cần xem thêm funnel theo trip để biết nguyên nhân cụ thể.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất là chia theo khung làm cả 26 trip rò rỉ sang train và test, còn chia theo trip loại bỏ hoàn toàn kiểu rò rỉ này. Tôi vẫn chưa rõ một ngưỡng dHash cố định sẽ hoạt động thế nào trên nhiều tốc độ, camera và điều kiện ánh sáng khác nhau. Một lỗi pipeline có thể chưa bắt là GPS nhảy đột ngột nhưng từng giá trị vẫn nằm trong miền hợp lệ; cần thêm kiểm tra tốc độ di chuyển suy ra từ khoảng cách GPS và thời gian.
