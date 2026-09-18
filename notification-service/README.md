# Notification Service - WebClient & Kafka Integration

## Mục tiêu
Kết hợp WebClient (Non-blocking) và Kafka Consumer trong cùng một luồng xử lý nghiệp vụ để gửi thông báo thông minh cho khách hàng.

## Kiến trúc

### Luồng xử lý
1. **Order Service** gửi sự kiện `order.created` đến Kafka topic `storex-order-events`
2. **Notification Service** nhận sự kiện từ Kafka
3. Gọi **User Preference API** để lấy kênh ưa thích (EMAIL hoặc ZALO)
4. Gọi **Email API** hoặc **Zalo API** để gửi thông báo
5. Xử lý lỗi với DLQ (Dead Letter Queue)

### Các thành phần chính

#### 1. WebClient Configuration (`WebClientConfig.java`)
- **Timeout**: Read timeout tối đa 3 giây
- **Retry**: Tự động retry tối đa 2 lần khi gặp lỗi 5xx, mỗi lần cách nhau 1 giây
- **Non-blocking**: Sử dụng Reactor Netty

#### 2. Kafka Consumer Configuration (`KafkaConsumerConfig.java`)
- **Topic**: `storex-order-events`
- **DLQ**: `storex-order-events.DLQ`
- **Retry**: 2 lần retry với delay 1 giây
- **Error Handler**: Ghi log và đẩy message lỗi vào DLQ

#### 3. Notification Consumer (`NotificationConsumer.java`)
- **Idempotency**: Sử dụng `ConcurrentHashMap` để lưu danh sách `orderId` đã xử lý
- **Fallback**: Khi User Preference API thất bại, mặc định chọn kênh EMAIL
- **Non-blocking**: Toàn bộ luồng xử lý sử dụng `.subscribe()` và `.flatMap()`, KHÔNG sử dụng `.block()`

## Các tình huống xử lý (Edge Cases)

### BUG-05: User Preference API bị chết hoặc trả về lỗi 500
- **Xử lý**: WebClient bắt lỗi, ghi log WARNING và chuyển sang Fallback (gửi Email)
- **Không làm gián đoạn luồng xử lý**

### BUG-06: Kafka gửi lại cùng một sự kiện (trùng lặp)
- **Xử lý**: Consumer kiểm tra `orderId` trong `ConcurrentHashMap`
- **Nếu đã xử lý**: Bỏ qua message, commit offset thành công
- **Nếu chưa xử lý**: Tiếp tục luồng xử lý bình thường

### BUG-07: Email API gửi thất bại liên tục sau 2 lần retry
- **Xử lý**: Consumer ném exception, ErrorHandler đẩy message vào DLQ
- **Log**: Ghi log mức ERROR với nội dung "Đã đẩy order {orderId} vào DLQ do lỗi gửi thông báo"

### REQ-01: Tính chất Non-blocking
- **Xử lý**: Toàn bộ luồng gọi API sử dụng WebClient với `.subscribe()` hoặc `.flatMap()`
- **Cấm**: KHÔNG sử dụng `.block()` trong Consumer

## Cấu hình

### Application Configuration (`application.yml`)
```yaml
spring:
  application:
    name: notification-service
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: storex-notification
      auto-offset-reset: earliest

server:
  port: 8083
```

### API Endpoints
- **User Preference API**: `GET http://localhost:8081/api/preferences/{userId}`
- **Email API**: `POST http://localhost:8082/api/notify/email`
- **Zalo API**: `POST http://localhost:8082/api/notify/zalo`

## Cách chạy

### 1. Khởi động Kafka
```bash
docker-compose up -d
```

### 2. Khởi động Notification Service
```bash
cd notification-service
./gradlew bootRun
```

### 3. Kiểm tra logs
- Xem logs để theo dõi luồng xử lý
- Kiểm tra DLQ topic nếu có message lỗi

## Testing

### Test BUG-05 (User Preference API failure)
1. Tắt User Preference API (port 8081)
2. Gửi sự kiện `order.created` mới
3. Kiểm tra log: Phải thấy WARNING và fallback sang EMAIL

### Test BUG-06 (Idempotency)
1. Gửi sự kiện `order.created` với `orderId = "ORD-123"`
2. Gửi lại cùng sự kiện đó
3. Kiểm tra log: Phải thấy message "Order ORD-123 đã được xử lý trước đó"

### Test BUG-07 (DLQ handling)
1. Tắt Email/Zalo API (port 8082)
2. Gửi sự kiện `order.created` mới
3. Kiểm tra log: Phải thấy ERROR "Đã đẩy order {orderId} vào DLQ do lỗi gửi thông báo"
4. Kiểm tra topic `storex-order-events.DLQ`

### Test REQ-01 (Non-blocking)
1. Kiểm tra code: KHÔNG có `.block()` trong NotificationConsumer
2. Tất cả API calls sử dụng `.subscribe()` hoặc `.flatMap()`

## Dependencies
- Spring Boot Starter WebFlux (Reactive Web)
- Spring Boot Starter Kafka
- Lombok
- Reactor Netty

## Port Mapping
- Notification Service: 8083
- Kafka: 9092
- User Preference API: 8081 (external)
- Notification API: 8082 (external)
