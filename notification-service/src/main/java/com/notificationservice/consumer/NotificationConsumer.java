package com.notificationservice.consumer;

import com.notificationservice.model.NotificationRequest;
import com.notificationservice.model.OrderEvent;
import com.notificationservice.model.UserPreference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class NotificationConsumer {

    private final WebClient webClient;
    
    // ConcurrentHashMap để lưu danh sách orderId đã xử lý (Idempotency)
    private final ConcurrentHashMap<String, Boolean> processedOrderIds = new ConcurrentHashMap<>();

    private static final String USER_PREFERENCE_API_BASE_URL = "http://localhost:8081/api/preferences";
    private static final String NOTIFICATION_API_BASE_URL = "http://localhost:8082/api/notify";
    private static final String DEFAULT_CHANNEL = "EMAIL";

    @Autowired
    public NotificationConsumer(WebClient webClient) {
        this.webClient = webClient;
    }

    @KafkaListener(topics = "storex-order-events", groupId = "storex-notification")
    public void consume(OrderEvent event) {
        String orderId = event.getOrderId();
        String customerId = event.getCustomerId();

        log.info("Notification-Service received order.created - orderId={}, customerId={}", orderId, customerId);

        // BUG-06: Kiểm tra tính lũy đẳng (Idempotency)
        // Nếu orderId đã được xử lý, bỏ qua message này
        if (processedOrderIds.containsKey(orderId)) {
            log.warn("Order {} đã được xử lý trước đó. Bỏ qua message trùng lặp.", orderId);
            return;
        }

        // Luồng xử lý non-blocking với WebClient
        processNotification(orderId, customerId)
                .doOnSuccess(success -> {
                    // Đánh dấu orderId đã xử lý thành công
                    processedOrderIds.put(orderId, true);
                    log.info("Đã xử lý thành công và đánh dấu order {} là đã xử lý", orderId);
                })
                .doOnError(error -> {
                    log.error("Lỗi khi xử lý thông báo cho order {}: {}", orderId, error.getMessage());
                    // Ném exception để DLQ handler xử lý
                    throw new RuntimeException("Failed to process notification for order: " + orderId, error);
                })
                .subscribe();
    }

    private Mono<Void> processNotification(String orderId, String customerId) {
        // Bước 1: Gọi User Preference API để lấy kênh ưa thích
        return getUserPreference(customerId)
                .flatMap(preferredChannel -> {
                    log.info("Kênh ưu tiên của khách hàng {}: {}", customerId, preferredChannel);
                    
                    // Bước 2: Gọi API gửi thông báo dựa trên kênh
                    return sendNotification(orderId, customerId, preferredChannel);
                })
                .onErrorResume(WebClientResponseException.class, ex -> {
                    // BUG-05: Fallback khi User Preference API thất bại (5xx hoặc Connection Refused)
                    if (ex.getStatusCode().is5xxServerError() || ex.getMessage().contains("Connection")) {
                        log.warn("User Preference API thất bại (status: {}, message: {}). Sử dụng Fallback: EMAIL", 
                                ex.getStatusCode(), ex.getMessage());
                        return sendNotification(orderId, customerId, DEFAULT_CHANNEL);
                    }
                    return Mono.error(ex);
                })
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(1)) // Retry tối đa 2 lần, mỗi lần cách nhau 1 giây
                        .filter(throwable -> throwable instanceof WebClientResponseException && 
                                ((WebClientResponseException) throwable).getStatusCode().is5xxServerError())
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> 
                                retrySignal.failure()))
                .then();
    }

    private Mono<String> getUserPreference(String customerId) {
        return webClient.get()
                .uri(USER_PREFERENCE_API_BASE_URL + "/{userId}", customerId)
                .retrieve()
                .bodyToMono(UserPreference.class)
                .map(UserPreference::getPreferredChannel)
                .timeout(Duration.ofSeconds(3)) // Timeout 3 giây
                .onErrorResume(throwable -> {
                    // BUG-05: Fallback khi timeout hoặc lỗi kết nối
                    log.warn("Lỗi khi gọi User Preference API cho customerId {}: {}. Sử dụng Fallback: EMAIL", 
                            customerId, throwable.getMessage());
                    return Mono.just(DEFAULT_CHANNEL);
                });
    }

    private Mono<Void> sendNotification(String orderId, String customerId, String channel) {
        String endpoint = switch (channel.toUpperCase()) {
            case "ZALO" -> NOTIFICATION_API_BASE_URL + "/zalo";
            case "EMAIL" -> NOTIFICATION_API_BASE_URL + "/email";
            default -> NOTIFICATION_API_BASE_URL + "/email"; // Default to EMAIL
        };

        NotificationRequest request = NotificationRequest.builder()
                .orderId(orderId)
                .customerId(customerId)
                .message("Đơn hàng " + orderId + " của bạn đã được xác nhận thành công")
                .build();

        log.info("Gửi thông báo qua kênh {} cho order {} đến endpoint {}", channel, orderId, endpoint);

        return webClient.post()
                .uri(endpoint)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(Void.class)
                .timeout(Duration.ofSeconds(3)) // Timeout 3 giây
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(1)) // Retry tối đa 2 lần, mỗi lần cách nhau 1 giây
                        .filter(throwable -> throwable instanceof WebClientResponseException && 
                                ((WebClientResponseException) throwable).getStatusCode().is5xxServerError())
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            // BUG-07: Sau khi retry thất bại, ném exception để DLQ handler xử lý
                            log.error("Đã đẩy order {} vào DLQ do lỗi gửi thông báo sau 2 lần retry", orderId);
                            return retrySignal.failure();
                        }))
                .doOnSuccess(v -> log.info("Đã gửi thông báo thành công qua {} cho order {}", channel, orderId));
    }
}
