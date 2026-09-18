package com.notificationservice.config;

import com.notificationservice.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition("storex-order-events.DLQ", record.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        // FixedBackOff(1000L, 2L): Delay 1s (1000ms), retry tối đa 2 lần sau lần thử ban đầu
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);

        ConsumerRecordRecoverer loggingAndDlqRecoverer = (record, exception) -> {
            recoverer.accept(record, exception);

            String orderId = "UNKNOWN";
            if (record.value() instanceof OrderEvent event) {
                orderId = event.getOrderId();
            } else if (record.value() != null) {
                orderId = record.value().toString();
            }

            log.error("Đã đẩy order {} vào DLQ do lỗi gửi thông báo", orderId);
        };

        return new DefaultErrorHandler(loggingAndDlqRecoverer, backOff);
    }
}
