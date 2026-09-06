package com.learningkafka.kafkapoc.producer;

import com.learningkafka.kafkapoc.model.NotificationEventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationEventPublisher {

    private static final String TOPIC = "notification-events";

    private final KafkaTemplate<String, NotificationEventDto> kafkaTemplate;

    public void publish(NotificationEventDto event) {
        String key = event.getTransactionId();
        log.info("Publishing {} for txn={} to {}", event.getEventType(), key, TOPIC);
        kafkaTemplate.send(TOPIC, key, event);
    }
}
