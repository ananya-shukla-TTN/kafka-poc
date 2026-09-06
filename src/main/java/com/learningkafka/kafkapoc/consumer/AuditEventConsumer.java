package com.learningkafka.kafkapoc.consumer;

import com.learningkafka.kafkapoc.model.NotificationEventDto;
import com.learningkafka.kafkapoc.model.TransactionEventDto;
import com.learningkafka.kafkapoc.producer.NotificationEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class AuditEventConsumer {

    private final NotificationEventPublisher notificationEventPublisher;

    @KafkaListener(topics = "transaction-events", groupId = "audit-logger")
    public void handleTransactionEvent(TransactionEventDto event) {
        log.info("AUDIT: {} txn={} folio={} type={}",
                event.getEventType(), event.getTransactionId(),
                event.getFolioNumber(), event.getTransactionType());

        if (!"TRANSACTION_APPROVED".equals(event.getEventType())) {
            // Only approved transactions trigger a customer notification
            return;
        }

        NotificationEventDto notification = NotificationEventDto.builder()
                .eventType("SEND_SMS")
                .transactionId(event.getTransactionId())
                .phoneNo(event.getPhoneNo())
                .templateType(mapTemplateType(event.getTransactionType()))
                .templateParams(Map.of(
                        "folioNumber", String.valueOf(event.getFolioNumber()),
                        "transactionType", String.valueOf(event.getTransactionType())
                ))
                .build();

        notificationEventPublisher.publish(notification);
    }

    private String mapTemplateType(String transactionType) {
        if (transactionType == null) {
            return "TRANSACTION_SUCCESS";
        }
        return switch (transactionType.toUpperCase()) {
            case "REDEMPTION" -> "REDEMPTION_SUCCESS";
            case "PURCHASE" -> "PURCHASE_SUCCESS";
            case "SWITCH" -> "SWITCH_SUCCESS";
            default -> "TRANSACTION_SUCCESS";
        };
    }
}
