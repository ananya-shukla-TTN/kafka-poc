package com.learningkafka.kafkapoc.producer;

import com.learningkafka.kafkapoc.model.TransactionEventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionEventPublisher {

    private static final String TOPIC = "transaction-events";

    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;

    public void publish(TransactionEventDto event) {
        // Partition key = folioNumber -> all events for the same folio land on the same partition
        String key = event.getFolioNumber();
        log.info("Publishing {} for txn={} folio={} to {}",
                event.getEventType(), event.getTransactionId(), key, TOPIC);
        kafkaTemplate.send(TOPIC, key, event);
    }
}
