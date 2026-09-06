package com.learningkafka.kafkapoc.controller;

import com.learningkafka.kafkapoc.model.TransactionEventDto;
import com.learningkafka.kafkapoc.producer.TransactionEventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequiredArgsConstructor
public class TransactionEventController {

    private final TransactionEventPublisher publisher;

    @PostMapping("/api/events/approved")
    public ResponseEntity<String> approved(@RequestBody TransactionEventDto request) {
        return publish(request, "TRANSACTION_APPROVED");
    }

    @PostMapping("/api/events/initiated")
    public ResponseEntity<String> initiated(@RequestBody TransactionEventDto request) {
        return publish(request, "TRANSACTION_INITIATED");
    }

    @PostMapping("/api/events/failed")
    public ResponseEntity<String> failed(@RequestBody TransactionEventDto request) {
        return publish(request, "TRANSACTION_FAILED");
    }

    private ResponseEntity<String> publish(TransactionEventDto request, String eventType) {
        TransactionEventDto event = request.toBuilder()
                .eventType(eventType)
                .timestamp(request.getTimestamp() != null ? request.getTimestamp() : Instant.now().toString())
                .build();
        publisher.publish(event);
        return ResponseEntity.accepted().body("Published " + eventType + " for txn=" + event.getTransactionId());
    }
}
