package com.learningkafka.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TransactionEventDto {

    private String eventType;
    private String transactionId;
    private String folioNumber;
    private String transactionType;
    private String phoneNo;
    private String timestamp;
}
