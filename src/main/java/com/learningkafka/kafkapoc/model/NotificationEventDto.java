package com.learningkafka.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEventDto {

    private String eventType;
    private String transactionId;
    private String phoneNo;
    private String templateType;
    private Map<String, String> templateParams;
}
