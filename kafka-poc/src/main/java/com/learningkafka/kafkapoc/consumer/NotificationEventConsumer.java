package com.learningkafka.kafkapoc.consumer;

import com.learningkafka.kafkapoc.model.NotificationEventDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class NotificationEventConsumer {

    @KafkaListener(topics = "notification-events", groupId = "sms-sender")
    public void handleNotification(NotificationEventDto event) {
        log.info("SMS sent (simulated) to {} template={} params={}",
                event.getPhoneNo(), event.getTemplateType(), event.getTemplateParams());
    }
}
