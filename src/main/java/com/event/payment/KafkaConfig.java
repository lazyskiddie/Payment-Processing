package com.event.payment;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConfig {

    static final String DLT = PaymentService.TOPIC + ".DLT";

    // More than 1 partition is what lets several consumer threads work in parallel.
    @Bean
    NewTopic paymentEventsTopic(@Value("${payment.kafka.partitions:6}") int partitions) {
        return TopicBuilder.name(PaymentService.TOPIC).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic paymentEventsDlt() {
        return TopicBuilder.name(DLT).partitions(1).replicas(1).build();
    }

     // Poison messages (bad JSON, unknown payment id) are retried 3x, 1s apart, then parked on
     // payment-events.DLT instead of blocking the partition. Spring Boot picks this bean up
     // automatically for @KafkaListener containers.

    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, PaymentEvent> eventTemplate,
                                         @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        // Deserialization failures arrive as raw bytes, so they need a byte[] template.
        // (Deliberately NOT a @Bean: a second KafkaTemplate bean would disable Boot's auto-configured one.)
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        KafkaTemplate<String, byte[]> bytesTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, eventTemplate);

        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(DLT, -1));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
