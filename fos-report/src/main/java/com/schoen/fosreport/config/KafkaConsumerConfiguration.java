package com.schoen.fosreport.config;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.schoen.fosreport.model.EventWindow;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/*
 * Kafka configuration beans for the Kafka consumer/listener.
 */
@Configuration
@NoArgsConstructor
public class KafkaConsumerConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerConfiguration.class);

    public static final String BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");


    @Bean
    public ConsumerFactory<String, EventWindow> consumerFactory() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "com.schoen");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        //LOG.info("Loaded: "+ properties);
        final ErrorHandlingDeserializer<String> headerErrorHandlingDeserializer
                = new ErrorHandlingDeserializer<>(new StringDeserializer());
        final ErrorHandlingDeserializer<EventWindow> errorHandlingDeserializer
                = new ErrorHandlingDeserializer<>(new JsonDeserializer<>(EventWindow.class, createObjectMapper()));
        return new DefaultKafkaConsumerFactory<>(properties, headerErrorHandlingDeserializer, errorHandlingDeserializer);
    }

    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(ConsumerFactory<String, EventWindow> consumerFactory) {
        final ConcurrentKafkaListenerContainerFactory<String, EventWindow> kafkaListenerContainerFactory
                = new ConcurrentKafkaListenerContainerFactory<>();
        kafkaListenerContainerFactory.setConcurrency(1);
        kafkaListenerContainerFactory.setConsumerFactory(consumerFactory);
        kafkaListenerContainerFactory.setCommonErrorHandler(new DefaultErrorHandler());
        return kafkaListenerContainerFactory;
    }

    private ObjectMapper createObjectMapper() {
        return Jackson2ObjectMapperBuilder.json()
                .visibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .featuresToDisable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

}
