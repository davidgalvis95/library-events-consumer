package com.learnkafka.libraryeventsconsumer.config;

import com.learnkafka.libraryeventsconsumer.deserialization.LibraryEventDeserializer;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Autowired
    private LibraryEventsService libraryEventsService;

    private final String bootstrapAddress;

    private final String groupId;

    public LibraryEventsConsumerConfig(@Value("${spring.kafka.producer.bootstrap-servers}") final String bootstrapAddress,
                                       @Value("${spring.kafka.producer.bootstrap-servers}") final String groupId) {
        this.bootstrapAddress = bootstrapAddress;
        this.groupId = groupId;
    }

    @Bean
    public ConsumerFactory<Integer, LibraryEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                LibraryEventDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    //we will override the common behavior of this bean in order to configure how kafka reads the records, acknowledge them and commit the offsets
    //This bean was extracted from the KafkaAnnotationDrivenConfiguration class, which was found by the KafkaAutoConfiguration class
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, LibraryEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
//        configurer.configure(factory, kafkaConsumerFactory);
//        factory.setConcurrency(3);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Exception in consumer config is {} and the record is {}", thrownException.getMessage(), data);
            //Usually this implementation to use a custom error handler is done to persist some damaged record to DB or keep track of it
            //Persist
        }));
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(context -> {
            if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException){
                //invoke recovery logic
                log.info("Inside the recoverable logic");
//                Arrays.asList(context.attributeNames())
//                        .forEach( attributeName -> {
//                            log.info("Attribute names is {}: ", attributeName);
//                            log.info("Attribute value is {}: ", context.getAttribute(attributeName));
//                        });
                final ConsumerRecord<Integer, LibraryEvent> consumerRecord = (ConsumerRecord<Integer, LibraryEvent>) context.getAttribute("record");
                libraryEventsService.handleRecovery(consumerRecord);
            }else{
                log.info("Inisde the non recoverable logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        });
        return factory;
    }

    private RetryTemplate retryTemplate() {
        final FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        final RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        //This is a map that is used in the SimpleRetryPolicy in another of its constructors so that we specify when to retry (for which exception to retry)
//      And for which one we should not retry
        final Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
        exceptionsMap.put(IllegalArgumentException.class, false);
        exceptionsMap.put(RecoverableDataAccessException.class, true);
//        final SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        //This is the number of times kafka will try if there is an exception, until there will be success, if not success then it will be converted in an error]
        //handled by the error handler
        final SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMap, true);
//        simpleRetryPolicy.setMaxAttempts(3);
        return simpleRetryPolicy;
    }
}
