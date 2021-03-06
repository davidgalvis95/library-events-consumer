package com.learnkafka.libraryeventsconsumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
//In order to acknowledge manually we need to implement this interface
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    //To manually commit the offsets we should implement this interface
    @Override
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord : {} ", consumerRecord);
        //Normally we acknowledge if there are certain conditions that are met, so that the offset gets committed
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
