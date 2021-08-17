package com.learnkafka.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.repository.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    private static final String TOPIC = "library-events";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;

    @Autowired
    private LibraryEventsRepository repository;

    public void processLibraryEvent(ConsumerRecord<Integer, LibraryEvent> consumerRecord) throws JsonProcessingException {
        final LibraryEvent libraryEvent = consumerRecord.value();


        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
            throw new RecoverableDataAccessException("Temporary network issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid library event type");
        }
    }

    private void validate(final LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id is missing");
        }

        final Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEvent.getLibraryEventId());
        if (libraryEventOptional.isEmpty()) {
            throw new IllegalArgumentException("Not found library event");
        }

        log.info("Validation successful for libraryEvent id: {}", libraryEvent.getLibraryEventId());
    }

    private void save(final LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        log.info("Successfully persisted library event {}", libraryEvent);
    }

    public void handleRecovery(final ConsumerRecord<Integer, LibraryEvent> record) {
        final Integer key = record.key();
        final LibraryEvent value = record.value();
        final ListenableFuture<SendResult<Integer, LibraryEvent>> listenableFuture = kafkaTemplate.send(TOPIC, key, value);
        listenableFuture.addCallback( new ListenableFutureCallback<SendResult<Integer, LibraryEvent>>()
        {
            @Override
            public void onFailure( final Throwable ex )
            {
                handleFailure( key, value, ex );
            }


            @Override
            public void onSuccess( final SendResult<Integer, LibraryEvent> result )
            {
                handleSuccess( key, value, result );
            }
        } );
    }

    private void handleSuccess(Integer key,
                               LibraryEvent value,
                               SendResult<Integer, LibraryEvent> result) {
        log.info("Message sent successfully for key {}, value {}, to partition {}", key, value, result.getRecordMetadata().partition());
    }


    private void handleFailure(Integer key,
                               LibraryEvent value,
                               Throwable exception) {
        log.info("Error sending message: {}", exception.getMessage());

        try {
            throw exception;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure {}", throwable.getMessage());
        }
    }
}
