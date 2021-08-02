package com.learnkafka.libraryeventsconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.consumer.LibraryEventsConsumer;
import com.learnkafka.libraryeventsconsumer.entity.Book;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEventType;
import com.learnkafka.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.stats.Count;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.atLeast;

@SpringBootTest
@EmbeddedKafka(topics = "library-events",
        partitions = 3)
//since we wont need the properties from the admin in the consumer, those ones will get removed
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
//        The following property is to connect the consumer to the embedded kafka, as the injected producer is
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
//        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" } )
public class LibraryEventsConsumerIntegrationTest {

    public static final String TOPIC = "library-events";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    //    Basically the following bean holds the list of all the listener containers that are available in kafka now
//    And the consumer that we have is a listener container
    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    //A spy bean gives us access to the real bean to get insights about the behavior of that bean
    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumer;

    @SpyBean
    private LibraryEventsService libraryEventsService;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
//        Then we need to get access to those listeners in order to wait for assignment of the listener container (in this case the consumer)
//        And the partitions of the topic, so that we won't have any issues reading the message in the end
        for (final MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1234,\"bookName\":\"My book\",\"bookAuthor\":\"David\"}}";
//        This one is asynchronous, but with the get turns into synchronous
//        kafkaTemplate.sendDefault(json).get();
        //This method is synchronous
        kafkaTemplate.send(TOPIC, json);

        //when
        //CountdownLatch blocks the current thread so that we can wait a some time while the message reaches the consumer
        //Because the consumer and this test run in different threads
        final CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        //Testing the repo, this find all of the crud repo returns iterable, hence the need of casting
        final List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertEquals(1, libraryEventList.size());

        libraryEventList.forEach(libraryEvent -> {
            assertNotNull(libraryEvent.getLibraryEventId());
            assertEquals(1234, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, InterruptedException {
        //given
        final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1234,\"bookName\":\"My book\",\"bookAuthor\":\"David\"}}";
        final LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        //Updating the book to mimic an update record
        final Book updatedBook = Book.builder()
                .bookId(1234 )
                .bookName("Kafka using spring boot 2.x")
                .bookAuthor("David Galvis")
                .build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        final String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.send(TOPIC, libraryEvent.getLibraryEventId(), updatedJson);

        //when
        final CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        //Testing the repo, this find all of the crud repo returns iterable, hence the need of casting
        final LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka using spring boot 2.x", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":1234,\"bookName\":\"My book\",\"bookAuthor\":\"David\"}}";
//        kafkaTemplate.sendDefault(libraryEventId, json).get();
        kafkaTemplate.send(TOPIC, libraryEventId, json);
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventsConsumer, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

}
