package com.github.picadoh.examples.kafka.consumer;

import com.github.picadoh.examples.kafka.mapper.AvroDataMapper;
import com.github.picadoh.examples.kafka.processor.EventBusDataProcessor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaLibConsumerTest {

    @Mock
    private AvroDataMapper dataMapper;

    @Mock
    private EventBusDataProcessor dataProcessor;

    private KafkaLibConsumer victim;

    @BeforeMethod
    public void setupScenario() throws IOException {
        initMocks(this);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConsumeMapAndProcess() {
        GenericRecord genericAvroRecord = mock(GenericRecord.class);
        when(dataMapper.to(any(byte[].class))).thenReturn(genericAvroRecord);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("test.topic", 0, 123L, null, new byte[]{1,2});
        ConsumerRecords<String, byte[]> consumerRecords = mock(ConsumerRecords.class);
        when(consumerRecords.iterator()).thenReturn(newArrayList(record).iterator());

        KafkaConsumer<String, byte[]> kafkaConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumer.poll(anyInt())).thenReturn(consumerRecords);

        victim = new KafkaLibConsumer<>("test.topic", kafkaConsumer, dataMapper, dataProcessor);

        victim.consume();

        verify(dataMapper).to(new byte[]{1, 2});
        verify(dataProcessor).process(genericAvroRecord);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotProcessMessagesIfNotSubscribed() {
        GenericRecord genericAvroRecord = mock(GenericRecord.class);
        when(dataMapper.to(any(byte[].class))).thenReturn(genericAvroRecord);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("test.topic.random", 0, 123L, null, new byte[]{1,2});
        ConsumerRecords<String, byte[]> consumerRecords = mock(ConsumerRecords.class);
        when(consumerRecords.iterator()).thenReturn(newArrayList(record).iterator());

        KafkaConsumer<String, byte[]> kafkaConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumer.poll(anyInt())).thenReturn(consumerRecords);

        victim = new KafkaLibConsumer<>("test.topic.subscribed", kafkaConsumer, dataMapper, dataProcessor);

        victim.consume();

        verifyZeroInteractions(dataMapper);
        verifyZeroInteractions(dataProcessor);
    }

}
