package com.github.picadoh.examples.kafka.producer;

import com.github.picadoh.examples.kafka.mapper.DataMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.Mock;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.google.common.io.Resources.getResource;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaLibProducerTest {

    @Mock
    private DataMapper<GenericRecord> dataMapper;

    @Mock
    private KafkaProducer<String, byte[]> producer;

    private KafkaLibProducer<GenericRecord> victim;

    @BeforeClass
    public void setupScenario() {
        initMocks(this);

        victim = new KafkaLibProducer<>("test.topic", producer, dataMapper);
    }

    @Test
    public void shouldProduceMessage() throws IOException {
        Schema schema = new Schema.Parser().parse(Resources.toString(getResource("schema/test.avro"), Charsets.UTF_8));

        byte[] bytes = new byte[]{1, 2};
        when(dataMapper.from(any(GenericRecord.class))).thenReturn(bytes);

        GenericRecord data = mock(GenericRecord.class);
        when(data.getSchema()).thenReturn(schema);
        when(data.get(eq("name"))).thenReturn("Anne");
        when(data.get(anyInt())).thenReturn("Anne");

        victim.produce(data);

        verify(dataMapper).from(data);
        verify(producer).send(eq(new ProducerRecord<String, byte[]>("test.topic", bytes)));
        verify(producer).flush();
    }

    @Test
    public void shouldCloseProducer() throws IOException {
        victim.close();
        verify(producer).close();
    }

}
