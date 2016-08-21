package com.github.picadoh.examples.kafka.producer;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.github.picadoh.examples.kafka.mapper.DataMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Wrapper around Kafka Producer API that instantiates a producer, a mapper.
 *
 * The mapper is used to create the byte[] to write into kafka from a schema.
 * The producer is used to write data into Kafka.
 *
 * @param <T> Schema record data type.
 */
public class KafkaLibProducer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLibProducer.class);

    private final KafkaProducer<String, byte[]> producer;
    private final DataMapper<T> dataMapper;
    private final String topic;

    public KafkaLibProducer(String topic, Properties properties, DataMapper<T> dataMapper) {
        this(topic, new KafkaProducer<String, byte[]>(properties), dataMapper);
    }

    @VisibleForTesting
    KafkaLibProducer(String topic, KafkaProducer<String, byte[]> producer, DataMapper<T> dataMapper) {
        this.producer = producer;
        this.topic = topic;
        this.dataMapper = dataMapper;
    }

    public void produce(T data) {
        LOGGER.debug("Producing " + data + " to " + topic);

        byte[] binary = dataMapper.from(data);
        producer.send(new ProducerRecord<String, byte[]>(topic, binary));
        producer.flush();
    }

    public void close() {
        this.producer.close();
    }

}
