package com.github.picadoh.examples.kafka.consumer;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.github.picadoh.examples.kafka.mapper.DataMapper;
import com.github.picadoh.examples.kafka.processor.DataProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

/**
 * A wrapper around Kafka's Consumer API that instantiates a consumer,
 * a mapper and a processor.
 *
 * The consumer is used to retrieve the messages from Kafka, the mapper converts
 * the messages into something meaningful. The processor does something with those
 * messages.
 *
 * @param <T> Schema record data type.
 */
public class KafkaLibConsumer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLibConsumer.class);

    private KafkaConsumer<String, byte[]> consumer;

    private final DataMapper<T> dataMapper;
    private final DataProcessor<T> dataProcessor;
    private final String topic;

    public KafkaLibConsumer(String topic, Properties properties, DataMapper<T> dataMapper, DataProcessor<T> dataProcessor) {
        this(topic, new KafkaConsumer<String, byte[]>(properties), dataMapper, dataProcessor);
    }

    @VisibleForTesting
    KafkaLibConsumer(String topic, KafkaConsumer<String, byte[]> consumer, DataMapper<T> dataMapper, DataProcessor<T> dataProcessor) {
        this.consumer = consumer;
        this.consumer.subscribe(newArrayList(topic));

        this.dataMapper = dataMapper;
        this.dataProcessor = dataProcessor;
        this.topic = topic;
    }

    public void consume() {
        ConsumerRecords<String, byte[]> records = consumer.poll(100);
        LOGGER.debug("Consuming {} records", records.count());

        for (ConsumerRecord<String, byte[]> record : records) {
            if (record.topic().equals(topic)) {
                LOGGER.info("Consuming {} from {}", record, topic);
                T data = dataMapper.to(record.value());
                dataProcessor.process(data);
                consumer.commitSync();
            }
        }
    }
}
