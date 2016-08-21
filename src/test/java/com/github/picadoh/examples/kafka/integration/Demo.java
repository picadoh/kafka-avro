package com.github.picadoh.examples.kafka.integration;

import com.github.picadoh.examples.kafka.consumer.KafkaLibConsumer;
import com.github.picadoh.examples.kafka.mapper.AvroDataMapper;
import com.github.picadoh.examples.kafka.mapper.DataMapper;
import com.github.picadoh.examples.kafka.processor.EventBusDataProcessor;
import com.github.picadoh.examples.kafka.producer.KafkaLibProducer;
import com.github.picadoh.examples.kafka.schema.AvroSchemaDefinitionLoader;
import com.google.common.base.Function;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.github.picadoh.examples.kafka.config.PropertiesLoader.fromFile;

public class Demo {

    private Tweet tweet = null;
    private EventBus eventBus = new EventBus();

    public Demo() {
        eventBus.register(this);
    }

    @Subscribe
    public void handleSubscription(Tweet tweet) {
        this.tweet = tweet;
        System.out.println("Test succeeded. Processed tweet " + tweet);
    }

    public void execute() throws InterruptedException, IOException {
        String topicName = "my.test.topic";

        // setup schema
        Schema schema = getSchema("schema/tweet.avro");

        // setup the mapper
        DataMapper<GenericRecord> mapper = new AvroDataMapper(schema);

        // setup the consumer
        KafkaLibConsumer<GenericRecord> consumer = new KafkaLibConsumer<>(
                topicName, fromFile("consumer.properties"), mapper, new EventBusDataProcessor<>(eventBus, TO_TWEET));

        // set up the producer
        KafkaLibProducer<GenericRecord> producer = new KafkaLibProducer<>(topicName, fromFile("producer.properties"), mapper);

        // produce data
        producer.produce(buildTestData(schema));

        // try and retry consume
        int retries = 0;
        int maxRetries = 10;
        while (retries++ < maxRetries && tweet == null) {
            TimeUnit.SECONDS.sleep(1);
            consumer.consume();
        }

        producer.close();
    }

    private GenericRecord buildTestData(Schema schema) {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("username", "hugopicado");
        avroRecord.put("tweet", "my #awesome tweet");
        return avroRecord;
    }

    private Schema getSchema(String fileName) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(AvroSchemaDefinitionLoader.fromFile(fileName).get());
    }

    private static final Function<GenericRecord, Tweet> TO_TWEET = new Function<GenericRecord, Tweet>() {
        @Override
        public Tweet apply(GenericRecord record) {
            return new Tweet(record.get("username").toString(), record.get("tweet").toString());
        }
    };

    public static void main(String[] args) throws Exception {
        new Demo().execute();
    }

}
