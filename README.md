### Kafka Producer/Consumer Example
This example implements a Kafka producer and consumer that use a Avro schema mapper to communicate data in the topic.

#### Pre-requisites
- Kafka 0.9.0
- Maven 3
- Java 7/8

#### Setup environment

###### Start Kafka Server

    kafka$ ./bin/zookeeper-server-start.sh config/zookeeper.properties

###### Start Zookeeper Server

    kafka$ ./bin/kafka-server-start.sh config/server.properties

###### Create Kafka Topic

    kafka$ ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic example.test.topic

#### Build application

    project$ mvn clean install

#### Example usage

    String topicName = "example.test.topic";

    // setup schema
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(AvroSchemaDefinitionLoader.fromFile("schema/tweet.avro").get());

    // setup the mapper
    DataMapper<GenericRecord> mapper = new AvroDataMapper(schema);

    // setup consumer
    DataProcessor<GenericRecord> processor = new AvroDataProcessor();
    KafkaLibConsumer<GenericRecord> consumer = new KafkaLibConsumer<>(
        topicName,
        PropertiesLoader.fromFile("consumer.properties"),
        mapper,
        processor);

    // set up the producer
    KafkaLibProducer<GenericRecord> producer = new KafkaLibProducer<>(
        topicName,
        PropertiesLoader.fromFile("producer.properties"),
        mapper);

    GenericData.Record avroRecord = new GenericData.Record(schema);
    avroRecord.put("username", "hugopicado");
    avroRecord.put("tweet", "my #awesome tweet");

    producer.produce(avroRecord);
    consumer.consume();
