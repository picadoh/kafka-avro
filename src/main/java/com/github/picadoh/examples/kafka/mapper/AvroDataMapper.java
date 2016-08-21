package com.github.picadoh.examples.kafka.mapper;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Avro data mapper responsible for converting between byte[] and Avro data record.
 */
public class AvroDataMapper implements DataMapper<GenericRecord> {
    private final Schema schema;

    public AvroDataMapper(Schema schema) {
        this.schema = schema;
    }

    @Override
    public GenericRecord to(byte[] value) {
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        return recordInjection.invert(value).get();
    }

    @Override
    public byte[] from(GenericRecord data) {
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        return recordInjection.apply(data);
    }

}
