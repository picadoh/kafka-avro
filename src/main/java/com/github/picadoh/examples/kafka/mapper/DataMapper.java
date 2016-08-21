package com.github.picadoh.examples.kafka.mapper;

/**
 * Interface for mapping implementations. Mappers will convert
 * a byte[] read from Kafka into some meaningful data.
 *
 * @param <T> Type of the data to convert byte[] to and from.
 */
public interface DataMapper<T> {

    T to(byte[] value);

    byte[] from(T data);

}
