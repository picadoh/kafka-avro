package com.github.picadoh.examples.kafka.processor;

/**
 * Interface to be implemented by processors of data.
 */
public interface DataProcessor<S> {

    void process(S data);

}
