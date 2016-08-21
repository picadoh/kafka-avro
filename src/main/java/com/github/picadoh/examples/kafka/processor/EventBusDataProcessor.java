package com.github.picadoh.examples.kafka.processor;

import com.google.common.base.Function;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor implementation that only takes a record, logs it at INFO level and posts to
 * an event bus.
 */
public class EventBusDataProcessor<S, T> implements DataProcessor<S> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventBusDataProcessor.class);

    private final EventBus eventBus;
    private final Function<S, T> converter;

    public EventBusDataProcessor(EventBus eventBus, Function<S, T> converter) {
        this.eventBus = eventBus;
        this.converter = converter;
    }

    @Override
    public void process(S data) {
        LOGGER.debug("Processing data: " + (data != null ? data.toString() : ""));

        T converted = converter.apply(data);
        if (converted != null) {
            eventBus.post(converted);
        } else {
            LOGGER.debug("Converting data resulted in a null result");
        }
    }
}
