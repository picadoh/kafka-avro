package com.github.picadoh.examples.kafka.processor;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.io.Resources.getResource;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EventBusDataProcessorTest {

    @Mock
    private EventBus bus;

    private EventBusDataProcessor<GenericRecord, String> victim;

    @BeforeMethod
    public void setupScenario() {
        initMocks(this);
    }

    @Test
    public void shouldProcessRecord() throws Exception {
        Schema schema = new Schema.Parser().parse(Resources.toString(getResource("schema/test.avro"), Charsets.UTF_8));

        victim = new EventBusDataProcessor<>(bus, new Function<GenericRecord, String>() {
            @Override
            public String apply(GenericRecord record) {
                return record.get("name").toString();
            }
        });

        GenericRecord data = mock(GenericRecord.class);
        when(data.getSchema()).thenReturn(schema);
        when(data.get(eq("name"))).thenReturn("Anne");
        when(data.get(anyInt())).thenReturn("Anne");

        victim.process(data);

        verify(bus).post("Anne");
    }

    @Test
    public void shouldNotPostWhenConverterReturnsNull() throws Exception {
        Schema schema = new Schema.Parser().parse(Resources.toString(getResource("schema/test.avro"), Charsets.UTF_8));

        victim = new EventBusDataProcessor<>(bus, new Function<GenericRecord, String>() {
            @Override
            public String apply(GenericRecord record) {
                return null;
            }
        });

        GenericRecord data = mock(GenericRecord.class);
        when(data.getSchema()).thenReturn(schema);
        when(data.get(eq("name"))).thenReturn("Anne");
        when(data.get(anyInt())).thenReturn("Anne");

        victim.process(data);

        verifyZeroInteractions(bus);
    }

}
