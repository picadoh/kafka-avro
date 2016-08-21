package com.github.picadoh.examples.kafka.mapper;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.google.common.io.Resources.getResource;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class AvroDataMapperTest {

    private AvroDataMapper victim;
    private Schema schema;

    @BeforeMethod
    public void setupScenario() throws IOException {
        schema = new Schema.Parser().parse(Resources.toString(getResource("schema/test.avro"), Charsets.UTF_8));
    }

    @Test
    public void shouldConvertBetweenAvroRecordAndByteArray() {
        GenericRecord data = mock(GenericRecord.class);
        when(data.getSchema()).thenReturn(schema);
        when(data.get(eq("name"))).thenReturn("Anne");
        when(data.get(anyInt())).thenReturn("Anne");

        victim = new AvroDataMapper(schema);
        byte[] bytes = victim.from(data);

        GenericRecord record = victim.to(bytes);
        assertEquals(record.get("name").toString(), "Anne");
    }

}
