package com.github.picadoh.examples.kafka.schema;

import com.google.common.base.Optional;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AvroSchemaDefinitionLoaderTest {

    @Test
    public void shouldLoadSchemaDefinitionFromFile() {
        Optional<String> schema = AvroSchemaDefinitionLoader.fromFile("schema/test.avro");

        assertTrue(schema.isPresent());
        assertEquals(schema.get().replaceAll("(\n| )",""),
                "{\"type\":\"record\",\"name\":\"person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");
    }

    @Test
    public void shouldReturnAbsentIfFileDoesNotExist() {
        Optional<String> schema = AvroSchemaDefinitionLoader.fromFile("schema/test.missing.avro");
        assertFalse(schema.isPresent());
    }

}
