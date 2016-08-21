package com.github.picadoh.examples.kafka.config;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

    public static Properties fromFile(String fileName) throws IOException {
        Properties properties = new Properties();
        InputStream props = Resources.getResource(fileName).openStream();
        properties.load(props);
        return properties;
    }

}
