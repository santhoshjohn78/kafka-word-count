package com.ehss;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;


@SpringBootApplication
public class WordCountApp {


    public static final String TOPICNAME = "test";


    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.application-id}")
    private String applicationId;

    @Bean
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean() {
        HashMap props = new HashMap();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaStreamsConfiguration config = new KafkaStreamsConfiguration(props);
        return new StreamsBuilderFactoryBean(config);
    }




    @Bean
    public KStream<String, Long> kStream(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream(TOPICNAME);
        KTable<String,Long> wordCount = stream.mapValues(value->value.toLowerCase()).flatMapValues(value -> Arrays.asList(value.split(" ")))
                        .selectKey((key,value)->value).groupByKey().count();
        stream.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .foreach((key, value) -> System.out.println(key + " -> " + value));

        return wordCount.toStream();
    }

    public static void main(String[] args) {
        SpringApplication.run(WordCountApp.class, args);
    }
}
