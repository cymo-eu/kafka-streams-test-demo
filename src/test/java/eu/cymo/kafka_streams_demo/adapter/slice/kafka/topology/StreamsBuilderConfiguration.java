package eu.cymo.kafka_streams_demo.adapter.slice.kafka.topology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({ KafkaProperties.class })
public class StreamsBuilderConfiguration {
    
    @Bean
    StreamsBuilder streamsBuilder(
            ObjectProvider<SslBundles> sslBundles,
            KafkaProperties kafkaProperties) {
        var streamsProperties = kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable());
        var streamsConfig = new StreamsConfig(streamsProperties);
        var topologyConfig = new TopologyConfig(streamsConfig);
        return new StreamsBuilder(topologyConfig);
    }
    
}
